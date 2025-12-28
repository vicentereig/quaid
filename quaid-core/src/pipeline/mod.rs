//! Parallel processing pipeline for conversation sync
//!
//! Three-stage pipeline with crossbeam channels:
//! 1. Fetch Stage - retrieve conversations from providers
//! 2. Media Stage - download attachments
//! 3. Embed Stage - chunk, embed, and persist

pub mod config;
pub mod messages;
pub mod stages;

pub use config::PipelineConfig;
pub use messages::PipelineMessage;

use crate::embeddings::{ChunkerConfig, Embedder, EmbeddingModel, MessageChunker};
use crate::providers::{Conversation, Message};
use crate::storage::parquet::ParquetStore;
use crate::storage::{EmbeddingsStore, ParquetStorageConfig};
use crossbeam_channel::bounded;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("Channel error: {0}")]
    Channel(String),

    #[error("Provider error: {0}")]
    Provider(String),

    #[error("Storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),

    #[error("Embedding error: {0}")]
    Embedding(#[from] crate::embeddings::EmbeddingError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Thread join error")]
    ThreadJoin,
}

pub type Result<T> = std::result::Result<T, PipelineError>;

/// Result of a pipeline run
#[derive(Debug, Default)]
pub struct PipelineResult {
    pub conversations_synced: usize,
    pub messages_processed: usize,
    pub attachments_downloaded: usize,
    pub embeddings_generated: usize,
    pub errors: Vec<String>,
}

/// The main pipeline orchestrator
pub struct Pipeline {
    config: PipelineConfig,
}

impl Pipeline {
    pub fn new(config: PipelineConfig) -> Self {
        Self { config }
    }

    /// Run the pipeline with a list of conversations to process
    pub fn run(
        &self,
        conversations: Vec<(String, Conversation, Vec<Message>)>, // (account_id, conv, messages)
    ) -> Result<PipelineResult> {
        let mut result = PipelineResult::default();

        if conversations.is_empty() {
            return Ok(result);
        }

        // Create channels between stages
        let (fetch_tx, fetch_rx) = bounded::<PipelineMessage>(self.config.channel_capacity);
        let (media_tx, media_rx) = bounded::<PipelineMessage>(self.config.channel_capacity);
        let (embed_tx, embed_rx) = bounded::<PipelineMessage>(self.config.channel_capacity);

        // Shared resources
        let storage_config = ParquetStorageConfig::new(&self.config.data_dir);
        let parquet_store = Arc::new(ParquetStore::new(storage_config.clone()));
        let embeddings_store = Arc::new(EmbeddingsStore::new(storage_config.clone()));
        let embedder: Arc<dyn Embedder> = Arc::new(
            EmbeddingModel::load_or_download(self.config.data_dir.join("models"))?,
        );
        let chunker = Arc::new(MessageChunker::new(ChunkerConfig::default()));

        // Spawn stage workers
        let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();

        // Stage 1: Feed conversations (single thread since we already have the data)
        // Move fetch_tx into the feeder thread (not clone)
        let convos = conversations;
        handles.push(thread::spawn(move || {
            for (account_id, conv, messages) in convos {
                let msg = PipelineMessage::ConversationFetched {
                    account_id,
                    conversation: conv,
                    messages,
                };
                if fetch_tx.send(msg).is_err() {
                    break;
                }
            }
            // fetch_tx dropped here, closing the channel
            Ok(())
        }));

        // Stage 2: Media download workers
        for _ in 0..self.config.media_workers {
            let rx = fetch_rx.clone();
            let tx = media_tx.clone();
            let storage = storage_config.clone();

            handles.push(thread::spawn(move || {
                stages::media_worker(rx, tx, storage)
            }));
        }
        // Drop our copies - workers have their own clones
        drop(fetch_rx);
        drop(media_tx);

        // Stage 3: Embed and persist workers
        for _ in 0..self.config.embed_workers {
            let rx = media_rx.clone();
            let tx = embed_tx.clone();
            let store = parquet_store.clone();
            let emb_store = embeddings_store.clone();
            let emb = embedder.clone();
            let chunk = chunker.clone();

            handles.push(thread::spawn(move || {
                stages::embed_worker(rx, tx, store, emb_store, emb, chunk)
            }));
        }
        // Drop our copies
        drop(media_rx);
        drop(embed_tx);

        // Collect results
        for msg in embed_rx {
            match msg {
                PipelineMessage::Complete {
                    conversation_id: _,
                    messages_count,
                    chunks_count,
                } => {
                    result.conversations_synced += 1;
                    result.messages_processed += messages_count;
                    result.embeddings_generated += chunks_count;
                }
                PipelineMessage::Error { message, .. } => {
                    result.errors.push(message);
                }
                _ => {}
            }
        }

        // Wait for all workers to finish
        for handle in handles {
            handle.join().map_err(|_| PipelineError::ThreadJoin)??;
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::{MessageContent, Role};
    use tempfile::tempdir;

    fn create_test_conversation(id: &str) -> Conversation {
        Conversation {
            id: id.to_string(),
            provider_id: "chatgpt".to_string(),
            title: format!("Test Conversation {}", id),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            model: Some("gpt-4".to_string()),
            project_id: None,
            project_name: None,
            is_archived: false,
        }
    }

    fn create_test_message(conv_id: &str, msg_id: &str, text: &str) -> Message {
        Message {
            id: msg_id.to_string(),
            conversation_id: conv_id.to_string(),
            parent_id: None,
            role: Role::User,
            content: MessageContent::Text {
                text: text.to_string(),
            },
            created_at: Some(chrono::Utc::now()),
            model: None,
        }
    }

    #[test]
    fn test_pipeline_empty() {
        let dir = tempdir().unwrap();
        let config = PipelineConfig::new(dir.path());
        let pipeline = Pipeline::new(config);

        let result = pipeline.run(vec![]).unwrap();

        assert_eq!(result.conversations_synced, 0);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_pipeline_single_conversation() {
        let dir = tempdir().unwrap();
        let config = PipelineConfig::new(dir.path());
        let pipeline = Pipeline::new(config);

        let conv = create_test_conversation("conv-1");
        let messages = vec![
            create_test_message("conv-1", "msg-1", "Hello"),
            create_test_message("conv-1", "msg-2", "World"),
        ];

        let result = pipeline
            .run(vec![("user-123".to_string(), conv, messages)])
            .unwrap();

        assert_eq!(result.conversations_synced, 1);
        assert_eq!(result.messages_processed, 2);
        assert!(result.errors.is_empty());

        // Verify parquet file was created
        let parquet_path = dir.path().join("conversations/chatgpt/conv-1.parquet");
        assert!(parquet_path.exists());
    }

    #[test]
    fn test_pipeline_multiple_conversations() {
        let dir = tempdir().unwrap();
        let config = PipelineConfig::new(dir.path());
        let pipeline = Pipeline::new(config);

        let convos: Vec<_> = (0..5)
            .map(|i| {
                let id = format!("conv-{}", i);
                let conv = create_test_conversation(&id);
                let messages = vec![
                    create_test_message(&id, &format!("msg-{}-1", i), "Message 1"),
                    create_test_message(&id, &format!("msg-{}-2", i), "Message 2"),
                ];
                ("user-123".to_string(), conv, messages)
            })
            .collect();

        let result = pipeline.run(convos).unwrap();

        assert_eq!(result.conversations_synced, 5);
        assert_eq!(result.messages_processed, 10);
    }

    #[test]
    fn test_pipeline_config_worker_counts() {
        let config = PipelineConfig {
            data_dir: std::path::PathBuf::from("/tmp"),
            fetch_workers: 4,
            media_workers: 2,
            embed_workers: 2,
            channel_capacity: 50,
        };

        assert_eq!(config.fetch_workers, 4);
        assert_eq!(config.media_workers, 2);
        assert_eq!(config.embed_workers, 2);
    }
}
