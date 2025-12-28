//! Pipeline stage worker implementations

use super::messages::PipelineMessage;
use super::Result;
use crate::embeddings::{Embedder, MessageChunker};
use crate::storage::parquet::ParquetStore;
use crate::storage::ParquetStorageConfig;
use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;

/// Stage 2: Media download worker
///
/// Receives ConversationFetched messages, downloads any attachments,
/// and forwards MediaDownloaded messages to the next stage.
pub fn media_worker(
    rx: Receiver<PipelineMessage>,
    tx: Sender<PipelineMessage>,
    _storage_config: ParquetStorageConfig,
) -> Result<()> {
    for msg in rx {
        match msg {
            PipelineMessage::ConversationFetched {
                account_id,
                conversation,
                messages,
            } => {
                // TODO: Download attachments when provider support is added
                // For now, just forward the message

                let result = PipelineMessage::MediaDownloaded {
                    account_id,
                    conversation,
                    messages,
                    attachments: vec![], // No attachments downloaded yet
                };

                if tx.send(result).is_err() {
                    break; // Receiver dropped, stop processing
                }
            }
            PipelineMessage::Shutdown => {
                let _ = tx.send(PipelineMessage::Shutdown);
                break;
            }
            PipelineMessage::Error { .. } => {
                // Forward errors to next stage
                let _ = tx.send(msg);
            }
            _ => {} // Ignore other message types
        }
    }

    Ok(())
}

/// Stage 3: Embed and persist worker
///
/// Receives MediaDownloaded messages, chunks messages, generates embeddings,
/// and persists to parquet files.
pub fn embed_worker(
    rx: Receiver<PipelineMessage>,
    tx: Sender<PipelineMessage>,
    store: Arc<ParquetStore>,
    embedder: Arc<dyn Embedder>,
    chunker: Arc<MessageChunker>,
) -> Result<()> {
    for msg in rx {
        match msg {
            PipelineMessage::MediaDownloaded {
                account_id,
                conversation,
                messages,
                attachments: _,
            } => {
                let conv_id = conversation.id.clone();
                let messages_count = messages.len();

                // Chunk all messages
                let chunks = chunker.chunk_messages(&messages);
                let chunks_count = chunks.len();

                // Generate embeddings for chunks
                let chunk_texts: Vec<&str> = chunks.iter().map(|c| c.text.as_str()).collect();
                let _embeddings = match embedder.embed_batch(&chunk_texts) {
                    Ok(e) => e,
                    Err(e) => {
                        let _ = tx.send(PipelineMessage::Error {
                            conversation_id: conv_id.clone(),
                            stage: "embed".to_string(),
                            message: format!("Embedding failed: {}", e),
                        });
                        continue;
                    }
                };

                // Write conversation to parquet
                if let Err(e) = store.write_conversation(&account_id, &conversation, &messages) {
                    let _ = tx.send(PipelineMessage::Error {
                        conversation_id: conv_id.clone(),
                        stage: "persist".to_string(),
                        message: format!("Failed to write parquet: {}", e),
                    });
                    continue;
                }

                // TODO: Write embeddings to separate parquet file

                // Send completion
                let _ = tx.send(PipelineMessage::Complete {
                    conversation_id: conv_id,
                    messages_count,
                    chunks_count,
                });
            }
            PipelineMessage::Shutdown => {
                let _ = tx.send(PipelineMessage::Shutdown);
                break;
            }
            PipelineMessage::Error { .. } => {
                // Forward errors
                let _ = tx.send(msg);
            }
            _ => {} // Ignore other message types
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embeddings::{ChunkerConfig, MockEmbeddingModel};
    use crate::providers::{Conversation, Message, MessageContent, Role};
    use crossbeam_channel::bounded;
    use tempfile::tempdir;

    fn create_test_conversation() -> Conversation {
        Conversation {
            id: "conv-1".to_string(),
            provider_id: "chatgpt".to_string(),
            title: "Test".to_string(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            model: None,
            project_id: None,
            project_name: None,
            is_archived: false,
        }
    }

    fn create_test_message(id: &str, text: &str) -> Message {
        Message {
            id: id.to_string(),
            conversation_id: "conv-1".to_string(),
            parent_id: None,
            role: Role::User,
            content: MessageContent::Text {
                text: text.to_string(),
            },
            created_at: None,
            model: None,
        }
    }

    #[test]
    fn test_media_worker_forwards_messages() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());

        let (in_tx, in_rx) = bounded(10);
        let (out_tx, out_rx) = bounded(10);

        // Send a conversation
        in_tx
            .send(PipelineMessage::ConversationFetched {
                account_id: "user-1".to_string(),
                conversation: create_test_conversation(),
                messages: vec![create_test_message("msg-1", "Hello")],
            })
            .unwrap();
        drop(in_tx); // Signal no more messages

        // Run worker
        let handle = std::thread::spawn(move || media_worker(in_rx, out_tx, config));

        // Check output
        let output = out_rx.recv().unwrap();
        if let PipelineMessage::MediaDownloaded {
            account_id,
            conversation,
            messages,
            attachments,
        } = output
        {
            assert_eq!(account_id, "user-1");
            assert_eq!(conversation.id, "conv-1");
            assert_eq!(messages.len(), 1);
            assert!(attachments.is_empty());
        } else {
            panic!("Expected MediaDownloaded message");
        }

        handle.join().unwrap().unwrap();
    }

    #[test]
    fn test_embed_worker_processes_messages() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());

        let (in_tx, in_rx) = bounded(10);
        let (out_tx, out_rx) = bounded(10);

        let store = Arc::new(ParquetStore::new(config));
        let embedder: Arc<dyn Embedder> = Arc::new(MockEmbeddingModel::new(384));
        let chunker = Arc::new(MessageChunker::new(ChunkerConfig::default()));

        // Send a media downloaded message
        in_tx
            .send(PipelineMessage::MediaDownloaded {
                account_id: "user-1".to_string(),
                conversation: create_test_conversation(),
                messages: vec![
                    create_test_message("msg-1", "Hello world"),
                    create_test_message("msg-2", "How are you?"),
                ],
                attachments: vec![],
            })
            .unwrap();
        drop(in_tx);

        // Run worker
        let handle =
            std::thread::spawn(move || embed_worker(in_rx, out_tx, store, embedder, chunker));

        // Check output
        let output = out_rx.recv().unwrap();
        if let PipelineMessage::Complete {
            conversation_id,
            messages_count,
            chunks_count,
        } = output
        {
            assert_eq!(conversation_id, "conv-1");
            assert_eq!(messages_count, 2);
            assert!(chunks_count >= 2); // At least one chunk per message
        } else {
            panic!("Expected Complete message, got {:?}", output);
        }

        handle.join().unwrap().unwrap();

        // Verify file was written
        let parquet_path = dir.path().join("conversations/chatgpt/conv-1.parquet");
        assert!(parquet_path.exists());
    }

    #[test]
    fn test_workers_handle_shutdown() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());

        let (in_tx, in_rx) = bounded(10);
        let (out_tx, out_rx) = bounded(10);

        in_tx.send(PipelineMessage::Shutdown).unwrap();
        drop(in_tx);

        let handle = std::thread::spawn(move || media_worker(in_rx, out_tx, config));

        // Should receive shutdown and exit cleanly
        let output = out_rx.recv().unwrap();
        assert!(matches!(output, PipelineMessage::Shutdown));

        handle.join().unwrap().unwrap();
    }
}
