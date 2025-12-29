//! Storage trait abstractions
//!
//! These traits define the interface for different storage backends (SQLite, Parquet, etc.)

use crate::providers::{Attachment, Conversation, Message};
use chrono::{DateTime, Utc};
use std::path::Path;

/// Result type for storage operations
pub type Result<T> = std::result::Result<T, super::StorageError>;

/// Trait for storing and retrieving conversations
pub trait ConversationStorage: Send + Sync {
    /// Save a conversation (upsert semantics)
    fn save_conversation(&self, account_id: &str, conv: &Conversation) -> Result<()>;

    /// Get a conversation by ID
    fn get_conversation(&self, id: &str) -> Result<Option<Conversation>>;

    /// Get just the updated_at timestamp (for incremental sync)
    fn get_conversation_updated_at(&self, id: &str) -> Result<Option<DateTime<Utc>>>;

    /// List all conversations for an account
    fn list_conversations(&self, account_id: &str) -> Result<Vec<Conversation>>;
}

/// Trait for storing and retrieving messages
pub trait MessageStorage: Send + Sync {
    /// Save a message (upsert semantics)
    fn save_message(&self, message: &Message) -> Result<()>;

    /// Get all messages for a conversation
    fn get_messages(&self, conversation_id: &str) -> Result<Vec<Message>>;
}

/// Trait for full-text search
pub trait SearchStorage: Send + Sync {
    /// Search messages by text query
    fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>>;
}

/// Trait for semantic/vector search
pub trait SemanticSearchStorage: Send + Sync {
    /// Search by embedding similarity
    fn search_semantic(&self, embedding: &[f32], limit: usize) -> Result<Vec<SemanticSearchResult>>;

    /// Hybrid search combining FTS and vector similarity
    fn search_hybrid(
        &self,
        query: &str,
        embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<SemanticSearchResult>>;
}

/// Trait for attachment storage
pub trait AttachmentStorage: Send + Sync {
    /// Save attachment metadata
    fn save_attachment(&self, attachment: &Attachment) -> Result<()>;

    /// Mark an attachment as downloaded
    fn mark_attachment_downloaded(&self, id: &str, local_path: &str) -> Result<()>;

    /// Get attachments that haven't been downloaded yet
    fn get_pending_attachments(&self) -> Result<Vec<Attachment>>;
}

/// Full-text search result
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub conversation_id: String,
    pub snippet: String,
}

/// Semantic search result with similarity score
#[derive(Debug, Clone)]
pub struct SemanticSearchResult {
    pub conversation_id: String,
    pub message_id: String,
    pub chunk_text: String,
    pub score: f32,
}

/// Configuration for Parquet storage
#[derive(Debug, Clone)]
pub struct ParquetStorageConfig {
    /// Base directory for parquet files
    pub base_dir: std::path::PathBuf,
}

impl ParquetStorageConfig {
    pub fn new(base_dir: impl AsRef<Path>) -> Self {
        Self {
            base_dir: base_dir.as_ref().to_path_buf(),
        }
    }

    /// Path for a conversation's parquet file
    pub fn conversation_path(&self, provider: &str, conversation_id: &str) -> std::path::PathBuf {
        self.base_dir
            .join("conversations")
            .join(provider)
            .join(format!("{}.parquet", conversation_id))
    }

    /// Path for a conversation's embeddings parquet file
    pub fn embeddings_path(&self, provider: &str, conversation_id: &str) -> std::path::PathBuf {
        self.base_dir
            .join("embeddings")
            .join(provider)
            .join(format!("{}.parquet", conversation_id))
    }

    /// Path for consolidated embeddings file (one per provider)
    pub fn consolidated_embeddings_path(&self, provider: &str) -> std::path::PathBuf {
        self.base_dir
            .join("embeddings")
            .join(format!("{}.parquet", provider))
    }

    /// Directory containing per-conversation embeddings for a provider
    pub fn embeddings_dir(&self, provider: &str) -> std::path::PathBuf {
        self.base_dir.join("embeddings").join(provider)
    }

    /// List all providers that have embeddings (either consolidated or per-conversation)
    pub fn list_embedding_providers(&self) -> std::io::Result<Vec<String>> {
        let embeddings_dir = self.base_dir.join("embeddings");
        if !embeddings_dir.exists() {
            return Ok(vec![]);
        }

        let mut providers = Vec::new();
        for entry in std::fs::read_dir(&embeddings_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                // Per-conversation directory (e.g., embeddings/fathom/)
                if let Some(name) = path.file_name() {
                    providers.push(name.to_string_lossy().to_string());
                }
            } else if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                // Consolidated file (e.g., embeddings/fathom.parquet)
                if let Some(stem) = path.file_stem() {
                    let provider = stem.to_string_lossy().to_string();
                    if !providers.contains(&provider) {
                        providers.push(provider);
                    }
                }
            }
        }
        Ok(providers)
    }

    /// Path for a conversation's media directory
    pub fn media_dir(&self, provider: &str, conversation_id: &str) -> std::path::PathBuf {
        self.base_dir
            .join("media")
            .join(provider)
            .join(conversation_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_storage_config_paths() {
        let config = ParquetStorageConfig::new("/data/quaid");

        assert_eq!(
            config.conversation_path("chatgpt", "conv-123"),
            std::path::PathBuf::from("/data/quaid/conversations/chatgpt/conv-123.parquet")
        );

        assert_eq!(
            config.embeddings_path("claude", "conv-456"),
            std::path::PathBuf::from("/data/quaid/embeddings/claude/conv-456.parquet")
        );

        assert_eq!(
            config.media_dir("fathom", "conv-789"),
            std::path::PathBuf::from("/data/quaid/media/fathom/conv-789")
        );
    }

    #[test]
    fn test_search_result_debug() {
        let result = SearchResult {
            conversation_id: "conv-123".to_string(),
            snippet: "Hello world".to_string(),
        };
        // Ensure Debug is implemented
        let _ = format!("{:?}", result);
    }

    #[test]
    fn test_semantic_search_result_clone() {
        let result = SemanticSearchResult {
            conversation_id: "conv-123".to_string(),
            message_id: "msg-456".to_string(),
            chunk_text: "Some text".to_string(),
            score: 0.95,
        };
        let cloned = result.clone();
        assert_eq!(cloned.score, 0.95);
    }
}
