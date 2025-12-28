//! DuckDB query interface for parquet files
//!
//! Provides SQL queries across multiple parquet files using DuckDB's glob support.

use super::{ParquetStorageConfig, Result, SearchResult, SemanticSearchResult};
use crate::providers::{Conversation, Message, MessageContent, Role};
use chrono::{DateTime, TimeZone, Utc};
use duckdb::{params, Connection};

/// DuckDB-based query interface for parquet files
pub struct DuckDbQuery {
    conn: Connection,
    config: ParquetStorageConfig,
}

impl DuckDbQuery {
    /// Create a new DuckDB query interface
    pub fn new(config: ParquetStorageConfig) -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Ok(Self { conn, config })
    }

    /// Create from an existing connection (for testing)
    pub fn with_connection(conn: Connection, config: ParquetStorageConfig) -> Self {
        Self { conn, config }
    }

    /// Query all conversations across all providers
    pub fn list_all_conversations(&self) -> Result<Vec<Conversation>> {
        let glob_pattern = self
            .config
            .base_dir
            .join("conversations")
            .join("*")
            .join("*.parquet");

        let glob_str = glob_pattern.to_string_lossy();

        // Check if any files exist
        if !self.has_parquet_files(&glob_str)? {
            return Ok(vec![]);
        }

        let mut stmt = self.conn.prepare(&format!(
            r#"
            SELECT DISTINCT
                conv_id,
                conv_provider_id,
                conv_title,
                conv_created_at,
                conv_updated_at,
                conv_model,
                conv_project_id,
                conv_project_name,
                conv_is_archived
            FROM read_parquet('{}')
            ORDER BY conv_updated_at DESC
            "#,
            glob_str
        ))?;

        let conversations = stmt
            .query_map([], |row| {
                Ok(Conversation {
                    id: row.get(0)?,
                    provider_id: row.get(1)?,
                    title: row.get(2)?,
                    created_at: Self::parse_timestamp(row.get::<_, i64>(3).ok()),
                    updated_at: Self::parse_timestamp(row.get::<_, i64>(4).ok()),
                    model: row.get(5).ok(),
                    project_id: row.get(6).ok(),
                    project_name: row.get(7).ok(),
                    is_archived: row.get::<_, bool>(8).unwrap_or(false),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(conversations)
    }

    /// Query conversations for a specific provider
    pub fn list_conversations_by_provider(&self, provider: &str) -> Result<Vec<Conversation>> {
        let glob_pattern = self
            .config
            .base_dir
            .join("conversations")
            .join(provider)
            .join("*.parquet");

        let glob_str = glob_pattern.to_string_lossy();

        if !self.has_parquet_files(&glob_str)? {
            return Ok(vec![]);
        }

        let mut stmt = self.conn.prepare(&format!(
            r#"
            SELECT DISTINCT
                conv_id,
                conv_provider_id,
                conv_title,
                conv_created_at,
                conv_updated_at,
                conv_model,
                conv_project_id,
                conv_project_name,
                conv_is_archived
            FROM read_parquet('{}')
            ORDER BY conv_updated_at DESC
            "#,
            glob_str
        ))?;

        let conversations = stmt
            .query_map([], |row| {
                Ok(Conversation {
                    id: row.get(0)?,
                    provider_id: row.get(1)?,
                    title: row.get(2)?,
                    created_at: Self::parse_timestamp(row.get::<_, i64>(3).ok()),
                    updated_at: Self::parse_timestamp(row.get::<_, i64>(4).ok()),
                    model: row.get(5).ok(),
                    project_id: row.get(6).ok(),
                    project_name: row.get(7).ok(),
                    is_archived: row.get::<_, bool>(8).unwrap_or(false),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(conversations)
    }

    /// Search messages across all conversations using LIKE pattern matching
    pub fn search_messages(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        let glob_pattern = self
            .config
            .base_dir
            .join("conversations")
            .join("*")
            .join("*.parquet");

        let glob_str = glob_pattern.to_string_lossy();

        if !self.has_parquet_files(&glob_str)? {
            return Ok(vec![]);
        }

        let search_pattern = format!("%{}%", query.replace('%', "\\%").replace('_', "\\_"));

        let mut stmt = self.conn.prepare(&format!(
            r#"
            SELECT
                conv_id,
                msg_content_json
            FROM read_parquet('{}')
            WHERE msg_content_json ILIKE ?
            LIMIT ?
            "#,
            glob_str
        ))?;

        let results = stmt
            .query_map(params![search_pattern, limit as i64], |row| {
                let conv_id: String = row.get(0)?;
                let content_json: String = row.get(1)?;

                // Extract snippet from content
                let snippet = Self::extract_snippet(&content_json, query);

                Ok(SearchResult {
                    conversation_id: conv_id,
                    snippet,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Get message count across all conversations
    pub fn count_messages(&self) -> Result<usize> {
        let glob_pattern = self
            .config
            .base_dir
            .join("conversations")
            .join("*")
            .join("*.parquet");

        let glob_str = glob_pattern.to_string_lossy();

        if !self.has_parquet_files(&glob_str)? {
            return Ok(0);
        }

        let count: i64 = self.conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM read_parquet('{}') WHERE msg_id != ''",
                glob_str
            ),
            [],
            |row| row.get(0),
        )?;

        Ok(count as usize)
    }

    /// Get conversation count
    pub fn count_conversations(&self) -> Result<usize> {
        let glob_pattern = self
            .config
            .base_dir
            .join("conversations")
            .join("*")
            .join("*.parquet");

        let glob_str = glob_pattern.to_string_lossy();

        if !self.has_parquet_files(&glob_str)? {
            return Ok(0);
        }

        let count: i64 = self.conn.query_row(
            &format!(
                "SELECT COUNT(DISTINCT conv_id) FROM read_parquet('{}')",
                glob_str
            ),
            [],
            |row| row.get(0),
        )?;

        Ok(count as usize)
    }

    /// Get messages for a specific conversation
    pub fn get_messages(&self, provider: &str, conversation_id: &str) -> Result<Vec<Message>> {
        let path = self.config.conversation_path(provider, conversation_id);

        if !path.exists() {
            return Ok(vec![]);
        }

        let path_str = path.to_string_lossy();

        let mut stmt = self.conn.prepare(&format!(
            r#"
            SELECT
                msg_id,
                conv_id,
                msg_parent_id,
                msg_role,
                msg_content_json,
                msg_created_at,
                msg_model
            FROM read_parquet('{}')
            WHERE msg_id != ''
            ORDER BY msg_created_at ASC
            "#,
            path_str
        ))?;

        let messages = stmt
            .query_map([], |row| {
                let role_str: String = row.get(3)?;
                let role = match role_str.as_str() {
                    "user" => Role::User,
                    "assistant" => Role::Assistant,
                    "system" => Role::System,
                    "tool" => Role::Tool,
                    _ => Role::User,
                };

                let content_json: String = row.get(4)?;
                let content: MessageContent = serde_json::from_str(&content_json)
                    .unwrap_or(MessageContent::Text { text: content_json });

                Ok(Message {
                    id: row.get(0)?,
                    conversation_id: row.get(1)?,
                    parent_id: row.get(2).ok(),
                    role,
                    content,
                    created_at: Some(Self::parse_timestamp(row.get::<_, i64>(5).ok())),
                    model: row.get(6).ok(),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(messages)
    }

    /// Check if any parquet files match the glob pattern
    fn has_parquet_files(&self, glob_pattern: &str) -> Result<bool> {
        // First, check if the parent directory exists
        // The glob pattern looks like /path/to/conversations/*/*.parquet
        // We need to check if /path/to/conversations exists
        let path = std::path::Path::new(glob_pattern);

        // Go up two levels from *.parquet to get the base conversations dir
        if let Some(parent) = path.parent().and_then(|p| p.parent()) {
            if !parent.exists() {
                return Ok(false);
            }
        }

        // Try to query the glob - if no files match, DuckDB will error
        let result = self.conn.query_row(
            &format!("SELECT COUNT(*) FROM read_parquet('{}') LIMIT 1", glob_pattern),
            [],
            |row| row.get::<_, i64>(0),
        );

        match result {
            Ok(_) => Ok(true),
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("No files found")
                    || err_str.contains("IO Error")
                    || err_str.contains("prepare is null")
                {
                    Ok(false)
                } else {
                    Err(e.into())
                }
            }
        }
    }

    /// Parse DuckDB timestamp (microseconds since epoch) to DateTime
    fn parse_timestamp(micros: Option<i64>) -> DateTime<Utc> {
        micros
            .and_then(|m| Utc.timestamp_millis_opt(m).single())
            .unwrap_or_else(Utc::now)
    }

    /// Extract a snippet around the search query from content JSON
    fn extract_snippet(content_json: &str, query: &str) -> String {
        // Try to parse as MessageContent and extract text
        let text = if let Ok(content) = serde_json::from_str::<MessageContent>(content_json) {
            match content {
                MessageContent::Text { text } => text,
                MessageContent::Code { code, .. } => code,
                MessageContent::Mixed { parts } => parts
                    .iter()
                    .filter_map(|p| match p {
                        MessageContent::Text { text } => Some(text.clone()),
                        MessageContent::Code { code, .. } => Some(code.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" "),
                _ => content_json.to_string(),
            }
        } else {
            content_json.to_string()
        };

        // Find the query position and extract context
        let lower_text = text.to_lowercase();
        let lower_query = query.to_lowercase();

        if let Some(pos) = lower_text.find(&lower_query) {
            let start = pos.saturating_sub(40);
            let end = (pos + query.len() + 40).min(text.len());

            let mut snippet = String::new();
            if start > 0 {
                snippet.push_str("...");
            }
            snippet.push_str(&text[start..end]);
            if end < text.len() {
                snippet.push_str("...");
            }
            snippet
        } else {
            // Just return first 80 chars
            if text.len() > 80 {
                format!("{}...", &text[..80])
            } else {
                text
            }
        }
    }

    /// Search embeddings by vector similarity
    ///
    /// Computes L2 distance between the query embedding and stored embeddings,
    /// returning the top-k most similar chunks.
    pub fn search_semantic(
        &self,
        query_embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<SemanticSearchResult>> {
        let glob_pattern = self
            .config
            .base_dir
            .join("embeddings")
            .join("*")
            .join("*.parquet");

        let glob_str = glob_pattern.to_string_lossy();

        // Check if any embedding files exist
        if !self.has_parquet_files(&glob_str)? {
            return Ok(vec![]);
        }

        // Convert query embedding to DuckDB list format
        let embedding_str = format!(
            "[{}]",
            query_embedding
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );

        // Query embeddings and compute L2 distance
        // DuckDB can compute list operations directly
        let sql = format!(
            r#"
            SELECT
                conversation_id,
                message_id,
                text,
                list_distance(embedding, {embedding}::FLOAT[384]) as distance
            FROM read_parquet('{glob}')
            ORDER BY distance ASC
            LIMIT {limit}
            "#,
            embedding = embedding_str,
            glob = glob_str,
            limit = limit
        );

        let mut stmt = self.conn.prepare(&sql)?;

        let results = stmt
            .query_map([], |row| {
                Ok(SemanticSearchResult {
                    conversation_id: row.get(0)?,
                    message_id: row.get(1)?,
                    chunk_text: row.get(2)?,
                    score: row.get(3)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(results)
    }

    /// Hybrid search combining FTS and vector similarity
    ///
    /// First performs keyword search to get candidates, then re-ranks by
    /// combining FTS score with vector similarity.
    pub fn search_hybrid(
        &self,
        query: &str,
        query_embedding: &[f32],
        limit: usize,
    ) -> Result<Vec<SemanticSearchResult>> {
        // Get FTS candidates (broader set)
        let fts_results = self.search_messages(query, limit * 3)?;

        if fts_results.is_empty() {
            // Fall back to pure semantic search
            return self.search_semantic(query_embedding, limit);
        }

        // Get semantic results
        let semantic_results = self.search_semantic(query_embedding, limit * 3)?;

        if semantic_results.is_empty() {
            // Convert FTS results to SemanticSearchResult
            return Ok(fts_results
                .into_iter()
                .take(limit)
                .map(|r| SemanticSearchResult {
                    conversation_id: r.conversation_id,
                    message_id: String::new(),
                    chunk_text: r.snippet,
                    score: 0.0,
                })
                .collect());
        }

        // Simple RRF (Reciprocal Rank Fusion) combining
        // Score = 1/(k + rank_fts) + 1/(k + rank_semantic)
        const K: f32 = 60.0;

        let mut combined: std::collections::HashMap<String, (String, String, f32)> =
            std::collections::HashMap::new();

        // Add FTS scores
        for (rank, result) in fts_results.iter().enumerate() {
            let score = 1.0 / (K + rank as f32);
            combined
                .entry(result.conversation_id.clone())
                .or_insert((String::new(), result.snippet.clone(), 0.0))
                .2 += score;
        }

        // Add semantic scores
        for (rank, result) in semantic_results.iter().enumerate() {
            let score = 1.0 / (K + rank as f32);
            let entry = combined
                .entry(result.conversation_id.clone())
                .or_insert((
                    result.message_id.clone(),
                    result.chunk_text.clone(),
                    0.0,
                ));
            entry.0 = result.message_id.clone();
            entry.1 = result.chunk_text.clone();
            entry.2 += score;
        }

        // Sort by combined score (descending)
        let mut results: Vec<_> = combined
            .into_iter()
            .map(|(conv_id, (msg_id, text, score))| SemanticSearchResult {
                conversation_id: conv_id,
                message_id: msg_id,
                chunk_text: text,
                score,
            })
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::parquet::ParquetStore;
    use std::path::Path;
    use tempfile::tempdir;

    fn create_test_conversation(id: &str, title: &str) -> Conversation {
        Conversation {
            id: id.to_string(),
            provider_id: "chatgpt".to_string(),
            title: title.to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: Some("gpt-4".to_string()),
            project_id: None,
            project_name: None,
            is_archived: false,
        }
    }

    fn create_test_message(conversation_id: &str, id: &str, text: &str) -> Message {
        Message {
            id: id.to_string(),
            conversation_id: conversation_id.to_string(),
            parent_id: None,
            role: Role::User,
            content: MessageContent::Text {
                text: text.to_string(),
            },
            created_at: Some(Utc::now()),
            model: None,
        }
    }

    fn setup_test_data(dir: &Path) -> ParquetStorageConfig {
        let config = ParquetStorageConfig::new(dir);
        let store = ParquetStore::new(config.clone());

        // Create some test conversations
        let conv1 = create_test_conversation("conv-1", "First Conversation");
        let messages1 = vec![
            create_test_message("conv-1", "msg-1", "Hello world"),
            create_test_message("conv-1", "msg-2", "How are you doing today?"),
        ];
        store.write_conversation("user-123", &conv1, &messages1).unwrap();

        let conv2 = create_test_conversation("conv-2", "Second Conversation");
        let messages2 = vec![
            create_test_message("conv-2", "msg-3", "This is a test message"),
            create_test_message("conv-2", "msg-4", "Testing search functionality"),
        ];
        store.write_conversation("user-123", &conv2, &messages2).unwrap();

        config
    }

    #[test]
    fn test_list_all_conversations() {
        let dir = tempdir().unwrap();
        let config = setup_test_data(dir.path());
        let query = DuckDbQuery::new(config).unwrap();

        let conversations = query.list_all_conversations().unwrap();
        assert_eq!(conversations.len(), 2);
    }

    #[test]
    fn test_list_conversations_by_provider() {
        let dir = tempdir().unwrap();
        let config = setup_test_data(dir.path());
        let query = DuckDbQuery::new(config).unwrap();

        let conversations = query.list_conversations_by_provider("chatgpt").unwrap();
        assert_eq!(conversations.len(), 2);

        let empty = query.list_conversations_by_provider("claude").unwrap();
        assert!(empty.is_empty());
    }

    #[test]
    fn test_search_messages() {
        let dir = tempdir().unwrap();
        let config = setup_test_data(dir.path());
        let query = DuckDbQuery::new(config).unwrap();

        let results = query.search_messages("test", 10).unwrap();
        assert!(!results.is_empty());
        assert!(results.iter().any(|r| r.snippet.to_lowercase().contains("test")));
    }

    #[test]
    fn test_search_messages_no_results() {
        let dir = tempdir().unwrap();
        let config = setup_test_data(dir.path());
        let query = DuckDbQuery::new(config).unwrap();

        let results = query.search_messages("xyznonexistent", 10).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_count_messages() {
        let dir = tempdir().unwrap();
        let config = setup_test_data(dir.path());
        let query = DuckDbQuery::new(config).unwrap();

        let count = query.count_messages().unwrap();
        assert_eq!(count, 4); // 2 conversations x 2 messages each
    }

    #[test]
    fn test_count_conversations() {
        let dir = tempdir().unwrap();
        let config = setup_test_data(dir.path());
        let query = DuckDbQuery::new(config).unwrap();

        let count = query.count_conversations().unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_get_messages() {
        let dir = tempdir().unwrap();
        let config = setup_test_data(dir.path());
        let query = DuckDbQuery::new(config).unwrap();

        let messages = query.get_messages("chatgpt", "conv-1").unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].id, "msg-1");
    }

    #[test]
    fn test_get_messages_nonexistent() {
        let dir = tempdir().unwrap();
        let config = setup_test_data(dir.path());
        let query = DuckDbQuery::new(config).unwrap();

        let messages = query.get_messages("chatgpt", "nonexistent").unwrap();
        assert!(messages.is_empty());
    }

    #[test]
    fn test_empty_database() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let query = DuckDbQuery::new(config).unwrap();

        // All queries should return empty, not error
        assert!(query.list_all_conversations().unwrap().is_empty());
        assert!(query.search_messages("test", 10).unwrap().is_empty());
        assert_eq!(query.count_messages().unwrap(), 0);
        assert_eq!(query.count_conversations().unwrap(), 0);
    }

    #[test]
    fn test_extract_snippet() {
        let content = r#"{"text": "This is a test message with some content"}"#;
        let snippet = DuckDbQuery::extract_snippet(content, "test");
        assert!(snippet.contains("test"));
    }
}
