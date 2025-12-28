//! Local storage for quaid using SQLite
//!
//! Stores conversations, messages, and attachments with full-text search support.

pub mod duckdb;
pub mod embeddings;
pub mod parquet;
pub mod traits;

pub use embeddings::EmbeddingsStore;
pub use traits::*;

use crate::providers::{Account, Attachment, Conversation, Message, ProviderId};
use rusqlite::{params, Connection, Result as SqliteResult};
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("DuckDB error: {0}")]
    DuckDb(#[from] ::duckdb::Error),

    #[error("Parquet error: {0}")]
    Parquet(String),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Main storage interface
pub struct Store {
    conn: Connection,
}

impl Store {
    /// Open or create a store at the given path
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    /// Create an in-memory store (for testing)
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let store = Self { conn };
        store.migrate()?;
        Ok(store)
    }

    /// Run database migrations
    fn migrate(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            -- Accounts table
            CREATE TABLE IF NOT EXISTS accounts (
                id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                email TEXT NOT NULL,
                name TEXT,
                avatar_url TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(provider, email)
            );

            -- Conversations table
            CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                title TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                model TEXT,
                project_id TEXT,
                project_name TEXT,
                is_archived INTEGER DEFAULT 0,
                raw_json TEXT,
                FOREIGN KEY (account_id) REFERENCES accounts(id)
            );

            -- Messages table
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                parent_id TEXT,
                role TEXT NOT NULL,
                content_type TEXT NOT NULL,
                content_json TEXT NOT NULL,
                created_at TEXT,
                model TEXT,
                raw_json TEXT,
                FOREIGN KEY (conversation_id) REFERENCES conversations(id)
            );

            -- Attachments table
            CREATE TABLE IF NOT EXISTS attachments (
                id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL,
                filename TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                download_url TEXT NOT NULL,
                local_path TEXT,
                downloaded_at TEXT,
                FOREIGN KEY (message_id) REFERENCES messages(id)
            );

            -- Full-text search on messages
            CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
                content,
                conversation_id
            );

            -- Note: FTS is populated manually via save_message, not triggers
            -- This avoids issues with json_extract on complex content types

            -- Indexes
            CREATE INDEX IF NOT EXISTS idx_conversations_account ON conversations(account_id);
            CREATE INDEX IF NOT EXISTS idx_conversations_updated ON conversations(updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id);
            CREATE INDEX IF NOT EXISTS idx_attachments_message ON attachments(message_id);
            "#,
        )?;
        Ok(())
    }

    // Account operations

    pub fn save_account(&self, account: &Account) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO accounts (id, provider, email, name, avatar_url)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
                email = excluded.email,
                name = excluded.name,
                avatar_url = excluded.avatar_url
            "#,
            params![
                account.id,
                account.provider.to_string(),
                account.email,
                account.name,
                account.avatar_url,
            ],
        )?;
        Ok(())
    }

    pub fn get_account(&self, provider: &ProviderId, email: &str) -> Result<Option<Account>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, provider, email, name, avatar_url FROM accounts WHERE provider = ?1 AND email = ?2"
        )?;

        let result = stmt.query_row(params![provider.to_string(), email], |row| {
            Ok(Account {
                id: row.get(0)?,
                provider: ProviderId(row.get(1)?),
                email: row.get(2)?,
                name: row.get(3)?,
                avatar_url: row.get(4)?,
            })
        });

        match result {
            Ok(account) => Ok(Some(account)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_accounts(&self) -> Result<Vec<Account>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, provider, email, name, avatar_url FROM accounts")?;

        let accounts = stmt
            .query_map([], |row| {
                Ok(Account {
                    id: row.get(0)?,
                    provider: ProviderId(row.get(1)?),
                    email: row.get(2)?,
                    name: row.get(3)?,
                    avatar_url: row.get(4)?,
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        Ok(accounts)
    }

    // Conversation operations

    pub fn save_conversation(&self, account_id: &str, conv: &Conversation) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO conversations (id, account_id, provider_id, title, created_at, updated_at, model, project_id, project_name, is_archived)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(id) DO UPDATE SET
                title = excluded.title,
                updated_at = excluded.updated_at,
                model = excluded.model,
                is_archived = excluded.is_archived
            "#,
            params![
                conv.id,
                account_id,
                conv.provider_id,
                conv.title,
                conv.created_at.to_rfc3339(),
                conv.updated_at.to_rfc3339(),
                conv.model,
                conv.project_id,
                conv.project_name,
                conv.is_archived as i32,
            ],
        )?;
        Ok(())
    }

    /// Get just the updated_at timestamp for a conversation (for incremental sync)
    pub fn get_conversation_updated_at(
        &self,
        id: &str,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
        let result = self.conn.query_row(
            "SELECT updated_at FROM conversations WHERE id = ?1",
            params![id],
            |row| {
                let updated_at: String = row.get(0)?;
                Ok(chrono::DateTime::parse_from_rfc3339(&updated_at)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .ok())
            },
        );

        match result {
            Ok(dt) => Ok(dt),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn get_conversation(&self, id: &str) -> Result<Option<Conversation>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, provider_id, title, created_at, updated_at, model, project_id, project_name, is_archived
             FROM conversations WHERE id = ?1"
        )?;

        let result = stmt.query_row(params![id], |row| {
            Ok(Conversation {
                id: row.get(0)?,
                provider_id: row.get(1)?,
                title: row.get(2)?,
                created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(3)?)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(|_| chrono::Utc::now()),
                updated_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)
                    .map(|dt| dt.with_timezone(&chrono::Utc))
                    .unwrap_or_else(|_| chrono::Utc::now()),
                model: row.get(5)?,
                project_id: row.get(6)?,
                project_name: row.get(7)?,
                is_archived: row.get::<_, i32>(8)? != 0,
            })
        });

        match result {
            Ok(conv) => Ok(Some(conv)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_conversations(&self, account_id: &str) -> Result<Vec<Conversation>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, provider_id, title, created_at, updated_at, model, project_id, project_name, is_archived
             FROM conversations WHERE account_id = ?1 ORDER BY updated_at DESC"
        )?;

        let convs = stmt
            .query_map(params![account_id], |row| {
                Ok(Conversation {
                    id: row.get(0)?,
                    provider_id: row.get(1)?,
                    title: row.get(2)?,
                    created_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(3)?)
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .unwrap_or_else(|_| chrono::Utc::now()),
                    updated_at: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(4)?)
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .unwrap_or_else(|_| chrono::Utc::now()),
                    model: row.get(5)?,
                    project_id: row.get(6)?,
                    project_name: row.get(7)?,
                    is_archived: row.get::<_, i32>(8)? != 0,
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        Ok(convs)
    }

    // Message operations

    pub fn save_message(&self, message: &Message) -> Result<()> {
        let content_json = serde_json::to_string(&message.content)?;
        let content_type = match &message.content {
            crate::providers::MessageContent::Text { .. } => "text",
            crate::providers::MessageContent::Code { .. } => "code",
            crate::providers::MessageContent::Image { .. } => "image",
            crate::providers::MessageContent::Audio { .. } => "audio",
            crate::providers::MessageContent::Mixed { .. } => "mixed",
        };

        // Extract text content for FTS indexing
        let text_content = extract_text_content(&message.content);

        self.conn.execute(
            r#"
            INSERT INTO messages (id, conversation_id, parent_id, role, content_type, content_json, created_at, model)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(id) DO UPDATE SET
                content_json = excluded.content_json
            "#,
            params![
                message.id,
                message.conversation_id,
                message.parent_id,
                format!("{:?}", message.role).to_lowercase(),
                content_type,
                content_json,
                message.created_at.map(|dt| dt.to_rfc3339()),
                message.model,
            ],
        )?;

        // Update FTS index
        if !text_content.is_empty() {
            self.conn.execute(
                "INSERT OR REPLACE INTO messages_fts (rowid, content, conversation_id)
                 SELECT rowid, ?1, ?2 FROM messages WHERE id = ?3",
                params![text_content, message.conversation_id, message.id],
            )?;
        }

        Ok(())
    }

    pub fn get_messages(&self, conversation_id: &str) -> Result<Vec<Message>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, conversation_id, parent_id, role, content_json, created_at, model
             FROM messages WHERE conversation_id = ?1 ORDER BY created_at ASC",
        )?;

        let messages = stmt
            .query_map(params![conversation_id], |row| {
                let role_str: String = row.get(3)?;
                let role = match role_str.as_str() {
                    "user" => crate::providers::Role::User,
                    "assistant" => crate::providers::Role::Assistant,
                    "system" => crate::providers::Role::System,
                    "tool" => crate::providers::Role::Tool,
                    _ => crate::providers::Role::User,
                };

                let content_json: String = row.get(4)?;
                let content: crate::providers::MessageContent =
                    serde_json::from_str(&content_json).unwrap_or(crate::providers::MessageContent::Text {
                        text: content_json,
                    });

                let created_at: Option<String> = row.get(5)?;
                let created_at = created_at.and_then(|s| {
                    chrono::DateTime::parse_from_rfc3339(&s)
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                        .ok()
                });

                Ok(Message {
                    id: row.get(0)?,
                    conversation_id: row.get(1)?,
                    parent_id: row.get(2)?,
                    role,
                    content,
                    created_at,
                    model: row.get(6)?,
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        Ok(messages)
    }

    // Search operations

    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT m.conversation_id, snippet(messages_fts, 0, '<mark>', '</mark>', '...', 32) as snippet
            FROM messages_fts
            JOIN messages m ON messages_fts.rowid = m.rowid
            WHERE messages_fts MATCH ?1
            ORDER BY rank
            LIMIT ?2
            "#,
        )?;

        let results = stmt
            .query_map(params![query, limit as i64], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        Ok(results)
    }

    // Attachment operations

    pub fn save_attachment(&self, attachment: &Attachment) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO attachments (id, message_id, filename, mime_type, size_bytes, download_url)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(id) DO NOTHING
            "#,
            params![
                attachment.id,
                attachment.message_id,
                attachment.filename,
                attachment.mime_type,
                attachment.size_bytes as i64,
                attachment.download_url,
            ],
        )?;
        Ok(())
    }

    pub fn mark_attachment_downloaded(&self, id: &str, local_path: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE attachments SET local_path = ?1, downloaded_at = CURRENT_TIMESTAMP WHERE id = ?2",
            params![local_path, id],
        )?;
        Ok(())
    }

    pub fn get_pending_attachments(&self) -> Result<Vec<Attachment>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, message_id, filename, mime_type, size_bytes, download_url
             FROM attachments WHERE local_path IS NULL",
        )?;

        let attachments = stmt
            .query_map([], |row| {
                Ok(Attachment {
                    id: row.get(0)?,
                    message_id: row.get(1)?,
                    filename: row.get(2)?,
                    mime_type: row.get(3)?,
                    size_bytes: row.get::<_, i64>(4)? as u64,
                    download_url: row.get(5)?,
                })
            })?
            .collect::<SqliteResult<Vec<_>>>()?;

        Ok(attachments)
    }

    // Stats

    pub fn stats(&self) -> Result<StoreStats> {
        let accounts: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM accounts", [], |row| row.get(0))?;
        let conversations: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM conversations", [], |row| row.get(0))?;
        let messages: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))?;
        let attachments: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM attachments", [], |row| row.get(0))?;

        Ok(StoreStats {
            accounts: accounts as usize,
            conversations: conversations as usize,
            messages: messages as usize,
            attachments: attachments as usize,
        })
    }
}

#[derive(Debug, Clone)]
pub struct StoreStats {
    pub accounts: usize,
    pub conversations: usize,
    pub messages: usize,
    pub attachments: usize,
}

/// Extract searchable text from message content
fn extract_text_content(content: &crate::providers::MessageContent) -> String {
    match content {
        crate::providers::MessageContent::Text { text } => text.clone(),
        crate::providers::MessageContent::Code { code, .. } => code.clone(),
        crate::providers::MessageContent::Image { alt, .. } => alt.clone().unwrap_or_default(),
        crate::providers::MessageContent::Audio { transcript, .. } => {
            transcript.clone().unwrap_or_default()
        }
        crate::providers::MessageContent::Mixed { parts } => parts
            .iter()
            .map(extract_text_content)
            .collect::<Vec<_>>()
            .join(" "),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::MessageContent;

    fn create_test_account() -> Account {
        Account {
            id: "user-123".to_string(),
            provider: ProviderId::chatgpt(),
            email: "test@example.com".to_string(),
            name: Some("Test User".to_string()),
            avatar_url: None,
        }
    }

    fn create_test_conversation() -> Conversation {
        Conversation {
            id: "conv-123".to_string(),
            provider_id: "chatgpt".to_string(),
            title: "Test Conversation".to_string(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            model: Some("gpt-4".to_string()),
            project_id: None,
            project_name: None,
            is_archived: false,
        }
    }

    fn create_test_message(conversation_id: &str) -> Message {
        Message {
            id: "msg-123".to_string(),
            conversation_id: conversation_id.to_string(),
            parent_id: None,
            role: crate::providers::Role::User,
            content: MessageContent::Text {
                text: "Hello, world!".to_string(),
            },
            created_at: Some(chrono::Utc::now()),
            model: None,
        }
    }

    #[test]
    fn test_store_creation() {
        let store = Store::in_memory().unwrap();
        let stats = store.stats().unwrap();
        assert_eq!(stats.accounts, 0);
        assert_eq!(stats.conversations, 0);
    }

    #[test]
    fn test_save_and_get_account() {
        let store = Store::in_memory().unwrap();
        let account = create_test_account();

        store.save_account(&account).unwrap();

        let retrieved = store
            .get_account(&ProviderId::chatgpt(), "test@example.com")
            .unwrap()
            .unwrap();

        assert_eq!(retrieved.id, account.id);
        assert_eq!(retrieved.email, account.email);
    }

    #[test]
    fn test_list_accounts() {
        let store = Store::in_memory().unwrap();

        let account1 = create_test_account();
        let mut account2 = create_test_account();
        account2.id = "user-456".to_string();
        account2.email = "other@example.com".to_string();

        store.save_account(&account1).unwrap();
        store.save_account(&account2).unwrap();

        let accounts = store.list_accounts().unwrap();
        assert_eq!(accounts.len(), 2);
    }

    #[test]
    fn test_save_and_get_conversation() {
        let store = Store::in_memory().unwrap();
        let account = create_test_account();
        store.save_account(&account).unwrap();

        let conv = create_test_conversation();
        store.save_conversation(&account.id, &conv).unwrap();

        let retrieved = store.get_conversation(&conv.id).unwrap().unwrap();
        assert_eq!(retrieved.id, conv.id);
        assert_eq!(retrieved.title, conv.title);
    }

    #[test]
    fn test_list_conversations() {
        let store = Store::in_memory().unwrap();
        let account = create_test_account();
        store.save_account(&account).unwrap();

        let conv1 = create_test_conversation();
        let mut conv2 = create_test_conversation();
        conv2.id = "conv-456".to_string();
        conv2.title = "Another Conversation".to_string();

        store.save_conversation(&account.id, &conv1).unwrap();
        store.save_conversation(&account.id, &conv2).unwrap();

        let convs = store.list_conversations(&account.id).unwrap();
        assert_eq!(convs.len(), 2);
    }

    #[test]
    fn test_save_and_get_messages() {
        let store = Store::in_memory().unwrap();
        let account = create_test_account();
        store.save_account(&account).unwrap();

        let conv = create_test_conversation();
        store.save_conversation(&account.id, &conv).unwrap();

        let msg = create_test_message(&conv.id);
        store.save_message(&msg).unwrap();

        let messages = store.get_messages(&conv.id).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, msg.id);
    }

    #[test]
    fn test_search_messages() {
        let store = Store::in_memory().unwrap();
        let account = create_test_account();
        store.save_account(&account).unwrap();

        let conv = create_test_conversation();
        store.save_conversation(&account.id, &conv).unwrap();

        let msg = create_test_message(&conv.id);
        store.save_message(&msg).unwrap();

        let results = store.search("hello", 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, conv.id);
    }

    #[test]
    fn test_attachment_workflow() {
        let store = Store::in_memory().unwrap();
        let account = create_test_account();
        store.save_account(&account).unwrap();

        let conv = create_test_conversation();
        store.save_conversation(&account.id, &conv).unwrap();

        let msg = create_test_message(&conv.id);
        store.save_message(&msg).unwrap();

        let attachment = Attachment {
            id: "att-123".to_string(),
            message_id: msg.id.clone(),
            filename: "image.png".to_string(),
            mime_type: "image/png".to_string(),
            size_bytes: 1024,
            download_url: "file-service://abc123".to_string(),
        };
        store.save_attachment(&attachment).unwrap();

        let pending = store.get_pending_attachments().unwrap();
        assert_eq!(pending.len(), 1);

        store
            .mark_attachment_downloaded(&attachment.id, "/path/to/image.png")
            .unwrap();

        let pending = store.get_pending_attachments().unwrap();
        assert_eq!(pending.len(), 0);
    }

    #[test]
    fn test_stats() {
        let store = Store::in_memory().unwrap();
        let account = create_test_account();
        store.save_account(&account).unwrap();

        let conv = create_test_conversation();
        store.save_conversation(&account.id, &conv).unwrap();

        let msg = create_test_message(&conv.id);
        store.save_message(&msg).unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.accounts, 1);
        assert_eq!(stats.conversations, 1);
        assert_eq!(stats.messages, 1);
    }
}
