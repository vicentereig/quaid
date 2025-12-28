pub mod chatgpt;
pub mod claude;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProviderError {
    #[error("Authentication required")]
    AuthRequired,

    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    #[error("Token expired")]
    TokenExpired,

    #[error("Rate limited, retry after {0} seconds")]
    RateLimited(u64),

    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("API error: {0}")]
    Api(String),

    #[error("Parse error: {0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, ProviderError>;

/// Unique identifier for a provider (e.g., "chatgpt", "claude", "gemini")
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProviderId(pub String);

impl ProviderId {
    pub fn chatgpt() -> Self {
        Self("chatgpt".to_string())
    }

    pub fn claude() -> Self {
        Self("claude".to_string())
    }
}

impl std::fmt::Display for ProviderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Account information for a provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub id: String,
    pub provider: ProviderId,
    pub email: String,
    pub name: Option<String>,
    pub avatar_url: Option<String>,
}

/// A conversation from any provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conversation {
    pub id: String,
    pub provider_id: String,
    pub title: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub model: Option<String>,
    pub project_id: Option<String>,
    pub project_name: Option<String>,
    pub is_archived: bool,
}

/// A message within a conversation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub conversation_id: String,
    pub parent_id: Option<String>,
    pub role: Role,
    pub content: MessageContent,
    pub created_at: Option<DateTime<Utc>>,
    pub model: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
    System,
    Tool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageContent {
    Text { text: String },
    Code { language: String, code: String },
    Image { url: String, alt: Option<String> },
    Audio { url: String, transcript: Option<String> },
    Mixed { parts: Vec<MessageContent> },
}

/// Attachment metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment {
    pub id: String,
    pub message_id: String,
    pub filename: String,
    pub mime_type: String,
    pub size_bytes: u64,
    pub download_url: String,
}

/// Progress callback for long-running operations
pub type ProgressCallback = Box<dyn Fn(usize, usize) + Send + Sync>;

/// The main trait that all providers must implement
#[async_trait]
pub trait Provider: Send + Sync {
    /// Get the provider identifier
    fn id(&self) -> ProviderId;

    /// Check if the provider is authenticated
    async fn is_authenticated(&self) -> bool;

    /// Authenticate the user (opens browser for OAuth flow)
    async fn authenticate(&mut self) -> Result<Account>;

    /// Get the currently authenticated account
    async fn account(&self) -> Result<Account>;

    /// List all conversations (paginated internally)
    async fn conversations(&self) -> Result<Vec<Conversation>>;

    /// Get a single conversation with all messages
    async fn conversation(&self, id: &str) -> Result<(Conversation, Vec<Message>)>;

    /// Get conversations for a specific project
    async fn project_conversations(&self, project_id: &str) -> Result<Vec<Conversation>>;

    /// Download an attachment to a local path
    async fn download_attachment(&self, attachment: &Attachment, path: &std::path::Path)
        -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_id_display() {
        let id = ProviderId::chatgpt();
        assert_eq!(id.to_string(), "chatgpt");
    }

    #[test]
    fn test_provider_id_equality() {
        assert_eq!(ProviderId::chatgpt(), ProviderId::chatgpt());
        assert_ne!(ProviderId::chatgpt(), ProviderId::claude());
    }

    #[test]
    fn test_role_serialization() {
        let user = Role::User;
        let json = serde_json::to_string(&user).unwrap();
        assert_eq!(json, "\"user\"");

        let parsed: Role = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, Role::User);
    }

    #[test]
    fn test_message_content_text() {
        let content = MessageContent::Text {
            text: "Hello, world!".to_string(),
        };
        let json = serde_json::to_string(&content).unwrap();
        assert!(json.contains("\"type\":\"text\""));
        assert!(json.contains("\"text\":\"Hello, world!\""));
    }

    #[test]
    fn test_message_content_mixed() {
        let content = MessageContent::Mixed {
            parts: vec![
                MessageContent::Text {
                    text: "Check out this image:".to_string(),
                },
                MessageContent::Image {
                    url: "https://example.com/image.png".to_string(),
                    alt: Some("Example".to_string()),
                },
            ],
        };
        let json = serde_json::to_string(&content).unwrap();
        assert!(json.contains("\"type\":\"mixed\""));
        assert!(json.contains("\"parts\""));
    }

    #[test]
    fn test_conversation_serialization() {
        let conv = Conversation {
            id: "conv-123".to_string(),
            provider_id: "chatgpt".to_string(),
            title: "Test conversation".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: Some("gpt-4".to_string()),
            project_id: None,
            project_name: None,
            is_archived: false,
        };

        let json = serde_json::to_string(&conv).unwrap();
        let parsed: Conversation = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, conv.id);
        assert_eq!(parsed.title, conv.title);
    }

    #[test]
    fn test_provider_error_display() {
        let err = ProviderError::AuthRequired;
        assert_eq!(err.to_string(), "Authentication required");

        let err = ProviderError::RateLimited(60);
        assert_eq!(err.to_string(), "Rate limited, retry after 60 seconds");
    }
}
