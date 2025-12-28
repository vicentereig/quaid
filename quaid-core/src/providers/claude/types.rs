//! Claude.ai API types based on reverse-engineered API
//!
//! These types are based on the claude.ai-ultimate-chat-exporter project:
//! https://github.com/GeoAnima/claude.ai-ultimate-chat-exporter

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Organization from /api/organizations
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiOrganization {
    pub uuid: String,
    pub name: Option<String>,
    #[serde(default)]
    pub settings: Option<serde_json::Value>,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

/// Conversation item from /api/organizations/{org}/chat_conversations
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiConversationItem {
    pub uuid: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub is_starred: bool,
    #[serde(default)]
    pub project_uuid: Option<String>,
}

/// Full conversation from /api/organizations/{org}/chat_conversations/{id}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiConversation {
    pub uuid: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub chat_messages: Vec<ApiChatMessage>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub project_uuid: Option<String>,
}

/// A message in a Claude conversation
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiChatMessage {
    pub uuid: String,
    pub sender: String, // "human" or "assistant"
    pub text: String,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub attachments: Vec<ApiAttachment>,
    #[serde(default)]
    pub files: Vec<ApiFile>,
    #[serde(default)]
    pub content: Vec<ApiContentBlock>,
}

/// Content block in a message (for structured content like artifacts)
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApiContentBlock {
    Text {
        text: String,
    },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        content: serde_json::Value,
    },
    #[serde(other)]
    Unknown,
}

/// Attachment metadata
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiAttachment {
    pub id: Option<String>,
    pub file_name: String,
    pub file_size: Option<u64>,
    pub file_type: Option<String>,
    #[serde(default)]
    pub extracted_content: Option<String>,
}

/// File reference
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiFile {
    pub file_uuid: Option<String>,
    pub file_name: String,
    pub file_size: Option<u64>,
    pub file_type: Option<String>,
}

/// Account/user info (from session or account endpoint)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiAccount {
    pub uuid: String,
    pub email: Option<String>,
    #[serde(alias = "full_name")]
    pub name: Option<String>,
    #[serde(default)]
    pub avatar_url: Option<String>,
}

/// Project info
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiProject {
    pub uuid: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_organization() {
        let json = r#"{
            "uuid": "org-123",
            "name": "Personal",
            "capabilities": ["chat", "artifacts"]
        }"#;

        let org: ApiOrganization = serde_json::from_str(json).unwrap();
        assert_eq!(org.uuid, "org-123");
        assert_eq!(org.name, Some("Personal".to_string()));
        assert_eq!(org.capabilities.len(), 2);
    }

    #[test]
    fn test_parse_organization_minimal() {
        let json = r#"{"uuid": "org-456"}"#;

        let org: ApiOrganization = serde_json::from_str(json).unwrap();
        assert_eq!(org.uuid, "org-456");
        assert!(org.name.is_none());
        assert!(org.capabilities.is_empty());
    }

    #[test]
    fn test_parse_conversation_item() {
        let json = r#"{
            "uuid": "conv-123",
            "name": "Test Chat",
            "created_at": "2025-01-15T10:30:00Z",
            "updated_at": "2025-01-15T11:00:00Z",
            "summary": "A test conversation",
            "model": "claude-3-opus-20240229",
            "is_starred": true
        }"#;

        let conv: ApiConversationItem = serde_json::from_str(json).unwrap();
        assert_eq!(conv.uuid, "conv-123");
        assert_eq!(conv.name, "Test Chat");
        assert!(conv.is_starred);
        assert_eq!(conv.model, Some("claude-3-opus-20240229".to_string()));
    }

    #[test]
    fn test_parse_conversation_item_minimal() {
        let json = r#"{
            "uuid": "conv-456",
            "name": "Minimal Chat",
            "created_at": "2025-01-15T10:30:00Z",
            "updated_at": "2025-01-15T10:30:00Z"
        }"#;

        let conv: ApiConversationItem = serde_json::from_str(json).unwrap();
        assert_eq!(conv.uuid, "conv-456");
        assert!(!conv.is_starred);
        assert!(conv.model.is_none());
    }

    #[test]
    fn test_parse_full_conversation() {
        let json = r#"{
            "uuid": "conv-789",
            "name": "Full Chat",
            "created_at": "2025-01-15T10:30:00Z",
            "updated_at": "2025-01-15T11:00:00Z",
            "chat_messages": [
                {
                    "uuid": "msg-1",
                    "sender": "human",
                    "text": "Hello, Claude!",
                    "created_at": "2025-01-15T10:30:00Z",
                    "attachments": [],
                    "files": [],
                    "content": []
                },
                {
                    "uuid": "msg-2",
                    "sender": "assistant",
                    "text": "Hello! How can I help you today?",
                    "created_at": "2025-01-15T10:30:05Z",
                    "attachments": [],
                    "files": [],
                    "content": []
                }
            ]
        }"#;

        let conv: ApiConversation = serde_json::from_str(json).unwrap();
        assert_eq!(conv.uuid, "conv-789");
        assert_eq!(conv.chat_messages.len(), 2);
        assert_eq!(conv.chat_messages[0].sender, "human");
        assert_eq!(conv.chat_messages[1].sender, "assistant");
    }

    #[test]
    fn test_parse_message_with_attachments() {
        let json = r#"{
            "uuid": "msg-1",
            "sender": "human",
            "text": "Check this file",
            "attachments": [
                {
                    "id": "att-1",
                    "file_name": "document.pdf",
                    "file_size": 1024,
                    "file_type": "application/pdf"
                }
            ],
            "files": [],
            "content": []
        }"#;

        let msg: ApiChatMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.attachments.len(), 1);
        assert_eq!(msg.attachments[0].file_name, "document.pdf");
    }

    #[test]
    fn test_parse_content_blocks() {
        let json = r#"{
            "uuid": "msg-1",
            "sender": "assistant",
            "text": "Here's some code",
            "attachments": [],
            "files": [],
            "content": [
                {"type": "text", "text": "Let me help you with that."},
                {"type": "tool_use", "id": "tool-1", "name": "code_editor", "input": {"code": "print('hello')"}}
            ]
        }"#;

        let msg: ApiChatMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.content.len(), 2);

        match &msg.content[0] {
            ApiContentBlock::Text { text } => assert!(text.contains("help")),
            _ => panic!("Expected Text block"),
        }

        match &msg.content[1] {
            ApiContentBlock::ToolUse { name, .. } => assert_eq!(name, "code_editor"),
            _ => panic!("Expected ToolUse block"),
        }
    }

    #[test]
    fn test_parse_account() {
        let json = r#"{
            "uuid": "user-123",
            "email": "test@example.com",
            "full_name": "Test User",
            "avatar_url": "https://example.com/avatar.png"
        }"#;

        let account: ApiAccount = serde_json::from_str(json).unwrap();
        assert_eq!(account.uuid, "user-123");
        assert_eq!(account.email, Some("test@example.com".to_string()));
        assert_eq!(account.name, Some("Test User".to_string()));
    }

    #[test]
    fn test_parse_project() {
        let json = r#"{
            "uuid": "proj-123",
            "name": "My Project",
            "description": "A test project",
            "created_at": "2025-01-15T10:30:00Z",
            "updated_at": "2025-01-15T11:00:00Z"
        }"#;

        let project: ApiProject = serde_json::from_str(json).unwrap();
        assert_eq!(project.uuid, "proj-123");
        assert_eq!(project.name, "My Project");
        assert_eq!(project.description, Some("A test project".to_string()));
    }

    #[test]
    fn test_sender_roles() {
        // Verify we handle both sender types correctly
        let human_json = r#"{
            "uuid": "msg-1",
            "sender": "human",
            "text": "Hello",
            "attachments": [],
            "files": [],
            "content": []
        }"#;

        let assistant_json = r#"{
            "uuid": "msg-2",
            "sender": "assistant",
            "text": "Hi there!",
            "attachments": [],
            "files": [],
            "content": []
        }"#;

        let human: ApiChatMessage = serde_json::from_str(human_json).unwrap();
        let assistant: ApiChatMessage = serde_json::from_str(assistant_json).unwrap();

        assert_eq!(human.sender, "human");
        assert_eq!(assistant.sender, "assistant");
    }

    #[test]
    fn test_unknown_content_block() {
        // Should handle unknown content types gracefully
        let json = r#"{
            "uuid": "msg-1",
            "sender": "assistant",
            "text": "Test",
            "attachments": [],
            "files": [],
            "content": [
                {"type": "some_future_type", "data": "whatever"}
            ]
        }"#;

        let msg: ApiChatMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.content.len(), 1);
        matches!(msg.content[0], ApiContentBlock::Unknown);
    }
}
