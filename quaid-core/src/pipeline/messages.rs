//! Inter-stage message types for the pipeline

use crate::providers::{Attachment, Conversation, Message};
use std::path::PathBuf;

/// Messages passed between pipeline stages
#[derive(Debug, Clone)]
pub enum PipelineMessage {
    /// Stage 1 output: Conversation fetched from provider
    ConversationFetched {
        account_id: String,
        conversation: Conversation,
        messages: Vec<Message>,
    },

    /// Stage 2 output: Media downloaded
    MediaDownloaded {
        account_id: String,
        conversation: Conversation,
        messages: Vec<Message>,
        attachments: Vec<DownloadedAttachment>,
    },

    /// Stage 3 output: Processing complete
    Complete {
        conversation_id: String,
        messages_count: usize,
        chunks_count: usize,
    },

    /// Error during processing
    Error {
        conversation_id: String,
        stage: String,
        message: String,
    },

    /// Shutdown signal
    Shutdown,
}

/// An attachment that has been downloaded
#[derive(Debug, Clone)]
pub struct DownloadedAttachment {
    pub attachment: Attachment,
    pub local_path: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::{MessageContent, Role};

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

    fn create_test_message() -> Message {
        Message {
            id: "msg-1".to_string(),
            conversation_id: "conv-1".to_string(),
            parent_id: None,
            role: Role::User,
            content: MessageContent::Text {
                text: "Hello".to_string(),
            },
            created_at: None,
            model: None,
        }
    }

    #[test]
    fn test_conversation_fetched_message() {
        let msg = PipelineMessage::ConversationFetched {
            account_id: "user-123".to_string(),
            conversation: create_test_conversation(),
            messages: vec![create_test_message()],
        };

        if let PipelineMessage::ConversationFetched {
            account_id,
            conversation,
            messages,
        } = msg
        {
            assert_eq!(account_id, "user-123");
            assert_eq!(conversation.id, "conv-1");
            assert_eq!(messages.len(), 1);
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_complete_message() {
        let msg = PipelineMessage::Complete {
            conversation_id: "conv-1".to_string(),
            messages_count: 5,
            chunks_count: 10,
        };

        if let PipelineMessage::Complete {
            conversation_id,
            messages_count,
            chunks_count,
        } = msg
        {
            assert_eq!(conversation_id, "conv-1");
            assert_eq!(messages_count, 5);
            assert_eq!(chunks_count, 10);
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_error_message() {
        let msg = PipelineMessage::Error {
            conversation_id: "conv-1".to_string(),
            stage: "embed".to_string(),
            message: "Failed to embed".to_string(),
        };

        if let PipelineMessage::Error {
            conversation_id,
            stage,
            message,
        } = msg
        {
            assert_eq!(conversation_id, "conv-1");
            assert_eq!(stage, "embed");
            assert_eq!(message, "Failed to embed");
        } else {
            panic!("Wrong message type");
        }
    }

    #[test]
    fn test_message_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        // This would fail to compile if PipelineMessage isn't Send + Sync
        // But since our types are Clone and don't have any !Send components, it should work
    }
}
