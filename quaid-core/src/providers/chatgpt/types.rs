//! ChatGPT API types based on reverse-engineered API
//!
//! These types are based on the chatgpt-exporter project:
//! https://github.com/pionxzh/chatgpt-exporter

use chrono::DateTime;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// Flexibly deserialize timestamps that can be either floats or ISO 8601 strings
fn deserialize_timestamp<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::{Error, Visitor};

    struct TimestampVisitor;

    impl<'de> Visitor<'de> for TimestampVisitor {
        type Value = f64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a float or ISO 8601 timestamp string")
        }

        fn visit_f64<E: Error>(self, v: f64) -> Result<f64, E> {
            Ok(v)
        }

        fn visit_i64<E: Error>(self, v: i64) -> Result<f64, E> {
            Ok(v as f64)
        }

        fn visit_u64<E: Error>(self, v: u64) -> Result<f64, E> {
            Ok(v as f64)
        }

        fn visit_str<E: Error>(self, v: &str) -> Result<f64, E> {
            // Try to parse as ISO 8601
            DateTime::parse_from_rfc3339(v)
                .map(|dt| dt.timestamp() as f64 + dt.timestamp_subsec_nanos() as f64 / 1e9)
                .map_err(|_| E::custom(format!("invalid timestamp: {}", v)))
        }
    }

    deserializer.deserialize_any(TimestampVisitor)
}

/// Flexibly deserialize optional timestamps
fn deserialize_optional_timestamp<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::{Error, Visitor};

    struct OptionalTimestampVisitor;

    impl<'de> Visitor<'de> for OptionalTimestampVisitor {
        type Value = Option<f64>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("null, a float, or ISO 8601 timestamp string")
        }

        fn visit_none<E: Error>(self) -> Result<Option<f64>, E> {
            Ok(None)
        }

        fn visit_unit<E: Error>(self) -> Result<Option<f64>, E> {
            Ok(None)
        }

        fn visit_some<D2>(self, deserializer: D2) -> Result<Option<f64>, D2::Error>
        where
            D2: Deserializer<'de>,
        {
            deserialize_timestamp(deserializer).map(Some)
        }

        fn visit_f64<E: Error>(self, v: f64) -> Result<Option<f64>, E> {
            Ok(Some(v))
        }

        fn visit_i64<E: Error>(self, v: i64) -> Result<Option<f64>, E> {
            Ok(Some(v as f64))
        }

        fn visit_u64<E: Error>(self, v: u64) -> Result<Option<f64>, E> {
            Ok(Some(v as f64))
        }

        fn visit_str<E: Error>(self, v: &str) -> Result<Option<f64>, E> {
            DateTime::parse_from_rfc3339(v)
                .map(|dt| Some(dt.timestamp() as f64 + dt.timestamp_subsec_nanos() as f64 / 1e9))
                .map_err(|_| E::custom(format!("invalid timestamp: {}", v)))
        }
    }

    deserializer.deserialize_any(OptionalTimestampVisitor)
}

/// Session response from /api/auth/session
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiSession {
    pub access_token: String,
    pub expires: String,
    pub user: ApiUser,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiUser {
    pub id: String,
    pub email: String,
    pub name: String,
    pub picture: String,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default)]
    pub mfa: bool,
}

/// List of conversations from /backend-api/conversations
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiConversations {
    pub items: Vec<ApiConversationItem>,
    pub limit: usize,
    pub offset: usize,
    pub total: Option<usize>,
    #[serde(default)]
    pub has_missing_conversations: bool,
}

/// Conversation item in list view (minimal info)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiConversationItem {
    pub id: String,
    pub title: String,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub create_time: f64,
    #[serde(default, deserialize_with = "deserialize_optional_timestamp")]
    pub update_time: Option<f64>,
}

/// Full conversation from /backend-api/conversation/:id
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiConversation {
    pub title: String,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub create_time: f64,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub update_time: f64,
    pub mapping: HashMap<String, ApiConversationNode>,
    pub current_node: Option<String>,
    #[serde(default)]
    pub is_archived: bool,
    #[serde(default)]
    pub moderation_results: Vec<serde_json::Value>,
    #[serde(default)]
    pub safe_urls: Vec<String>,
}

/// A node in the conversation graph
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiConversationNode {
    pub id: String,
    pub message: Option<ApiNodeMessage>,
    pub parent: Option<String>,
    #[serde(default)]
    pub children: Vec<String>,
}

/// Message within a conversation node
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiNodeMessage {
    pub id: Option<String>,
    pub author: ApiAuthor,
    pub content: serde_json::Value, // Dynamic based on content_type
    pub status: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_timestamp")]
    pub create_time: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_timestamp")]
    pub update_time: Option<f64>,
    pub metadata: Option<ApiMessageMetadata>,
    pub recipient: Option<String>,
    #[serde(default)]
    pub weight: f64,
    pub end_turn: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiAuthor {
    pub role: String,
    pub name: Option<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiMessageMetadata {
    pub model_slug: Option<String>,
    pub finish_details: Option<ApiFinishDetails>,
    pub citations: Option<Vec<ApiCitation>>,
    pub timestamp_: Option<String>,
    pub parent_id: Option<String>,
    pub request_id: Option<String>,
    pub aggregate_result: Option<ApiAggregateResult>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiFinishDetails {
    #[serde(rename = "type")]
    pub finish_type: Option<String>,
    pub stop_tokens: Option<Vec<i64>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiCitation {
    pub start_ix: Option<i32>,
    pub end_ix: Option<i32>,
    pub citation_format_type: Option<String>,
    pub metadata: Option<ApiCitationMetadata>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiCitationMetadata {
    pub title: Option<String>,
    pub url: Option<String>,
    pub text: Option<String>,
    #[serde(rename = "type")]
    pub meta_type: Option<String>,
}

/// Result from code execution
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiAggregateResult {
    pub code: Option<String>,
    pub status: Option<String>,
    pub messages: Option<Vec<ApiExecutionMessage>>,
    pub run_id: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_timestamp")]
    pub start_time: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_optional_timestamp")]
    pub end_time: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiExecutionMessage {
    pub message_type: String,
    pub sender: Option<String>,
    pub image_url: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_optional_timestamp")]
    pub time: Option<f64>,
}

/// Projects (called "Gizmos" in the API)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiProjectsResponse {
    pub items: Vec<ApiGizmoWrapper>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiGizmoWrapper {
    pub gizmo: ApiGizmoInner,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiGizmoInner {
    pub gizmo: ApiProjectInfo,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiProjectInfo {
    pub id: String,
    pub organization_id: Option<String>,
    pub display: Option<ApiProjectDisplay>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiProjectDisplay {
    pub name: String,
    pub description: Option<String>,
}

/// Project conversations response
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiProjectConversations {
    pub items: Vec<ApiConversationItem>,
    pub cursor: Option<usize>,
}

/// File download response
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum ApiFileDownload {
    Success {
        download_url: String,
        file_name: String,
        creation_time: Option<String>,
        #[serde(default)]
        metadata: serde_json::Value,
    },
    Error {
        error_code: Option<String>,
        error_message: Option<String>,
    },
}

/// Account check response
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiAccountsCheck {
    pub accounts: HashMap<String, ApiAccountInfo>,
    pub account_ordering: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiAccountInfo {
    pub account: ApiAccountDetail,
    #[serde(default)]
    pub features: Vec<String>,
    pub entitlement: Option<ApiEntitlement>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiAccountDetail {
    pub account_id: Option<String>,
    pub account_user_role: Option<String>,
    pub structure: Option<String>, // "personal" or "workspace"
    pub plan_type: Option<String>, // "free", "team", etc.
    pub name: Option<String>,
    #[serde(default)]
    pub is_deactivated: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiEntitlement {
    pub subscription_id: Option<String>,
    #[serde(default)]
    pub has_active_subscription: bool,
    pub subscription_plan: Option<String>,
    pub expires_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_session() {
        let json = r#"{
            "accessToken": "test-token",
            "expires": "2025-01-01T00:00:00.000Z",
            "user": {
                "id": "user-123",
                "email": "test@example.com",
                "name": "Test User",
                "picture": "https://example.com/avatar.png",
                "groups": [],
                "mfa": false
            }
        }"#;

        let session: ApiSession = serde_json::from_str(json).unwrap();
        assert_eq!(session.access_token, "test-token");
        assert_eq!(session.user.email, "test@example.com");
    }

    #[test]
    fn test_parse_conversations_list() {
        let json = r#"{
            "items": [
                {"id": "conv-1", "title": "Test Chat", "create_time": 1725512345.0}
            ],
            "limit": 20,
            "offset": 0,
            "total": 1,
            "has_missing_conversations": false
        }"#;

        let convs: ApiConversations = serde_json::from_str(json).unwrap();
        assert_eq!(convs.items.len(), 1);
        assert_eq!(convs.items[0].id, "conv-1");
    }

    #[test]
    fn test_parse_conversation_node() {
        let json = r#"{
            "id": "node-1",
            "message": {
                "id": "msg-1",
                "author": {"role": "user", "metadata": {}},
                "content": {"content_type": "text", "parts": ["Hello"]},
                "status": "finished_successfully",
                "recipient": "all",
                "weight": 1.0
            },
            "parent": null,
            "children": ["node-2"]
        }"#;

        let node: ApiConversationNode = serde_json::from_str(json).unwrap();
        assert_eq!(node.id, "node-1");
        assert!(node.message.is_some());
        assert_eq!(node.children.len(), 1);
    }

    #[test]
    fn test_parse_file_download_success() {
        let json = r#"{
            "status": "success",
            "download_url": "https://example.com/file.png",
            "file_name": "image.png",
            "creation_time": "2025-01-01T00:00:00Z",
            "metadata": {}
        }"#;

        let download: ApiFileDownload = serde_json::from_str(json).unwrap();
        match download {
            ApiFileDownload::Success { download_url, .. } => {
                assert_eq!(download_url, "https://example.com/file.png");
            }
            _ => panic!("Expected Success variant"),
        }
    }

    #[test]
    fn test_parse_file_download_error() {
        let json = r#"{
            "status": "error",
            "error_code": "not_found",
            "error_message": "File not found"
        }"#;

        let download: ApiFileDownload = serde_json::from_str(json).unwrap();
        match download {
            ApiFileDownload::Error { error_message, .. } => {
                assert_eq!(error_message, Some("File not found".to_string()));
            }
            _ => panic!("Expected Error variant"),
        }
    }

    #[test]
    fn test_parse_conversation_with_multimodal() {
        let json = r#"{
            "title": "Image Chat",
            "create_time": 1725512345.0,
            "update_time": 1725512400.0,
            "mapping": {
                "root": {
                    "id": "root",
                    "message": null,
                    "parent": null,
                    "children": ["node-1"]
                },
                "node-1": {
                    "id": "node-1",
                    "message": {
                        "author": {"role": "user", "metadata": {}},
                        "content": {
                            "content_type": "multimodal_text",
                            "parts": [
                                "What's in this image?",
                                {
                                    "content_type": "image_asset_pointer",
                                    "asset_pointer": "file-service://abc123",
                                    "width": 800,
                                    "height": 600
                                }
                            ]
                        },
                        "recipient": "all",
                        "weight": 1.0
                    },
                    "parent": "root",
                    "children": []
                }
            },
            "current_node": "node-1",
            "is_archived": false,
            "moderation_results": [],
            "safe_urls": []
        }"#;

        let conv: ApiConversation = serde_json::from_str(json).unwrap();
        assert_eq!(conv.title, "Image Chat");
        assert!(conv.mapping.contains_key("node-1"));
    }
}
