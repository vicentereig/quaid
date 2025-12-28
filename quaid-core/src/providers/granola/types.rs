//! Granola API types based on reverse-engineered API
//!
//! Based on: https://github.com/getprobo/reverse-engineering-granola-api
//! Token location: ~/Library/Application Support/Granola/supabase.json

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Raw file format from Granola desktop app
/// The tokens are stored as JSON strings, not objects
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GranolaCredentialsFile {
    #[serde(default)]
    pub workos_tokens: Option<String>,
    #[serde(default)]
    pub cognito_tokens: Option<String>,
    #[serde(default)]
    pub user_info: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Parsed credentials for API usage
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GranolaCredentials {
    pub access_token: String,
    pub refresh_token: String,
    #[serde(default)]
    pub expires_in: Option<i64>,
    #[serde(default)]
    pub obtained_at: Option<i64>,
    #[serde(default)]
    pub token_type: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub external_id: Option<String>,
}

/// User info parsed from the file
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GranolaUserInfo {
    pub id: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub user_metadata: Option<GranolaUserMetadata>,
}

/// User metadata from user_info
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GranolaUserMetadata {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub picture: Option<String>,
}

impl GranolaCredentialsFile {
    /// Parse the WorkOS tokens from the JSON string
    pub fn parse_workos_tokens(&self) -> Option<GranolaCredentials> {
        self.workos_tokens
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
    }

    /// Parse the user info from the JSON string
    pub fn parse_user_info(&self) -> Option<GranolaUserInfo> {
        self.user_info
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
    }
}

/// WorkOS authentication response
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WorkOsAuthResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_in: i64,
    #[serde(default)]
    pub token_type: Option<String>,
}

/// Document from get-documents endpoint
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiDocument {
    #[serde(alias = "document_id")]
    pub id: String,
    #[serde(default)]
    pub title: String,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub workspace_name: Option<String>,
    #[serde(default)]
    pub folders: Vec<ApiFolder>,
    #[serde(default)]
    pub meeting_date: Option<DateTime<Utc>>,
    #[serde(default)]
    pub sources: Vec<String>,
    /// ProseMirror content structure
    #[serde(default)]
    pub content: Option<serde_json::Value>,
    /// Notes as ProseMirror content structure
    #[serde(default)]
    pub notes: Option<serde_json::Value>,
}

/// Folder reference within a document
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiFolder {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
}

/// Response from get-documents endpoint
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiDocumentsResponse {
    #[serde(default)]
    pub documents: Vec<ApiDocument>,
    #[serde(default, alias = "docs")]
    pub items: Vec<ApiDocument>,
}

impl ApiDocumentsResponse {
    /// Get all documents from either field
    pub fn all_documents(self) -> Vec<ApiDocument> {
        if !self.documents.is_empty() {
            self.documents
        } else {
            self.items
        }
    }
}

/// Transcript entry (utterance)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiUtterance {
    #[serde(default)]
    pub source: Option<String>,
    pub text: String,
    #[serde(default)]
    pub start_time: Option<f64>,
    #[serde(default)]
    pub end_time: Option<f64>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub speaker: Option<String>,
}

/// Response from get-document-transcript endpoint
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiTranscriptResponse {
    #[serde(default)]
    pub utterances: Vec<ApiUtterance>,
    #[serde(default)]
    pub transcript: Vec<ApiUtterance>,
}

impl ApiTranscriptResponse {
    /// Get all utterances from either field
    pub fn all_utterances(self) -> Vec<ApiUtterance> {
        if !self.utterances.is_empty() {
            self.utterances
        } else {
            self.transcript
        }
    }
}

/// Wrapper for workspace in the response
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiWorkspaceWrapper {
    pub workspace: ApiWorkspace,
}

/// Workspace from get-workspaces endpoint
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiWorkspace {
    #[serde(alias = "workspace_id")]
    pub id: String,
    #[serde(alias = "display_name", default)]
    pub name: String,
    #[serde(default)]
    pub slug: Option<String>,
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub owner_id: Option<String>,
}

/// Response from get-workspaces endpoint
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiWorkspacesResponse {
    pub workspaces: Vec<ApiWorkspaceWrapper>,
}

/// Document list (folder) from get-document-lists endpoint
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiDocumentList {
    pub id: String,
    #[serde(alias = "title")]
    pub name: String,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default, alias = "document_ids")]
    pub documents: Vec<String>,
    #[serde(default)]
    pub is_favourite: bool,
}

/// Response from get-document-lists endpoint
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiDocumentListsResponse {
    pub lists: Vec<ApiDocumentList>,
}

/// User/account information
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiUser {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub avatar_url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_credentials_file() {
        // The file format has nested JSON strings
        let workos_tokens = r#"{"access_token":"eyJhbGciOiJS...","refresh_token":"refresh_abc123","expires_in":21600,"obtained_at":1705329600000,"token_type":"Bearer"}"#;
        let user_info = r#"{"id":"user-123","email":"test@example.com","user_metadata":{"name":"Test User"}}"#;

        let json = format!(
            r#"{{"workos_tokens":"{}","user_info":"{}","session_id":"session_123"}}"#,
            workos_tokens.replace('"', "\\\""),
            user_info.replace('"', "\\\"")
        );

        let file: GranolaCredentialsFile = serde_json::from_str(&json).unwrap();
        let creds = file.parse_workos_tokens().unwrap();
        assert!(creds.access_token.starts_with("eyJ"));
        assert!(creds.refresh_token.starts_with("refresh_"));
        assert_eq!(creds.expires_in, Some(21600));

        let user = file.parse_user_info().unwrap();
        assert_eq!(user.email, Some("test@example.com".to_string()));
    }

    #[test]
    fn test_parse_workos_response() {
        let json = r#"{
            "access_token": "new_token",
            "refresh_token": "new_refresh",
            "expires_in": 3600,
            "token_type": "Bearer"
        }"#;

        let response: WorkOsAuthResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.access_token, "new_token");
        assert_eq!(response.expires_in, 3600);
    }

    #[test]
    fn test_parse_document() {
        let json = r#"{
            "id": "doc-123",
            "title": "Weekly Standup Notes",
            "created_at": "2025-01-15T10:00:00Z",
            "updated_at": "2025-01-15T11:00:00Z",
            "workspace_id": "ws-1",
            "folders": [
                {"id": "folder-1", "name": "Team Meetings"}
            ],
            "sources": ["microphone", "system"]
        }"#;

        let doc: ApiDocument = serde_json::from_str(json).unwrap();
        assert_eq!(doc.id, "doc-123");
        assert_eq!(doc.title, "Weekly Standup Notes");
        assert_eq!(doc.folders.len(), 1);
        assert_eq!(doc.sources.len(), 2);
    }

    #[test]
    fn test_parse_documents_response_with_documents() {
        let json = r#"{
            "documents": [
                {
                    "id": "doc-1",
                    "title": "Meeting 1",
                    "created_at": "2025-01-15T10:00:00Z",
                    "updated_at": "2025-01-15T10:00:00Z"
                }
            ]
        }"#;

        let response: ApiDocumentsResponse = serde_json::from_str(json).unwrap();
        let docs = response.all_documents();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].title, "Meeting 1");
    }

    #[test]
    fn test_parse_documents_response_with_items() {
        let json = r#"{
            "items": [
                {
                    "document_id": "doc-2",
                    "title": "Meeting 2",
                    "created_at": "2025-01-15T10:00:00Z",
                    "updated_at": "2025-01-15T10:00:00Z"
                }
            ]
        }"#;

        let response: ApiDocumentsResponse = serde_json::from_str(json).unwrap();
        let docs = response.all_documents();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].id, "doc-2");
    }

    #[test]
    fn test_parse_transcript() {
        let json = r#"{
            "utterances": [
                {
                    "source": "microphone",
                    "text": "Hello everyone",
                    "start_time": 0.0,
                    "end_time": 2.5,
                    "confidence": 0.95,
                    "speaker": "Alice"
                }
            ]
        }"#;

        let response: ApiTranscriptResponse = serde_json::from_str(json).unwrap();
        let utterances = response.all_utterances();
        assert_eq!(utterances.len(), 1);
        assert_eq!(utterances[0].text, "Hello everyone");
        assert_eq!(utterances[0].speaker, Some("Alice".to_string()));
    }

    #[test]
    fn test_parse_workspace() {
        let json = r#"{
            "id": "ws-123",
            "name": "Personal",
            "created_at": "2025-01-01T00:00:00Z",
            "owner_id": "user-1"
        }"#;

        let workspace: ApiWorkspace = serde_json::from_str(json).unwrap();
        assert_eq!(workspace.id, "ws-123");
        assert_eq!(workspace.name, "Personal");
    }

    #[test]
    fn test_parse_document_list() {
        let json = r#"{
            "id": "list-1",
            "name": "Important Meetings",
            "workspace_id": "ws-1",
            "documents": ["doc-1", "doc-2", "doc-3"],
            "is_favourite": true
        }"#;

        let list: ApiDocumentList = serde_json::from_str(json).unwrap();
        assert_eq!(list.id, "list-1");
        assert_eq!(list.name, "Important Meetings");
        assert_eq!(list.documents.len(), 3);
        assert!(list.is_favourite);
    }

    #[test]
    fn test_parse_document_list_with_aliases() {
        let json = r#"{
            "id": "list-2",
            "title": "Q4 Reviews",
            "document_ids": ["doc-4", "doc-5"]
        }"#;

        let list: ApiDocumentList = serde_json::from_str(json).unwrap();
        assert_eq!(list.name, "Q4 Reviews");
        assert_eq!(list.documents.len(), 2);
    }
}
