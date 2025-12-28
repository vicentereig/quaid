//! Granola provider implementation
//!
//! Syncs meeting notes, transcripts, and documents from Granola
//! using their reverse-engineered API.
//!
//! Token source: ~/Library/Application Support/Granola/supabase.json
//! API reference: https://github.com/getprobo/reverse-engineering-granola-api

pub mod types;

use crate::providers::{
    Account, Attachment, Conversation, Message, MessageContent, Provider, ProviderId,
    ProviderError, Result, Role,
};
use async_trait::async_trait;
use reqwest::{header, Client};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use types::*;

const API_BASE: &str = "https://api.granola.ai";
const WORKOS_AUTH_URL: &str = "https://api.workos.com/user_management/authenticate";
const WORKOS_CLIENT_ID: &str = "client_01HPNB6DXHV2SBPKY31CZMK5YP"; // Granola's WorkOS client ID

/// Granola provider
pub struct GranolaProvider {
    client: Client,
    credentials: Arc<RwLock<Option<GranolaCredentials>>>,
    credentials_path: PathBuf,
}

impl GranolaProvider {
    /// Create a new Granola provider, loading credentials from the Granola app data
    pub fn new() -> Self {
        let credentials_path = get_credentials_path();
        let credentials = load_credentials_from_file(&credentials_path);
        let client = build_client();

        Self {
            client,
            credentials: Arc::new(RwLock::new(credentials)),
            credentials_path,
        }
    }

    /// Create a provider with explicit credentials (for testing)
    #[cfg(test)]
    pub fn with_credentials(credentials: GranolaCredentials) -> Self {
        Self {
            client: build_client(),
            credentials: Arc::new(RwLock::new(Some(credentials))),
            credentials_path: get_credentials_path(),
        }
    }

    /// Get the current access token, refreshing if needed
    async fn get_access_token(&self) -> Result<String> {
        let creds = self.credentials.read().await;
        let creds = creds.as_ref().ok_or(ProviderError::AuthRequired)?;

        // Check if token might be expired (be conservative)
        // Calculate expiry from obtained_at + expires_in
        let is_expired = match (creds.obtained_at, creds.expires_in) {
            (Some(obtained_at_ms), Some(expires_in_sec)) => {
                let obtained_at_sec = obtained_at_ms / 1000; // Convert ms to seconds
                let expires_at = obtained_at_sec + expires_in_sec;
                let now = chrono::Utc::now().timestamp();
                now >= expires_at - 300 // Expired or expiring in 5 minutes
            }
            _ => false, // Can't determine, assume valid
        };

        if is_expired {
            let _ = creds; // Release borrow before refresh
            return self.refresh_token().await;
        }

        Ok(creds.access_token.clone())
    }

    /// Refresh the access token using WorkOS
    async fn refresh_token(&self) -> Result<String> {
        let refresh_token = {
            let creds = self.credentials.read().await;
            let creds = creds.as_ref().ok_or(ProviderError::AuthRequired)?;
            creds.refresh_token.clone()
        };

        let response = self
            .client
            .post(WORKOS_AUTH_URL)
            .json(&serde_json::json!({
                "client_id": WORKOS_CLIENT_ID,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ProviderError::AuthFailed(format!(
                "Token refresh failed ({}): {}",
                status,
                truncate(&text, 200)
            )));
        }

        let text = response.text().await.unwrap_or_default();
        let auth_response: WorkOsAuthResponse = serde_json::from_str(&text).map_err(|e| {
            ProviderError::Parse(format!("Failed to parse WorkOS response: {} - body: {}", e, truncate(&text, 300)))
        })?;

        // CRITICAL: WorkOS rotates refresh tokens - save the new one immediately
        let now_ms = chrono::Utc::now().timestamp_millis();
        let new_credentials = GranolaCredentials {
            access_token: auth_response.access_token.clone(),
            refresh_token: auth_response.refresh_token,
            expires_in: Some(auth_response.expires_in),
            obtained_at: Some(now_ms),
            token_type: auth_response.token_type,
            session_id: None,
            external_id: None,
        };

        // Save to memory
        *self.credentials.write().await = Some(new_credentials.clone());

        // Save to file (so it persists across runs)
        save_credentials_to_file(&self.credentials_path, &new_credentials);

        Ok(auth_response.access_token)
    }

    /// Make an authenticated POST request (Granola uses POST for most endpoints)
    async fn api_post<T, B>(&self, endpoint: &str, body: &B) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
        B: serde::Serialize,
    {
        let token = self.get_access_token().await?;
        let url = format!("{}{}", API_BASE, endpoint);

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", token))
            .json(body)
            .send()
            .await?;

        let status = response.status();

        if status == 401 || status == 403 {
            // Try refreshing token once
            let token = self.refresh_token().await?;
            let response = self
                .client
                .post(&url)
                .header("Authorization", format!("Bearer {}", token))
                .json(body)
                .send()
                .await?;

            let retry_status = response.status();
            if !retry_status.is_success() {
                let text = response.text().await.unwrap_or_default();
                return Err(ProviderError::Api(format!(
                    "{}: {}",
                    retry_status,
                    truncate(&text, 500)
                )));
            }

            let text = response.text().await.unwrap_or_default();
            return serde_json::from_str(&text).map_err(|e| {
                ProviderError::Parse(format!("Failed to parse response: {} - body: {}", e, truncate(&text, 300)))
            });
        }

        if status == 429 {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(60);
            return Err(ProviderError::RateLimited(retry_after));
        }

        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(ProviderError::Api(format!(
                "{}: {}",
                status,
                truncate(&text, 500)
            )));
        }

        let text = response.text().await.unwrap_or_default();
        serde_json::from_str(&text).map_err(|e| {
            ProviderError::Parse(format!("Failed to parse response: {} - body: {}", e, truncate(&text, 300)))
        })
    }

    /// Fetch all documents with pagination
    async fn fetch_all_documents(&self) -> Result<Vec<ApiDocument>> {
        let mut documents = Vec::new();
        let mut offset = 0;
        let limit = 100;

        loop {
            let response: ApiDocumentsResponse = self
                .api_post(
                    "/v2/get-documents",
                    &serde_json::json!({
                        "limit": limit,
                        "offset": offset,
                        "include_last_viewed_panel": false
                    }),
                )
                .await?;

            let docs = response.all_documents();
            let count = docs.len();
            documents.extend(docs);

            if count < limit {
                break;
            }

            offset += limit;
        }

        Ok(documents)
    }

    /// Fetch transcript for a document
    async fn fetch_transcript(&self, document_id: &str) -> Result<Vec<ApiUtterance>> {
        let response: ApiTranscriptResponse = self
            .api_post(
                "/v1/get-document-transcript",
                &serde_json::json!({
                    "document_id": document_id
                }),
            )
            .await?;

        Ok(response.all_utterances())
    }

    /// Convert a Granola document to our Conversation type
    fn document_to_conversation(doc: &ApiDocument) -> Conversation {
        Conversation {
            id: doc.id.clone(),
            provider_id: "granola".to_string(),
            title: doc.title.clone(),
            created_at: doc.created_at,
            updated_at: doc.updated_at.unwrap_or(doc.created_at),
            model: None,
            project_id: doc.workspace_id.clone(),
            project_name: doc.workspace_name.clone(),
            is_archived: false,
        }
    }

    /// Convert transcript utterances to Messages
    fn utterances_to_messages(doc_id: &str, utterances: &[ApiUtterance]) -> Vec<Message> {
        utterances
            .iter()
            .enumerate()
            .map(|(idx, utterance)| {
                let speaker = utterance
                    .speaker
                    .clone()
                    .or_else(|| utterance.source.clone())
                    .unwrap_or_else(|| "Speaker".to_string());

                let text = format!("**{}**: {}", speaker, utterance.text);

                Message {
                    id: format!("{}-{}", doc_id, idx),
                    conversation_id: doc_id.to_string(),
                    parent_id: if idx > 0 {
                        Some(format!("{}-{}", doc_id, idx - 1))
                    } else {
                        None
                    },
                    role: Role::User,
                    content: MessageContent::Text { text },
                    created_at: None,
                    model: None,
                }
            })
            .collect()
    }

    /// Build a notes message from document content
    fn build_notes_message(doc: &ApiDocument) -> Option<Message> {
        // Try to extract notes from the document (both fields are ProseMirror JSON)
        let notes = doc
            .notes
            .as_ref()
            .and_then(extract_text_from_prosemirror)
            .or_else(|| doc.content.as_ref().and_then(extract_text_from_prosemirror))?;

        if notes.is_empty() {
            return None;
        }

        Some(Message {
            id: format!("{}-notes", doc.id),
            conversation_id: doc.id.clone(),
            parent_id: None,
            role: Role::Assistant, // Notes are AI-enhanced
            content: MessageContent::Text {
                text: format!("## Meeting Notes\n\n{}", notes),
            },
            created_at: Some(doc.created_at),
            model: Some("granola-ai".to_string()),
        })
    }
}

impl Default for GranolaProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Provider for GranolaProvider {
    fn id(&self) -> ProviderId {
        ProviderId(String::from("granola"))
    }

    async fn is_authenticated(&self) -> bool {
        self.credentials.read().await.is_some()
    }

    async fn authenticate(&mut self) -> Result<Account> {
        // Check if we already have credentials from the Granola app
        if self.credentials.read().await.is_some() {
            println!("Found existing Granola credentials from desktop app.");
            return self.account().await;
        }

        // Granola doesn't have a web OAuth flow we can use
        // The user needs to have the desktop app installed and logged in
        println!("Granola authentication requires the desktop app to be installed and logged in.");
        println!();
        println!("Please ensure:");
        println!("1. Granola desktop app is installed (https://granola.ai)");
        println!("2. You are logged into the app");
        println!();

        // Try to reload credentials from file
        let credentials = load_credentials_from_file(&self.credentials_path);

        if credentials.is_none() {
            let path = self.credentials_path.display();
            return Err(ProviderError::AuthFailed(format!(
                "No credentials found at {}. Please log into the Granola desktop app first.",
                path
            )));
        }

        *self.credentials.write().await = credentials;
        println!("Credentials loaded successfully!");

        self.account().await
    }

    async fn account(&self) -> Result<Account> {
        if self.credentials.read().await.is_none() {
            return Err(ProviderError::AuthRequired);
        }

        // Granola doesn't have a dedicated user endpoint
        // We'll try to get workspaces to validate the token and create a basic account
        let workspaces: ApiWorkspacesResponse = self
            .api_post("/v1/get-workspaces", &serde_json::json!({}))
            .await?;

        let first_workspace = workspaces.workspaces.first().map(|w| &w.workspace);
        let workspace_name = first_workspace
            .map(|w| w.name.clone())
            .unwrap_or_else(|| "Granola User".to_string());

        // Try to get email from the credentials file
        let email = load_user_info_from_file(&self.credentials_path)
            .and_then(|u| u.email)
            .unwrap_or_else(|| "unknown".to_string());

        Ok(Account {
            id: format!(
                "granola-{}",
                first_workspace
                    .map(|w| w.id.as_str())
                    .unwrap_or("unknown")
            ),
            provider: self.id(),
            email,
            name: Some(workspace_name),
            avatar_url: None,
        })
    }

    async fn conversations(&self) -> Result<Vec<Conversation>> {
        let documents = self.fetch_all_documents().await?;
        Ok(documents.iter().map(Self::document_to_conversation).collect())
    }

    async fn conversation(&self, id: &str) -> Result<(Conversation, Vec<Message>)> {
        // Fetch the document with content
        let response: ApiDocumentsResponse = self
            .api_post(
                "/v1/get-documents-batch",
                &serde_json::json!({
                    "document_ids": [id],
                    "include_last_viewed_panel": true
                }),
            )
            .await?;

        let doc = response
            .all_documents()
            .into_iter()
            .next()
            .ok_or_else(|| ProviderError::Api(format!("Document {} not found", id)))?;

        let conversation = Self::document_to_conversation(&doc);

        // Try to fetch transcript (may 404 if no transcript exists)
        let utterances = match self.fetch_transcript(id).await {
            Ok(u) => u,
            Err(_) => vec![], // Transcript not available
        };

        let mut messages = Self::utterances_to_messages(id, &utterances);

        // Add notes as a special message at the beginning
        if let Some(notes_msg) = Self::build_notes_message(&doc) {
            messages.insert(0, notes_msg);
        }

        Ok((conversation, messages))
    }

    async fn project_conversations(&self, project_id: &str) -> Result<Vec<Conversation>> {
        let all = self.conversations().await?;
        Ok(all
            .into_iter()
            .filter(|c| c.project_id.as_deref() == Some(project_id))
            .collect())
    }

    async fn download_attachment(
        &self,
        _attachment: &Attachment,
        _path: &Path,
    ) -> Result<()> {
        Err(ProviderError::Api(
            "Attachment download not supported for Granola".to_string(),
        ))
    }
}

/// Get the path to Granola's credentials file
fn get_credentials_path() -> PathBuf {
    if cfg!(target_os = "macos") {
        dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("Granola")
            .join("supabase.json")
    } else if cfg!(target_os = "windows") {
        dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("Granola")
            .join("supabase.json")
    } else {
        // Linux - guess based on common patterns
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("Granola")
            .join("supabase.json")
    }
}

/// Load credentials from the Granola app's storage
fn load_credentials_from_file(path: &Path) -> Option<GranolaCredentials> {
    let content = std::fs::read_to_string(path).ok()?;
    let file: GranolaCredentialsFile = serde_json::from_str(&content).ok()?;
    file.parse_workos_tokens()
}

/// Load user info from the Granola app's storage
fn load_user_info_from_file(path: &Path) -> Option<GranolaUserInfo> {
    let content = std::fs::read_to_string(path).ok()?;
    let file: GranolaCredentialsFile = serde_json::from_str(&content).ok()?;
    file.parse_user_info()
}

/// Save credentials back to file (needed after token refresh)
/// We read the existing file, update just the workos_tokens, and write back
fn save_credentials_to_file(path: &Path, credentials: &GranolaCredentials) {
    // Read existing file to preserve other fields
    let existing = std::fs::read_to_string(path).ok();
    let mut file: GranolaCredentialsFile = existing
        .as_ref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(GranolaCredentialsFile {
            workos_tokens: None,
            cognito_tokens: None,
            user_info: None,
            session_id: credentials.session_id.clone(),
        });

    // Update just the workos_tokens field
    if let Ok(tokens_json) = serde_json::to_string(credentials) {
        file.workos_tokens = Some(tokens_json);
    }

    // Write back
    if let Ok(content) = serde_json::to_string_pretty(&file) {
        let _ = std::fs::write(path, content);
    }
}

/// Build HTTP client with appropriate headers
fn build_client() -> Client {
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::USER_AGENT,
        "Granola/1.0 (Quaid Sync)".parse().unwrap(),
    );
    headers.insert(header::ACCEPT, "application/json".parse().unwrap());
    headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

    Client::builder()
        .default_headers(headers)
        .build()
        .expect("Failed to build HTTP client")
}

/// Extract text from ProseMirror content structure
fn extract_text_from_prosemirror(content: &serde_json::Value) -> Option<String> {
    let mut texts = Vec::new();
    extract_text_recursive(content, &mut texts);

    if texts.is_empty() {
        None
    } else {
        Some(texts.join("\n"))
    }
}

fn extract_text_recursive(value: &serde_json::Value, texts: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(obj) => {
            // Check for text content
            if let Some(text) = obj.get("text").and_then(|t| t.as_str()) {
                if !text.trim().is_empty() {
                    texts.push(text.to_string());
                }
            }

            // Recurse into content array
            if let Some(content) = obj.get("content") {
                extract_text_recursive(content, texts);
            }

            // Recurse into other fields
            for (key, val) in obj {
                if key != "text" && key != "content" {
                    extract_text_recursive(val, texts);
                }
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr {
                extract_text_recursive(item, texts);
            }
        }
        _ => {}
    }
}

/// Truncate a string safely at char boundaries
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        let mut end = max_len;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_id() {
        let provider = GranolaProvider::new();
        assert_eq!(provider.id().0, "granola");
    }

    #[test]
    fn test_document_to_conversation() {
        let doc = ApiDocument {
            id: "doc-123".to_string(),
            title: "Team Meeting".to_string(),
            created_at: chrono::Utc::now(),
            updated_at: Some(chrono::Utc::now()),
            workspace_id: Some("ws-1".to_string()),
            workspace_name: Some("Work".to_string()),
            folders: vec![],
            meeting_date: None,
            sources: vec![],
            content: None,
            notes: None,
        };

        let conv = GranolaProvider::document_to_conversation(&doc);
        assert_eq!(conv.id, "doc-123");
        assert_eq!(conv.title, "Team Meeting");
        assert_eq!(conv.provider_id, "granola");
        assert_eq!(conv.project_id, Some("ws-1".to_string()));
    }

    #[test]
    fn test_utterances_to_messages() {
        let utterances = vec![
            ApiUtterance {
                source: Some("microphone".to_string()),
                text: "Hello".to_string(),
                start_time: Some(0.0),
                end_time: Some(1.0),
                confidence: Some(0.9),
                speaker: Some("Alice".to_string()),
            },
            ApiUtterance {
                source: Some("system".to_string()),
                text: "Hi there".to_string(),
                start_time: Some(1.5),
                end_time: Some(2.5),
                confidence: Some(0.85),
                speaker: None,
            },
        ];

        let messages = GranolaProvider::utterances_to_messages("doc-1", &utterances);
        assert_eq!(messages.len(), 2);

        match &messages[0].content {
            MessageContent::Text { text } => {
                assert!(text.contains("Alice"));
                assert!(text.contains("Hello"));
            }
            _ => panic!("Expected Text content"),
        }

        match &messages[1].content {
            MessageContent::Text { text } => {
                assert!(text.contains("system")); // Falls back to source
                assert!(text.contains("Hi there"));
            }
            _ => panic!("Expected Text content"),
        }

        assert_eq!(messages[1].parent_id, Some("doc-1-0".to_string()));
    }

    #[test]
    fn test_extract_text_from_prosemirror() {
        let content = serde_json::json!({
            "type": "doc",
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {"type": "text", "text": "First paragraph."}
                    ]
                },
                {
                    "type": "paragraph",
                    "content": [
                        {"type": "text", "text": "Second paragraph."}
                    ]
                }
            ]
        });

        let text = extract_text_from_prosemirror(&content).unwrap();
        assert!(text.contains("First paragraph"));
        assert!(text.contains("Second paragraph"));
    }

    #[test]
    fn test_credentials_path() {
        let path = get_credentials_path();
        assert!(path.to_string_lossy().contains("Granola"));
        assert!(path.to_string_lossy().contains("supabase.json"));
    }
}
