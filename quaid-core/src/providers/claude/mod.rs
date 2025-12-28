//! Claude.ai provider implementation
//!
//! Syncs conversations from claude.ai using reverse-engineered API endpoints.

pub mod types;

use crate::credentials::{CredentialStore, KeyringStore};
use crate::providers::{
    Account, Attachment, Conversation, Message, MessageContent, Provider, ProviderId,
    ProviderError, Result, Role,
};
use async_trait::async_trait;
use reqwest::{header, Client};
use std::path::Path;
use std::sync::Arc;
use types::*;

const API_BASE: &str = "https://claude.ai/api";
const KEYRING_SERVICE: &str = "quaid";
const KEYRING_USER_COOKIES: &str = "claude-cookies";
const KEYRING_USER_ORG: &str = "claude-org-id";

/// Claude.ai provider
pub struct ClaudeProvider {
    client: Client,
    cookies: Option<String>,
    org_id: Option<String>,
    #[allow(dead_code)]
    account: Option<ApiAccount>,
    credential_store: Arc<dyn CredentialStore>,
}

impl ClaudeProvider {
    /// Create a new Claude provider, loading credentials from keyring if available
    pub fn new() -> Self {
        Self::with_credential_store(Arc::new(KeyringStore::new()))
    }

    /// Create with a custom credential store (for testing)
    pub fn with_credential_store(credential_store: Arc<dyn CredentialStore>) -> Self {
        let cookies = credential_store
            .get(KEYRING_SERVICE, KEYRING_USER_COOKIES)
            .ok();
        let org_id = credential_store
            .get(KEYRING_SERVICE, KEYRING_USER_ORG)
            .ok();
        let client = build_client(cookies.as_deref());

        Self {
            client,
            cookies,
            org_id,
            account: None,
            credential_store,
        }
    }

    /// Create a provider with explicit credentials (for testing)
    #[cfg(test)]
    pub fn with_credentials(cookies: Option<String>, org_id: Option<String>) -> Self {
        use crate::credentials::MockStore;
        let client = build_client(cookies.as_deref());
        Self {
            client,
            cookies,
            org_id,
            account: None,
            credential_store: Arc::new(MockStore::new()),
        }
    }

    /// Get the organization ID, fetching if not cached
    async fn get_org_id(&self) -> Result<String> {
        if let Some(ref org_id) = self.org_id {
            return Ok(org_id.clone());
        }

        let url = format!("{}/organizations", API_BASE);
        let resp = self.client.get(&url).send().await?;

        let status = resp.status();
        let body = resp.text().await?;

        if !status.is_success() {
            return Err(ProviderError::Api(format!(
                "GET {} failed with {}: {}",
                url,
                status,
                truncate_body(&body, 500)
            )));
        }

        let orgs: Vec<ApiOrganization> = serde_json::from_str(&body).map_err(|e| {
            ProviderError::Parse(format!(
                "Failed to parse organizations: {}. Body: {}",
                e,
                truncate_body(&body, 500)
            ))
        })?;

        orgs.first()
            .map(|o| o.uuid.clone())
            .ok_or_else(|| ProviderError::Api("No organizations found".to_string()))
    }

    /// Fetch user account info
    async fn fetch_account(&self) -> Result<ApiAccount> {
        // Try to get account info from the bootstrap endpoint
        let url = format!("{}/bootstrap", API_BASE);
        let resp = self.client.get(&url).send().await?;

        let status = resp.status();
        let body = resp.text().await?;

        if status.is_success() {
            // Bootstrap response contains account info
            let bootstrap: serde_json::Value = serde_json::from_str(&body).map_err(|e| {
                ProviderError::Parse(format!(
                    "Failed to parse bootstrap: {}. Body: {}",
                    e,
                    truncate_body(&body, 500)
                ))
            })?;

            if let Some(account) = bootstrap.get("account") {
                return serde_json::from_value(account.clone())
                    .map_err(|e| ProviderError::Parse(e.to_string()));
            }
        }

        // Fallback: create a minimal account from what we know
        // The org endpoint might have user info
        Err(ProviderError::Api(format!(
            "Could not fetch account info. Bootstrap response: {}",
            truncate_body(&body, 500)
        )))
    }

    /// Convert Claude API conversation to our domain model
    fn convert_conversation(&self, api_conv: &ApiConversation) -> Conversation {
        Conversation {
            id: api_conv.uuid.clone(),
            provider_id: "claude".to_string(),
            title: api_conv.name.clone(),
            created_at: api_conv.created_at,
            updated_at: api_conv.updated_at,
            model: api_conv.model.clone(),
            project_id: api_conv.project_uuid.clone(),
            project_name: None, // Would need separate project fetch
            is_archived: false, // Claude doesn't seem to have this
        }
    }

    /// Convert Claude API message to our domain model
    fn convert_message(&self, conv_id: &str, api_msg: &ApiChatMessage) -> Message {
        let role = match api_msg.sender.as_str() {
            "human" => Role::User,
            "assistant" => Role::Assistant,
            _ => Role::User,
        };

        // Build content from text and any content blocks
        let content = if api_msg.content.is_empty() {
            MessageContent::Text {
                text: api_msg.text.clone(),
            }
        } else {
            // Has structured content - combine text with content blocks
            let mut parts = vec![MessageContent::Text {
                text: api_msg.text.clone(),
            }];

            for block in &api_msg.content {
                match block {
                    ApiContentBlock::Text { text } => {
                        parts.push(MessageContent::Text { text: text.clone() });
                    }
                    ApiContentBlock::ToolUse { name, input, .. } => {
                        parts.push(MessageContent::Code {
                            language: name.clone(),
                            code: serde_json::to_string_pretty(input).unwrap_or_default(),
                        });
                    }
                    _ => {}
                }
            }

            if parts.len() == 1 {
                parts.pop().unwrap()
            } else {
                MessageContent::Mixed { parts }
            }
        };

        Message {
            id: api_msg.uuid.clone(),
            conversation_id: conv_id.to_string(),
            parent_id: None, // Claude uses flat message list, not tree
            role,
            content,
            created_at: api_msg.created_at,
            model: None, // Model is at conversation level in Claude
        }
    }

    /// Fetch a conversation with its attachments (for sync)
    pub async fn conversation_with_attachments(
        &self,
        id: &str,
    ) -> Result<(Conversation, Vec<Message>, Vec<Attachment>)> {
        if self.cookies.is_none() {
            return Err(ProviderError::AuthRequired);
        }

        let org_id = self.get_org_id().await?;
        let url = format!(
            "{}/organizations/{}/chat_conversations/{}",
            API_BASE, org_id, id
        );

        let api_conv: ApiConversation = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| ProviderError::Api(e.to_string()))?
            .json()
            .await?;

        let conversation = self.convert_conversation(&api_conv);
        let messages: Vec<Message> = api_conv
            .chat_messages
            .iter()
            .map(|m| self.convert_message(id, m))
            .collect();
        let attachments = self.extract_attachments(&api_conv);

        Ok((conversation, messages, attachments))
    }

    /// Extract attachments from a conversation's messages
    fn extract_attachments(&self, api_conv: &ApiConversation) -> Vec<Attachment> {
        let mut attachments = Vec::new();

        for msg in &api_conv.chat_messages {
            // Extract from files array (current API format)
            for file in &msg.files {
                if let Some(uuid) = file.uuid() {
                    attachments.push(Attachment {
                        id: uuid.to_string(),
                        message_id: msg.uuid.clone(),
                        filename: file.file_name.clone(),
                        mime_type: file.mime_type(),
                        size_bytes: file.file_size.unwrap_or(0),
                        download_url: uuid.to_string(), // We use file_uuid as the download identifier
                    });
                }
            }

            // Extract from attachments array (legacy format)
            // Skip attachments with extracted_content - these were pasted content, not actual files
            for att in &msg.attachments {
                // If extracted_content exists, the content was inline text that Claude parsed
                // These don't have downloadable files on Claude's servers
                if att.extracted_content.is_some() {
                    continue;
                }

                if let Some(ref id) = att.id {
                    attachments.push(Attachment {
                        id: id.clone(),
                        message_id: msg.uuid.clone(),
                        filename: att.file_name.clone(),
                        mime_type: att.file_type.clone().unwrap_or_else(|| "application/octet-stream".to_string()),
                        size_bytes: att.file_size.unwrap_or(0),
                        download_url: id.clone(),
                    });
                }
            }
        }

        attachments
    }
}

impl Default for ClaudeProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Provider for ClaudeProvider {
    fn id(&self) -> ProviderId {
        ProviderId::claude()
    }

    async fn is_authenticated(&self) -> bool {
        self.cookies.is_some() && self.org_id.is_some()
    }

    async fn authenticate(&mut self) -> Result<Account> {
        // Browser-based authentication flow
        use chromiumoxide::browser::{Browser, BrowserConfig};
        use futures::StreamExt;

        println!("Opening browser for Claude authentication...");
        println!("Please log in to your Claude account.");

        // Set up user data dir to persist session
        let user_data_dir = dirs::data_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("."))
            .join("quaid")
            .join("claude-chrome-profile");
        std::fs::create_dir_all(&user_data_dir).ok();

        let mut builder = BrowserConfig::builder()
            .with_head()
            .user_data_dir(&user_data_dir)
            .arg("--disable-blink-features=AutomationControlled")
            .arg("--disable-infobars")
            .arg("--no-first-run")
            .window_size(1280, 900);

        // Try to find Chrome on the system
        if let Some(chrome_path) = find_chrome() {
            builder = builder.chrome_executable(chrome_path);
        }

        let config = builder
            .build()
            .map_err(|e| ProviderError::AuthFailed(e.to_string()))?;

        let (browser, mut handler) = Browser::launch(config)
            .await
            .map_err(|e| ProviderError::AuthFailed(e.to_string()))?;

        let handle = tokio::spawn(async move {
            while let Some(event) = handler.next().await {
                if event.is_err() {
                    break;
                }
            }
        });

        let page = browser
            .new_page("https://claude.ai/login")
            .await
            .map_err(|e| ProviderError::AuthFailed(e.to_string()))?;

        // Wait for successful login by checking for redirect to /new or /chats
        println!("Waiting for login... (this window will close automatically)");

        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            let url = page.url().await.ok().flatten().unwrap_or_default();

            // Check if we've navigated away from login
            if url.contains("/new") || url.contains("/chats") || url.contains("/chat/") {
                println!("Login detected!");
                break;
            }
        }

        // Extract cookies from browser
        let cookies = page.get_cookies().await.ok().map(|cookies| {
            cookies
                .into_iter()
                .filter(|c| c.domain.contains("claude.ai") || c.domain.contains("anthropic.com"))
                .map(|c| format!("{}={}", c.name, c.value))
                .collect::<Vec<_>>()
                .join("; ")
        });

        // Close browser
        drop(browser);
        handle.abort();

        // Save cookies
        if let Some(ref cookie_str) = cookies {
            if !cookie_str.is_empty() {
                self.cookies = Some(cookie_str.clone());
                self.client = build_client(Some(cookie_str));

                // Fetch org ID
                let org_id = self.get_org_id().await?;
                self.org_id = Some(org_id.clone());

                // Save to credential store
                if let Err(e) = self.credential_store.set(KEYRING_SERVICE, KEYRING_USER_COOKIES, cookie_str) {
                    eprintln!("Warning: failed to save cookies: {}", e);
                }
                if let Err(e) = self.credential_store.set(KEYRING_SERVICE, KEYRING_USER_ORG, &org_id) {
                    eprintln!("Warning: failed to save org ID: {}", e);
                }

                println!("Authentication successful!");
            }
        }

        if self.cookies.is_none() {
            return Err(ProviderError::AuthFailed(
                "Could not extract session cookies".to_string(),
            ));
        }

        self.account().await
    }

    async fn account(&self) -> Result<Account> {
        if self.cookies.is_none() {
            return Err(ProviderError::AuthRequired);
        }

        let api_account = self.fetch_account().await?;

        Ok(Account {
            id: api_account.uuid.clone(),
            provider: ProviderId::claude(),
            email: api_account.email.clone().unwrap_or_else(|| "unknown".to_string()),
            name: api_account.best_name(),
            avatar_url: api_account.avatar_url,
        })
    }

    async fn conversations(&self) -> Result<Vec<Conversation>> {
        if self.cookies.is_none() {
            return Err(ProviderError::AuthRequired);
        }

        let org_id = self.get_org_id().await?;
        let url = format!("{}/organizations/{}/chat_conversations", API_BASE, org_id);

        let api_convs: Vec<ApiConversationItem> = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| ProviderError::Api(e.to_string()))?
            .json()
            .await?;

        let conversations = api_convs
            .iter()
            .map(|c| Conversation {
                id: c.uuid.clone(),
                provider_id: "claude".to_string(),
                title: c.name.clone(),
                created_at: c.created_at,
                updated_at: c.updated_at,
                model: c.model.clone(),
                project_id: c.project_uuid.clone(),
                project_name: None,
                is_archived: false,
            })
            .collect();

        Ok(conversations)
    }

    async fn conversation(&self, id: &str) -> Result<(Conversation, Vec<Message>)> {
        if self.cookies.is_none() {
            return Err(ProviderError::AuthRequired);
        }

        let org_id = self.get_org_id().await?;
        let url = format!(
            "{}/organizations/{}/chat_conversations/{}",
            API_BASE, org_id, id
        );

        let api_conv: ApiConversation = self
            .client
            .get(&url)
            .send()
            .await?
            .error_for_status()
            .map_err(|e| ProviderError::Api(e.to_string()))?
            .json()
            .await?;

        let conversation = self.convert_conversation(&api_conv);
        let messages: Vec<Message> = api_conv
            .chat_messages
            .iter()
            .map(|m| self.convert_message(id, m))
            .collect();

        Ok((conversation, messages))
    }

    async fn project_conversations(&self, project_id: &str) -> Result<Vec<Conversation>> {
        // Filter conversations by project_id
        let all_convs = self.conversations().await?;
        Ok(all_convs
            .into_iter()
            .filter(|c| c.project_id.as_deref() == Some(project_id))
            .collect())
    }

    async fn download_attachment(
        &self,
        attachment: &Attachment,
        path: &Path,
    ) -> Result<()> {
        if self.cookies.is_none() {
            return Err(ProviderError::AuthRequired);
        }

        let org_id = self.get_org_id().await?;

        // The download_url should be the file_uuid
        // URL pattern: /api/{org_id}/files/{file_uuid}/preview
        let file_uuid = &attachment.download_url;
        let url = format!("{}/{}/files/{}/preview", API_BASE, org_id, file_uuid);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ProviderError::Api(format!(
                "Failed to download file: {}",
                response.status()
            )));
        }

        let bytes = response.bytes().await?;

        tokio::fs::write(path, bytes)
            .await
            .map_err(|e| ProviderError::Api(format!("Failed to write file: {}", e)))?;

        Ok(())
    }
}

/// Build HTTP client with browser-like headers
fn build_client(cookies: Option<&str>) -> Client {
    let mut headers = header::HeaderMap::new();

    headers.insert(
        header::USER_AGENT,
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            .parse()
            .unwrap(),
    );
    headers.insert(
        header::ACCEPT,
        "application/json, text/plain, */*".parse().unwrap(),
    );
    headers.insert(
        header::ACCEPT_LANGUAGE,
        "en-US,en;q=0.9".parse().unwrap(),
    );
    headers.insert(header::ACCEPT_ENCODING, "gzip, deflate, br".parse().unwrap());
    headers.insert("Sec-Fetch-Dest", "empty".parse().unwrap());
    headers.insert("Sec-Fetch-Mode", "cors".parse().unwrap());
    headers.insert("Sec-Fetch-Site", "same-origin".parse().unwrap());
    headers.insert(header::REFERER, "https://claude.ai/".parse().unwrap());
    headers.insert(header::ORIGIN, "https://claude.ai".parse().unwrap());

    if let Some(cookie_str) = cookies {
        if let Ok(cookie_val) = cookie_str.parse() {
            headers.insert(header::COOKIE, cookie_val);
        }
    }

    Client::builder()
        .default_headers(headers)
        .cookie_store(true)
        .gzip(true)
        .brotli(true)
        .deflate(true)
        .build()
        .expect("Failed to build HTTP client")
}

/// Safely truncate a string at a char boundary
fn truncate_body(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        // Find a valid char boundary
        let mut end = max_len;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}...", &s[..end])
    }
}

/// Find Chrome/Chromium executable on the system
fn find_chrome() -> Option<std::path::PathBuf> {
    let candidates = if cfg!(target_os = "macos") {
        vec![
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
            "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
            "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
            "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        ]
    } else if cfg!(target_os = "linux") {
        vec![
            "/usr/bin/google-chrome",
            "/usr/bin/google-chrome-stable",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/snap/bin/chromium",
        ]
    } else {
        // Windows - chromiumoxide should handle this
        vec![]
    };

    for candidate in candidates {
        let path = std::path::PathBuf::from(candidate);
        if path.exists() {
            return Some(path);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_provider_id() {
        let provider = ClaudeProvider::with_credentials(None, None);
        assert_eq!(provider.id(), ProviderId::claude());
    }

    #[test]
    fn test_not_authenticated_without_credentials() {
        let provider = ClaudeProvider::with_credentials(None, None);
        let rt = tokio::runtime::Runtime::new().unwrap();
        assert!(!rt.block_on(provider.is_authenticated()));
    }

    #[test]
    fn test_authenticated_with_credentials() {
        let provider = ClaudeProvider::with_credentials(
            Some("session=abc123".to_string()),
            Some("org-123".to_string()),
        );
        let rt = tokio::runtime::Runtime::new().unwrap();
        assert!(rt.block_on(provider.is_authenticated()));
    }

    #[test]
    fn test_convert_message_human() {
        let provider = ClaudeProvider::with_credentials(None, None);
        let api_msg = ApiChatMessage {
            uuid: "msg-1".to_string(),
            sender: "human".to_string(),
            text: "Hello!".to_string(),
            created_at: Some(Utc::now()),
            updated_at: None,
            attachments: vec![],
            files: vec![],
            content: vec![],
        };

        let msg = provider.convert_message("conv-1", &api_msg);
        assert_eq!(msg.role, Role::User);
        assert_eq!(msg.id, "msg-1");
        assert_eq!(msg.conversation_id, "conv-1");
    }

    #[test]
    fn test_convert_message_assistant() {
        let provider = ClaudeProvider::with_credentials(None, None);
        let api_msg = ApiChatMessage {
            uuid: "msg-2".to_string(),
            sender: "assistant".to_string(),
            text: "Hello! How can I help?".to_string(),
            created_at: Some(Utc::now()),
            updated_at: None,
            attachments: vec![],
            files: vec![],
            content: vec![],
        };

        let msg = provider.convert_message("conv-1", &api_msg);
        assert_eq!(msg.role, Role::Assistant);
        match msg.content {
            MessageContent::Text { text } => assert!(text.contains("How can I help")),
            _ => panic!("Expected Text content"),
        }
    }

    #[test]
    fn test_convert_conversation() {
        let provider = ClaudeProvider::with_credentials(None, None);
        let now = Utc::now();
        let api_conv = ApiConversation {
            uuid: "conv-123".to_string(),
            name: "Test Chat".to_string(),
            created_at: now,
            updated_at: now,
            chat_messages: vec![],
            summary: None,
            model: Some("claude-3-opus".to_string()),
            project_uuid: Some("proj-1".to_string()),
        };

        let conv = provider.convert_conversation(&api_conv);
        assert_eq!(conv.id, "conv-123");
        assert_eq!(conv.title, "Test Chat");
        assert_eq!(conv.provider_id, "claude");
        assert_eq!(conv.model, Some("claude-3-opus".to_string()));
        assert_eq!(conv.project_id, Some("proj-1".to_string()));
    }

    #[test]
    fn test_build_client_with_cookies() {
        let client = build_client(Some("session=test123"));
        // Client should be built successfully
        assert!(client.get("https://example.com").build().is_ok());
    }

    #[test]
    fn test_build_client_without_cookies() {
        let client = build_client(None);
        assert!(client.get("https://example.com").build().is_ok());
    }
}
