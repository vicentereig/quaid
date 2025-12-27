mod types;

use crate::providers::{
    Account, Attachment, Conversation, Message, MessageContent, Provider, ProviderId,
    ProviderError, Result, Role,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use chromiumoxide::browser::{Browser, BrowserConfig};
use futures::StreamExt;
use reqwest::Client;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

pub use types::*;

const BASE_URL: &str = "https://chatgpt.com";
const API_URL: &str = "https://chatgpt.com/backend-api";

const KEYRING_SERVICE: &str = "quaid";
const KEYRING_USER: &str = "chatgpt-token";

/// ChatGPT provider implementation
pub struct ChatGptProvider {
    client: Client,
    token: Arc<RwLock<Option<String>>>,
    account_id: Arc<RwLock<Option<String>>>, // For team accounts
}

impl ChatGptProvider {
    pub fn new() -> Self {
        // Try to load token from keyring
        let stored_token = Self::load_token_from_keyring();

        Self {
            client: Client::builder()
                .cookie_store(true)
                .build()
                .expect("Failed to create HTTP client"),
            token: Arc::new(RwLock::new(stored_token)),
            account_id: Arc::new(RwLock::new(None)),
        }
    }

    /// Create with an existing token (for testing or restored sessions)
    pub fn with_token(token: String) -> Self {
        Self {
            client: Client::builder()
                .cookie_store(true)
                .build()
                .expect("Failed to create HTTP client"),
            token: Arc::new(RwLock::new(Some(token))),
            account_id: Arc::new(RwLock::new(None)),
        }
    }

    /// Load token from system keyring
    fn load_token_from_keyring() -> Option<String> {
        keyring::Entry::new(KEYRING_SERVICE, KEYRING_USER)
            .ok()
            .and_then(|entry| entry.get_password().ok())
    }

    /// Save token to system keyring
    fn save_token_to_keyring(token: &str) -> Result<()> {
        keyring::Entry::new(KEYRING_SERVICE, KEYRING_USER)
            .map_err(|e| ProviderError::AuthFailed(format!("Keyring error: {}", e)))?
            .set_password(token)
            .map_err(|e| ProviderError::AuthFailed(format!("Failed to save token: {}", e)))
    }

    async fn get_token(&self) -> Result<String> {
        let token = self.token.read().await;
        token.clone().ok_or(ProviderError::AuthRequired)
    }

    async fn api_get<T: serde::de::DeserializeOwned>(&self, endpoint: &str) -> Result<T> {
        let token = self.get_token().await?;
        let url = format!("{}{}", API_URL, endpoint);

        let mut req = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .header("X-Authorization", format!("Bearer {}", token));

        // Add team account header if present
        if let Some(account_id) = self.account_id.read().await.as_ref() {
            req = req.header("Chatgpt-Account-Id", account_id);
        }

        let response = req.send().await?;

        if response.status() == 401 {
            return Err(ProviderError::TokenExpired);
        }

        if response.status() == 429 {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(60);
            return Err(ProviderError::RateLimited(retry_after));
        }

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(ProviderError::Api(format!("{}: {}", status, text)));
        }

        response
            .json()
            .await
            .map_err(|e| ProviderError::Parse(e.to_string()))
    }

    /// Fetch all conversations with pagination
    async fn fetch_all_conversations(&self) -> Result<Vec<ApiConversationItem>> {
        let mut conversations = Vec::new();
        let mut offset = 0;
        let limit = 100;

        loop {
            let result: ApiConversations = self
                .api_get(&format!("/conversations?offset={}&limit={}", offset, limit))
                .await?;

            if result.items.is_empty() {
                break;
            }

            conversations.extend(result.items);

            if let Some(total) = result.total {
                if offset + limit >= total as usize {
                    break;
                }
            }

            offset += limit;
        }

        Ok(conversations)
    }

    /// Convert API conversation to our unified format
    fn convert_conversation(api: &ApiConversation, id: &str) -> Conversation {
        Conversation {
            id: id.to_string(),
            provider_id: "chatgpt".to_string(),
            title: api.title.clone(),
            created_at: timestamp_to_datetime(api.create_time),
            updated_at: timestamp_to_datetime(api.update_time),
            model: extract_model_from_mapping(&api.mapping),
            project_id: None,
            project_name: None,
            is_archived: api.is_archived,
        }
    }

    /// Extract messages from the conversation mapping
    fn extract_messages(api: &ApiConversation) -> Vec<Message> {
        let mut messages = Vec::new();

        // Find the current node and traverse backwards
        let Some(start_id) = api.current_node.as_ref() else {
            return messages;
        };

        let mut current_id = Some(start_id.clone());
        let mut nodes = Vec::new();

        // Traverse from current node to root
        while let Some(id) = current_id {
            if let Some(node) = api.mapping.get(&id) {
                if node.parent.is_some() {
                    // Skip root node
                    nodes.push(node.clone());
                }
                current_id = node.parent.clone();
            } else {
                break;
            }
        }

        // Reverse to get chronological order
        nodes.reverse();

        // Convert nodes to messages
        for node in nodes {
            if let Some(msg) = &node.message {
                // Skip system and context messages
                if msg.author.role == "system" {
                    continue;
                }
                if let Some(content_type) = msg.content.get("content_type").and_then(|v| v.as_str())
                {
                    if content_type == "user_editable_context"
                        || content_type == "model_editable_context"
                    {
                        continue;
                    }
                }

                // Skip messages not intended for "all"
                if msg.recipient.as_deref() != Some("all") && msg.author.role != "user" {
                    continue;
                }

                if let Some(message) = convert_api_message(msg, &node.id) {
                    messages.push(message);
                }
            }
        }

        messages
    }
}

impl Default for ChatGptProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Provider for ChatGptProvider {
    fn id(&self) -> ProviderId {
        ProviderId::chatgpt()
    }

    async fn is_authenticated(&self) -> bool {
        self.token.read().await.is_some()
    }

    async fn authenticate(&mut self) -> Result<Account> {
        // Launch browser for user to log in
        // Create a persistent user data directory so Chrome looks like a real browser
        let user_data_dir = dirs::data_dir()
            .unwrap_or_else(|| std::path::PathBuf::from("."))
            .join("quaid")
            .join("chrome-profile");
        std::fs::create_dir_all(&user_data_dir).ok();

        let mut builder = BrowserConfig::builder()
            .with_head() // Show browser window
            .user_data_dir(&user_data_dir)
            .arg("--disable-blink-features=AutomationControlled")
            .arg("--no-first-run")
            .arg("--no-default-browser-check")
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

        // Spawn handler task
        let handle = tokio::spawn(async move {
            while let Some(event) = handler.next().await {
                if let Err(e) = event {
                    eprintln!("Browser event error: {:?}", e);
                }
            }
        });

        // Navigate to ChatGPT
        let page = browser
            .new_page(BASE_URL)
            .await
            .map_err(|e| ProviderError::AuthFailed(e.to_string()))?;

        // Wait for user to log in by polling for session
        println!("Please log in to ChatGPT in the browser window...");
        println!("(Waiting for authentication...)");

        let (token, account) = loop {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;

            // Check current URL to see if we're on the main chat page
            let url = page.url().await.ok().flatten().unwrap_or_default();

            // Only try to get token if we're on the main site (not login page)
            if url.contains("chatgpt.com") && !url.contains("auth") && !url.contains("login") {
                // Try to fetch session by evaluating JS in the page
                let result = page
                    .evaluate(r#"
                        (async () => {
                            try {
                                const r = await fetch('/api/auth/session', { credentials: 'include' });
                                if (r.ok) {
                                    return await r.json();
                                }
                                return { error: r.status };
                            } catch (e) {
                                return { error: e.message };
                            }
                        })()
                    "#)
                    .await;

                match result {
                    Ok(eval_result) => {
                        if let Some(value) = eval_result.value() {
                            // Check for error
                            if let Some(err) = value.get("error") {
                                eprintln!("Session fetch error: {:?}", err);
                                continue;
                            }

                            if let Some(access_token) = value.get("accessToken").and_then(|v| v.as_str()) {
                                if !access_token.is_empty() {
                                    println!("Authentication successful!");

                                    // Extract user info from the same response
                                    let user = value.get("user");
                                    let account = Account {
                                        id: user.and_then(|u| u.get("id")).and_then(|v| v.as_str()).unwrap_or("unknown").to_string(),
                                        provider: ProviderId::chatgpt(),
                                        email: user.and_then(|u| u.get("email")).and_then(|v| v.as_str()).unwrap_or("unknown").to_string(),
                                        name: user.and_then(|u| u.get("name")).and_then(|v| v.as_str()).map(|s| s.to_string()),
                                        avatar_url: user.and_then(|u| u.get("picture")).and_then(|v| v.as_str()).map(|s| s.to_string()),
                                    };

                                    break (access_token.to_string(), account);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Evaluate error: {:?}", e);
                    }
                }
            }
        };

        // Store the token in memory and keyring
        *self.token.write().await = Some(token.clone());
        Self::save_token_to_keyring(&token)?;

        // Close browser
        drop(browser);
        handle.abort();

        // Save account and return
        Ok(account)
    }

    async fn account(&self) -> Result<Account> {
        // Session endpoint is at base URL, not the backend-api
        let token = self.get_token().await?;
        let url = format!("{}/api/auth/session", BASE_URL);

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(ProviderError::AuthFailed("Failed to fetch session".to_string()));
        }

        let session: ApiSession = response
            .json()
            .await
            .map_err(|e| ProviderError::Parse(e.to_string()))?;

        Ok(Account {
            id: session.user.id,
            provider: ProviderId::chatgpt(),
            email: session.user.email,
            name: Some(session.user.name),
            avatar_url: Some(session.user.picture),
        })
    }

    async fn conversations(&self) -> Result<Vec<Conversation>> {
        let items = self.fetch_all_conversations().await?;

        Ok(items
            .iter()
            .map(|item| Conversation {
                id: item.id.clone(),
                provider_id: "chatgpt".to_string(),
                title: item.title.clone(),
                created_at: timestamp_to_datetime(item.create_time),
                updated_at: timestamp_to_datetime(item.create_time), // API doesn't give update_time in list
                model: None,
                project_id: None,
                project_name: None,
                is_archived: false,
            })
            .collect())
    }

    async fn conversation(&self, id: &str) -> Result<(Conversation, Vec<Message>)> {
        let api: ApiConversation = self.api_get(&format!("/conversation/{}", id)).await?;

        let conversation = Self::convert_conversation(&api, id);
        let messages = Self::extract_messages(&api);

        Ok((conversation, messages))
    }

    async fn project_conversations(&self, project_id: &str) -> Result<Vec<Conversation>> {
        let mut conversations = Vec::new();
        let mut offset = 0;
        let limit = 50;

        loop {
            let result: ApiProjectConversations = self
                .api_get(&format!(
                    "/gizmos/{}/conversations?cursor={}&limit={}",
                    project_id, offset, limit
                ))
                .await?;

            if result.items.is_empty() {
                break;
            }

            for item in &result.items {
                conversations.push(Conversation {
                    id: item.id.clone(),
                    provider_id: "chatgpt".to_string(),
                    title: item.title.clone(),
                    created_at: timestamp_to_datetime(item.create_time),
                    updated_at: timestamp_to_datetime(item.create_time),
                    model: None,
                    project_id: Some(project_id.to_string()),
                    project_name: None,
                    is_archived: false,
                });
            }

            if result.cursor.is_none() {
                break;
            }

            offset += limit;
        }

        Ok(conversations)
    }

    async fn download_attachment(
        &self,
        attachment: &Attachment,
        path: &Path,
    ) -> Result<()> {
        // Get signed download URL
        let file_id = attachment
            .download_url
            .strip_prefix("file-service://")
            .unwrap_or(&attachment.download_url);

        let download_info: ApiFileDownload =
            self.api_get(&format!("/files/{}/download", file_id)).await?;

        match download_info {
            ApiFileDownload::Success { download_url, .. } => {
                // Download the file
                let response = self.client.get(&download_url).send().await?;
                let bytes = response.bytes().await?;

                // Write to path
                tokio::fs::write(path, bytes)
                    .await
                    .map_err(|e| ProviderError::Api(format!("Failed to write file: {}", e)))?;

                Ok(())
            }
            ApiFileDownload::Error { error_message, .. } => Err(ProviderError::Api(
                error_message.unwrap_or_else(|| "Unknown download error".to_string()),
            )),
        }
    }
}

// Helper functions

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

    for path in candidates {
        let p = std::path::PathBuf::from(path);
        if p.exists() {
            return Some(p);
        }
    }

    None
}

fn timestamp_to_datetime(ts: f64) -> DateTime<Utc> {
    DateTime::from_timestamp(ts as i64, ((ts.fract()) * 1_000_000_000.0) as u32)
        .unwrap_or_else(Utc::now)
}

fn extract_model_from_mapping(
    mapping: &std::collections::HashMap<String, ApiConversationNode>,
) -> Option<String> {
    for node in mapping.values() {
        if let Some(msg) = &node.message {
            if let Some(meta) = &msg.metadata {
                if let Some(slug) = &meta.model_slug {
                    return Some(slug.clone());
                }
            }
        }
    }
    None
}

fn convert_api_message(msg: &ApiNodeMessage, node_id: &str) -> Option<Message> {
    let role = match msg.author.role.as_str() {
        "user" => Role::User,
        "assistant" => Role::Assistant,
        "system" => Role::System,
        "tool" => Role::Tool,
        _ => return None,
    };

    let content = convert_content(&msg.content)?;

    Some(Message {
        id: msg.id.clone().unwrap_or_else(|| node_id.to_string()),
        conversation_id: String::new(), // Filled in by caller
        parent_id: None,                // Could be extracted from node.parent
        role,
        content,
        created_at: msg.create_time.map(timestamp_to_datetime),
        model: msg.metadata.as_ref().and_then(|m| m.model_slug.clone()),
    })
}

fn convert_content(content: &serde_json::Value) -> Option<MessageContent> {
    let content_type = content.get("content_type")?.as_str()?;

    match content_type {
        "text" => {
            let parts = content.get("parts")?.as_array()?;
            let text = parts
                .iter()
                .filter_map(|p| p.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            Some(MessageContent::Text { text })
        }
        "code" => {
            let code = content.get("text")?.as_str()?.to_string();
            let language = content
                .get("language")
                .and_then(|l| l.as_str())
                .unwrap_or("unknown")
                .to_string();
            Some(MessageContent::Code { language, code })
        }
        "multimodal_text" => {
            let parts = content.get("parts")?.as_array()?;
            let mut message_parts = Vec::new();

            for part in parts {
                if let Some(text) = part.as_str() {
                    message_parts.push(MessageContent::Text {
                        text: text.to_string(),
                    });
                } else if let Some(content_type) = part.get("content_type").and_then(|c| c.as_str())
                {
                    match content_type {
                        "image_asset_pointer" => {
                            if let Some(url) = part.get("asset_pointer").and_then(|u| u.as_str()) {
                                message_parts.push(MessageContent::Image {
                                    url: url.to_string(),
                                    alt: None,
                                });
                            }
                        }
                        "audio_transcription" => {
                            if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                                message_parts.push(MessageContent::Text {
                                    text: format!("[audio] {}", text),
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }

            if message_parts.len() == 1 {
                Some(message_parts.remove(0))
            } else {
                Some(MessageContent::Mixed {
                    parts: message_parts,
                })
            }
        }
        "execution_output" => {
            let text = content.get("text")?.as_str()?.to_string();
            Some(MessageContent::Code {
                language: "output".to_string(),
                code: text,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_id() {
        let provider = ChatGptProvider::new();
        assert_eq!(provider.id(), ProviderId::chatgpt());
    }

    #[test]
    fn test_timestamp_conversion() {
        let ts = 1725512345.12345;
        let dt = timestamp_to_datetime(ts);
        assert_eq!(dt.timestamp(), 1725512345);
    }

    #[test]
    fn test_convert_text_content() {
        let content = serde_json::json!({
            "content_type": "text",
            "parts": ["Hello, ", "world!"]
        });

        let result = convert_content(&content).unwrap();
        match result {
            MessageContent::Text { text } => assert_eq!(text, "Hello, \nworld!"),
            _ => panic!("Expected Text content"),
        }
    }

    #[test]
    fn test_convert_code_content() {
        let content = serde_json::json!({
            "content_type": "code",
            "language": "python",
            "text": "print('hello')"
        });

        let result = convert_content(&content).unwrap();
        match result {
            MessageContent::Code { language, code } => {
                assert_eq!(language, "python");
                assert_eq!(code, "print('hello')");
            }
            _ => panic!("Expected Code content"),
        }
    }

    #[test]
    fn test_convert_multimodal_content() {
        let content = serde_json::json!({
            "content_type": "multimodal_text",
            "parts": [
                "Check this image:",
                {
                    "content_type": "image_asset_pointer",
                    "asset_pointer": "file-service://abc123"
                }
            ]
        });

        let result = convert_content(&content).unwrap();
        match result {
            MessageContent::Mixed { parts } => {
                assert_eq!(parts.len(), 2);
            }
            _ => panic!("Expected Mixed content"),
        }
    }

    #[tokio::test]
    async fn test_provider_unauthenticated() {
        let provider = ChatGptProvider::new();
        assert!(!provider.is_authenticated().await);
    }

    #[tokio::test]
    async fn test_provider_with_token() {
        let provider = ChatGptProvider::with_token("test-token".to_string());
        assert!(provider.is_authenticated().await);
    }

    #[tokio::test]
    async fn test_get_token_when_none() {
        let provider = ChatGptProvider::new();
        let result = provider.get_token().await;
        assert!(matches!(result, Err(ProviderError::AuthRequired)));
    }
}
