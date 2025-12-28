//! Fathom.video provider implementation
//!
//! Syncs meeting recordings, transcripts, and summaries from Fathom
//! using their official public API.
//!
//! API Documentation: https://developers.fathom.ai

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
use tokio::sync::RwLock;
use types::*;

const API_BASE: &str = "https://api.fathom.ai/external/v1";
const KEYRING_SERVICE: &str = "quaid";
const KEYRING_API_KEY: &str = "fathom-api-key";

/// Fathom.video provider
pub struct FathomProvider {
    client: Client,
    api_key: Arc<RwLock<Option<String>>>,
    credential_store: Arc<dyn CredentialStore>,
}

impl FathomProvider {
    /// Create a new Fathom provider, loading API key from keyring if available
    pub fn new() -> Self {
        Self::with_credential_store(Arc::new(KeyringStore::new()))
    }

    /// Create with a custom credential store (for testing)
    pub fn with_credential_store(credential_store: Arc<dyn CredentialStore>) -> Self {
        let api_key = credential_store
            .get(KEYRING_SERVICE, KEYRING_API_KEY)
            .ok();

        Self {
            client: build_client(),
            api_key: Arc::new(RwLock::new(api_key)),
            credential_store,
        }
    }

    /// Create a provider with an explicit API key (for testing)
    pub fn with_api_key(api_key: String) -> Self {
        Self {
            client: build_client(),
            api_key: Arc::new(RwLock::new(Some(api_key))),
            credential_store: Arc::new(KeyringStore::new()),
        }
    }

    /// Get the current API key
    async fn get_api_key(&self) -> Result<String> {
        self.api_key
            .read()
            .await
            .clone()
            .ok_or(ProviderError::AuthRequired)
    }

    /// Make an authenticated GET request
    async fn api_get<T: serde::de::DeserializeOwned>(&self, endpoint: &str) -> Result<T> {
        let api_key = self.get_api_key().await?;
        let url = format!("{}{}", API_BASE, endpoint);

        let response = self
            .client
            .get(&url)
            .header("X-Api-Key", &api_key)
            .send()
            .await?;

        let status = response.status();

        if status == 401 {
            return Err(ProviderError::AuthFailed("Invalid API key".to_string()));
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
            return Err(ProviderError::Api(format!("{}: {}", status, truncate(&text, 500))));
        }

        let text = response.text().await?;
        serde_json::from_str(&text).map_err(|e| {
            ProviderError::Parse(format!("{}: {}", e, truncate(&text, 200)))
        })
    }

    /// Fetch all meetings with transcripts (public for efficient bulk sync)
    pub async fn fetch_all_meetings_with_transcripts(&self) -> Result<Vec<ApiMeeting>> {
        self.fetch_all_meetings(true).await
    }

    /// Convert a meeting to conversation + messages (public for bulk sync)
    pub fn meeting_to_data(&self, meeting: &ApiMeeting) -> (Conversation, Vec<Message>) {
        let conversation = Self::meeting_to_conversation(meeting);
        let mut messages = Self::transcript_to_messages(&meeting.id(), &meeting.transcript);

        if let Some(summary_msg) = Self::build_summary_message(meeting) {
            messages.insert(0, summary_msg);
        }

        (conversation, messages)
    }

    /// Fetch all meetings with pagination
    async fn fetch_all_meetings(&self, include_transcript: bool) -> Result<Vec<ApiMeeting>> {
        let mut meetings = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let mut endpoint = "/meetings?limit=100".to_string();
            if include_transcript {
                endpoint.push_str("&include_transcript=true");
            }
            if let Some(ref c) = cursor {
                endpoint.push_str(&format!("&cursor={}", c));
            }

            let response: ApiMeetingsResponse = self.api_get(&endpoint).await?;
            meetings.extend(response.items);

            match response.next_cursor {
                Some(next) if !next.is_empty() => cursor = Some(next),
                _ => break,
            }
        }

        Ok(meetings)
    }

    /// Convert a Fathom meeting to our Conversation type
    fn meeting_to_conversation(meeting: &ApiMeeting) -> Conversation {
        let updated_at = meeting
            .recording_end_time
            .or(meeting.scheduled_end_time)
            .unwrap_or(meeting.created_at);

        Conversation {
            id: meeting.id(),
            provider_id: "fathom".to_string(),
            title: meeting.display_title(),
            created_at: meeting.created_at,
            updated_at,
            model: None, // Fathom doesn't have a model concept
            project_id: meeting.recorded_by.as_ref().and_then(|r| r.team.clone()),
            project_name: meeting.recorded_by.as_ref().and_then(|r| r.team.clone()),
            is_archived: false,
        }
    }

    /// Convert transcript entries to Messages
    fn transcript_to_messages(meeting_id: &str, transcript: &[ApiTranscriptEntry]) -> Vec<Message> {
        transcript
            .iter()
            .enumerate()
            .map(|(idx, entry)| {
                // Format text with speaker name
                let speaker = entry
                    .speaker
                    .display_name
                    .clone()
                    .unwrap_or_else(|| "Speaker".to_string());
                let text = format!("**{}**: {}", speaker, entry.text);

                Message {
                    id: format!("{}-{}", meeting_id, idx),
                    conversation_id: meeting_id.to_string(),
                    parent_id: if idx > 0 {
                        Some(format!("{}-{}", meeting_id, idx - 1))
                    } else {
                        None
                    },
                    role: Role::User, // All transcript entries are "user" speech
                    content: MessageContent::Text { text },
                    created_at: None, // Individual timestamps are relative, not absolute
                    model: None,
                }
            })
            .collect()
    }

    /// Build a summary message from meeting data
    fn build_summary_message(meeting: &ApiMeeting) -> Option<Message> {
        let summary = meeting.default_summary.as_ref()?;
        let markdown = summary.markdown_formatted.as_ref()?;

        if markdown.is_empty() {
            return None;
        }

        // Build a comprehensive summary including action items
        let mut content = markdown.clone();

        if !meeting.action_items.is_empty() {
            content.push_str("\n\n## Action Items\n\n");
            for item in &meeting.action_items {
                let assignee = item
                    .assignee
                    .as_ref()
                    .and_then(|a| a.name.as_ref())
                    .map(|n| format!(" ({})", n))
                    .unwrap_or_default();
                content.push_str(&format!("- {}{}\n", item.description, assignee));
            }
        }

        Some(Message {
            id: format!("{}-summary", meeting.id()),
            conversation_id: meeting.id(),
            parent_id: None,
            role: Role::Assistant, // Summary is AI-generated
            content: MessageContent::Text { text: content },
            created_at: Some(meeting.created_at),
            model: Some("fathom-ai".to_string()),
        })
    }
}

impl Default for FathomProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Provider for FathomProvider {
    fn id(&self) -> ProviderId {
        ProviderId(String::from("fathom"))
    }

    async fn is_authenticated(&self) -> bool {
        self.api_key.read().await.is_some()
    }

    async fn authenticate(&mut self) -> Result<Account> {
        // Fathom uses API keys, not OAuth
        // User needs to provide their API key manually
        println!("Fathom uses API key authentication.");
        println!("Get your API key from: https://fathom.video/api_settings/new");
        println!();
        print!("Enter your Fathom API key: ");

        // Read from stdin
        use std::io::{self, Write};
        io::stdout().flush().map_err(|e| ProviderError::AuthFailed(e.to_string()))?;

        let mut api_key = String::new();
        io::stdin()
            .read_line(&mut api_key)
            .map_err(|e| ProviderError::AuthFailed(e.to_string()))?;

        let api_key = api_key.trim().to_string();

        if api_key.is_empty() {
            return Err(ProviderError::AuthFailed("No API key provided".to_string()));
        }

        // Validate by making a test request
        *self.api_key.write().await = Some(api_key.clone());

        // Try to fetch meetings to validate the key
        match self.api_get::<ApiMeetingsResponse>("/meetings?limit=1").await {
            Ok(_) => {
                // Save to credential store
                if let Err(e) = self.credential_store.set(KEYRING_SERVICE, KEYRING_API_KEY, &api_key) {
                    eprintln!("Warning: failed to save API key: {}", e);
                }
                println!("API key validated and saved!");
                self.account().await
            }
            Err(e) => {
                *self.api_key.write().await = None;
                Err(ProviderError::AuthFailed(format!("Invalid API key: {}", e)))
            }
        }
    }

    async fn account(&self) -> Result<Account> {
        // Fathom API doesn't have a dedicated user endpoint
        // We'll create a basic account from what we know
        let api_key = self.get_api_key().await?;

        // Try to get some info from meetings to identify the user
        let response: ApiMeetingsResponse = self.api_get("/meetings?limit=1").await?;

        let (email, name) = response
            .items
            .first()
            .and_then(|m| m.recorded_by.as_ref())
            .map(|r| (r.email.clone(), r.name.clone()))
            .unwrap_or((None, None));

        Ok(Account {
            id: format!("fathom-{}", &api_key[..8.min(api_key.len())]),
            provider: self.id(),
            email: email.unwrap_or_else(|| "unknown".to_string()),
            name,
            avatar_url: None,
        })
    }

    async fn conversations(&self) -> Result<Vec<Conversation>> {
        let meetings = self.fetch_all_meetings(false).await?;
        Ok(meetings.iter().map(Self::meeting_to_conversation).collect())
    }

    async fn conversation(&self, id: &str) -> Result<(Conversation, Vec<Message>)> {
        // Fetch all meetings with transcripts and find the one we need
        // (Fathom API doesn't have a single-meeting endpoint)
        let meetings = self.fetch_all_meetings(true).await?;

        let meeting = meetings
            .into_iter()
            .find(|m| m.id() == id)
            .ok_or_else(|| ProviderError::Api(format!("Meeting {} not found", id)))?;

        let conversation = Self::meeting_to_conversation(&meeting);

        // Build messages from transcript
        let mut messages = Self::transcript_to_messages(id, &meeting.transcript);

        // Add summary as a special message at the beginning
        if let Some(summary_msg) = Self::build_summary_message(&meeting) {
            messages.insert(0, summary_msg);
        }

        Ok((conversation, messages))
    }

    async fn project_conversations(&self, project_id: &str) -> Result<Vec<Conversation>> {
        // Filter by team ID
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
        // Fathom doesn't have traditional attachments
        // Video recordings might be downloadable via share_url
        Err(ProviderError::Api(
            "Attachment download not supported for Fathom".to_string(),
        ))
    }
}

/// Build HTTP client with appropriate headers
fn build_client() -> Client {
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::ACCEPT,
        "application/json".parse().unwrap(),
    );
    headers.insert(
        header::CONTENT_TYPE,
        "application/json".parse().unwrap(),
    );

    Client::builder()
        .default_headers(headers)
        .build()
        .expect("Failed to build HTTP client")
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
    use crate::credentials::MockStore;

    #[test]
    fn test_provider_id() {
        let provider = FathomProvider::with_credential_store(Arc::new(MockStore::new()));
        assert_eq!(provider.id().0, "fathom");
    }

    #[tokio::test]
    async fn test_not_authenticated_without_key() {
        let provider = FathomProvider::with_credential_store(Arc::new(MockStore::new()));
        assert!(!provider.is_authenticated().await);
    }

    #[tokio::test]
    async fn test_authenticated_with_key() {
        let provider = FathomProvider::with_api_key("test-key".to_string());
        assert!(provider.is_authenticated().await);
    }

    #[tokio::test]
    async fn test_provider_with_stored_credentials() {
        let store = MockStore::with_credentials(vec![
            (KEYRING_SERVICE, KEYRING_API_KEY, "stored-api-key"),
        ]);
        let provider = FathomProvider::with_credential_store(Arc::new(store));
        assert!(provider.is_authenticated().await);
    }

    #[test]
    fn test_meeting_to_conversation() {
        let meeting = ApiMeeting {
            title: Some("Team Sync".to_string()),
            meeting_title: None,
            url: "https://fathom.video/calls/meeting-123".to_string(),
            share_url: None,
            created_at: chrono::Utc::now(),
            scheduled_start_time: None,
            scheduled_end_time: None,
            recording_start_time: None,
            recording_end_time: None,
            meeting_type: None,
            transcript_language: None,
            calendar_invitees: vec![],
            recorded_by: None,
            transcript: vec![],
            default_summary: None,
            action_items: vec![],
            crm_matches: None,
        };

        let conv = FathomProvider::meeting_to_conversation(&meeting);
        assert_eq!(conv.id, "meeting-123");
        assert_eq!(conv.title, "Team Sync");
        assert_eq!(conv.provider_id, "fathom");
    }

    #[test]
    fn test_transcript_to_messages() {
        let transcript = vec![
            ApiTranscriptEntry {
                speaker: ApiSpeaker {
                    display_name: Some("Alice".to_string()),
                    matched_calendar_invitee_email: None,
                },
                text: "Hello everyone".to_string(),
                timestamp: Some("00:00".to_string()),
            },
            ApiTranscriptEntry {
                speaker: ApiSpeaker {
                    display_name: Some("Bob".to_string()),
                    matched_calendar_invitee_email: None,
                },
                text: "Hi Alice".to_string(),
                timestamp: Some("00:02".to_string()),
            },
        ];

        let messages = FathomProvider::transcript_to_messages("meeting-1", &transcript);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].id, "meeting-1-0");
        assert_eq!(messages[1].parent_id, Some("meeting-1-0".to_string()));

        match &messages[0].content {
            MessageContent::Text { text } => {
                assert!(text.contains("Alice"));
                assert!(text.contains("Hello everyone"));
            }
            _ => panic!("Expected Text content"),
        }
    }

    #[test]
    fn test_build_summary_message() {
        let meeting = ApiMeeting {
            title: None,
            meeting_title: None,
            url: "https://fathom.video/calls/meeting-123".to_string(),
            share_url: None,
            created_at: chrono::Utc::now(),
            scheduled_start_time: None,
            scheduled_end_time: None,
            recording_start_time: None,
            recording_end_time: None,
            meeting_type: None,
            transcript_language: None,
            calendar_invitees: vec![],
            recorded_by: None,
            transcript: vec![],
            default_summary: Some(ApiSummary {
                template_name: Some("Default".to_string()),
                markdown_formatted: Some("## Summary\nGreat meeting!".to_string()),
            }),
            action_items: vec![
                ApiActionItem {
                    description: "Follow up on proposal".to_string(),
                    assignee: Some(ApiAssignee {
                        name: Some("Alice".to_string()),
                        email: None,
                    }),
                },
            ],
            crm_matches: None,
        };

        let summary_msg = FathomProvider::build_summary_message(&meeting).unwrap();
        assert_eq!(summary_msg.id, "meeting-123-summary");
        assert_eq!(summary_msg.role, Role::Assistant);

        match summary_msg.content {
            MessageContent::Text { text } => {
                assert!(text.contains("Great meeting"));
                assert!(text.contains("Action Items"));
                assert!(text.contains("Follow up on proposal"));
                assert!(text.contains("Alice"));
            }
            _ => panic!("Expected Text content"),
        }
    }

    #[test]
    fn test_truncate() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello world", 5), "hello...");
    }
}
