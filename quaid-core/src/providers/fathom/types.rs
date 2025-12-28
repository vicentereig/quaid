//! Fathom.video API types
//!
//! Based on the official Fathom API documentation:
//! https://developers.fathom.ai/quickstart

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

/// Deserialize null as empty vec
fn null_as_empty_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<Vec<T>>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

/// Response from GET /meetings endpoint
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiMeetingsResponse {
    #[serde(alias = "meetings")]
    pub items: Vec<ApiMeeting>,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// A Fathom meeting/recording
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiMeeting {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub meeting_title: Option<String>,
    pub url: String,
    #[serde(default)]
    pub share_url: Option<String>,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub scheduled_start_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub scheduled_end_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub recording_start_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub recording_end_time: Option<DateTime<Utc>>,
    #[serde(default)]
    pub meeting_type: Option<String>,
    #[serde(default)]
    pub transcript_language: Option<String>,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub calendar_invitees: Vec<ApiInvitee>,
    #[serde(default)]
    pub recorded_by: Option<ApiRecordedBy>,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub transcript: Vec<ApiTranscriptEntry>,
    #[serde(default)]
    pub default_summary: Option<ApiSummary>,
    #[serde(default, deserialize_with = "null_as_empty_vec")]
    pub action_items: Vec<ApiActionItem>,
    #[serde(default)]
    pub crm_matches: Option<ApiCrmMatches>,
}

impl ApiMeeting {
    /// Get meeting ID from URL (e.g., https://fathom.video/calls/123 -> 123)
    pub fn id(&self) -> String {
        self.url
            .rsplit('/')
            .next()
            .unwrap_or(&self.url)
            .to_string()
    }

    /// Get the best available title for this meeting
    pub fn display_title(&self) -> String {
        self.title
            .clone()
            .or_else(|| self.meeting_title.clone())
            .unwrap_or_else(|| format!("Meeting {}", self.id()))
    }
}

/// Calendar invitee/attendee
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiInvitee {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub is_external: Option<bool>,
}

/// Who recorded the meeting
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiRecordedBy {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub team: Option<String>,
}

/// A transcript entry (utterance)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiTranscriptEntry {
    pub speaker: ApiSpeaker,
    pub text: String,
    #[serde(default)]
    pub timestamp: Option<String>,
}

/// Speaker in a transcript entry
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiSpeaker {
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub matched_calendar_invitee_email: Option<String>,
}

/// Meeting summary
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiSummary {
    #[serde(default)]
    pub template_name: Option<String>,
    #[serde(default)]
    pub markdown_formatted: Option<String>,
}

/// Action item from meeting
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiActionItem {
    pub description: String,
    #[serde(default)]
    pub assignee: Option<ApiAssignee>,
}

/// Action item assignee
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiAssignee {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
}

/// CRM matches (contacts, companies, deals)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiCrmMatches {
    #[serde(default)]
    pub contacts: Vec<serde_json::Value>,
    #[serde(default)]
    pub companies: Vec<serde_json::Value>,
    #[serde(default)]
    pub deals: Vec<serde_json::Value>,
}

/// Response for teams endpoint
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiTeamsResponse {
    pub teams: Vec<ApiTeamInfo>,
}

/// Full team information
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ApiTeamInfo {
    pub id: String,
    pub name: String,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_meetings_response() {
        let json = r#"{
            "items": [
                {
                    "url": "https://fathom.video/calls/meeting-123",
                    "title": "Weekly Standup",
                    "created_at": "2025-01-15T10:00:00Z",
                    "calendar_invitees": [
                        {"name": "Alice", "email": "alice@example.com", "is_external": false}
                    ],
                    "transcript": [
                        {"speaker": {"display_name": "Alice"}, "text": "Good morning everyone", "timestamp": "00:00"}
                    ]
                }
            ],
            "limit": 100,
            "next_cursor": "abc123"
        }"#;

        let response: ApiMeetingsResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.items.len(), 1);
        assert_eq!(response.items[0].display_title(), "Weekly Standup");
        assert_eq!(response.items[0].id(), "meeting-123");
        assert_eq!(response.items[0].transcript.len(), 1);
        assert_eq!(response.next_cursor, Some("abc123".to_string()));
    }

    #[test]
    fn test_parse_meeting_minimal() {
        let json = r#"{
            "url": "https://fathom.video/calls/meeting-456",
            "created_at": "2025-01-15T10:00:00Z"
        }"#;

        let meeting: ApiMeeting = serde_json::from_str(json).unwrap();
        assert_eq!(meeting.id(), "meeting-456");
        assert!(meeting.title.is_none());
        assert!(meeting.transcript.is_empty());
    }

    #[test]
    fn test_display_title_fallback() {
        let meeting = ApiMeeting {
            title: None,
            meeting_title: Some("Calendar Title".to_string()),
            url: "https://fathom.video/calls/test-123".to_string(),
            share_url: None,
            created_at: Utc::now(),
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

        assert_eq!(meeting.display_title(), "Calendar Title");
        assert_eq!(meeting.id(), "test-123");
    }

    #[test]
    fn test_parse_transcript_entry() {
        let json = r#"{
            "speaker": {"display_name": "Bob", "matched_calendar_invitee_email": "bob@example.com"},
            "text": "Let's discuss the roadmap",
            "timestamp": "00:05"
        }"#;

        let entry: ApiTranscriptEntry = serde_json::from_str(json).unwrap();
        assert_eq!(entry.speaker.display_name, Some("Bob".to_string()));
        assert_eq!(entry.text, "Let's discuss the roadmap");
        assert_eq!(entry.timestamp, Some("00:05".to_string()));
    }

    #[test]
    fn test_parse_summary() {
        let summary = ApiSummary {
            template_name: Some("Default".to_string()),
            markdown_formatted: Some("Summary content here".to_string()),
        };

        assert_eq!(summary.template_name, Some("Default".to_string()));
        assert!(summary.markdown_formatted.as_ref().unwrap().contains("Summary"));
    }

    #[test]
    fn test_parse_action_item() {
        let json = r#"{
            "description": "Review the proposal by Friday",
            "assignee": {
                "name": "Charlie",
                "email": "charlie@example.com"
            }
        }"#;

        let item: ApiActionItem = serde_json::from_str(json).unwrap();
        assert_eq!(item.description, "Review the proposal by Friday");
        assert_eq!(item.assignee.as_ref().unwrap().name, Some("Charlie".to_string()));
    }
}
