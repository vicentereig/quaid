//! Parquet storage for conversations
//!
//! Stores each conversation as a separate parquet file with its messages.

use super::{ParquetStorageConfig, Result, StorageError};
use crate::providers::{Conversation, Message, MessageContent, Role};
use arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::sync::Arc;

/// Parquet-based conversation storage
///
/// Stores each conversation as a separate parquet file:
/// - conversations/{provider}/{conversation_id}.parquet
pub struct ParquetStore {
    config: ParquetStorageConfig,
}

impl ParquetStore {
    pub fn new(config: ParquetStorageConfig) -> Self {
        Self { config }
    }

    /// Combined schema for conversation + messages in a single file
    fn combined_schema() -> Schema {
        Schema::new(vec![
            // Conversation fields (prefixed)
            Field::new("conv_id", DataType::Utf8, false),
            Field::new("conv_provider_id", DataType::Utf8, false),
            Field::new("conv_title", DataType::Utf8, false),
            Field::new(
                "conv_created_at",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "conv_updated_at",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new("conv_model", DataType::Utf8, true),
            Field::new("conv_project_id", DataType::Utf8, true),
            Field::new("conv_project_name", DataType::Utf8, true),
            Field::new("conv_is_archived", DataType::Boolean, false),
            // Message fields
            Field::new("msg_id", DataType::Utf8, false),
            Field::new("msg_parent_id", DataType::Utf8, true),
            Field::new("msg_role", DataType::Utf8, false),
            Field::new("msg_content_type", DataType::Utf8, false),
            Field::new("msg_content_json", DataType::Utf8, false),
            Field::new(
                "msg_created_at",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("msg_model", DataType::Utf8, true),
        ])
    }

    /// Write a conversation with its messages to a parquet file
    pub fn write_conversation(
        &self,
        _account_id: &str,
        conv: &Conversation,
        messages: &[Message],
    ) -> Result<std::path::PathBuf> {
        let path = self.config.conversation_path(&conv.provider_id, &conv.id);

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = File::create(&path)?;
        let schema = Arc::new(Self::combined_schema());

        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| StorageError::Parquet(e.to_string()))?;

        // Build arrays for each message row (denormalized with conversation data)
        let num_rows = messages.len().max(1); // At least one row for conversation metadata

        let conv_ids: Vec<&str> = vec![&conv.id; num_rows];
        let conv_provider_ids: Vec<&str> = vec![&conv.provider_id; num_rows];
        let conv_titles: Vec<&str> = vec![&conv.title; num_rows];
        let conv_created_ats: Vec<i64> = vec![conv.created_at.timestamp_millis(); num_rows];
        let conv_updated_ats: Vec<i64> = vec![conv.updated_at.timestamp_millis(); num_rows];
        let conv_models: Vec<Option<&str>> = vec![conv.model.as_deref(); num_rows];
        let conv_project_ids: Vec<Option<&str>> = vec![conv.project_id.as_deref(); num_rows];
        let conv_project_names: Vec<Option<&str>> = vec![conv.project_name.as_deref(); num_rows];
        let conv_is_archiveds: Vec<bool> = vec![conv.is_archived; num_rows];

        // Message data
        let (msg_ids, msg_parent_ids, msg_roles, msg_content_types, msg_content_jsons, msg_created_ats, msg_models): (
            Vec<String>,
            Vec<Option<String>>,
            Vec<String>,
            Vec<String>,
            Vec<String>,
            Vec<Option<i64>>,
            Vec<Option<String>>,
        ) = if messages.is_empty() {
            // No messages - create a placeholder row
            (
                vec!["".to_string()],
                vec![None],
                vec!["".to_string()],
                vec!["".to_string()],
                vec!["".to_string()],
                vec![None],
                vec![None],
            )
        } else {
            messages
                .iter()
                .map(|m| {
                    let content_type = match &m.content {
                        MessageContent::Text { .. } => "text",
                        MessageContent::Code { .. } => "code",
                        MessageContent::Image { .. } => "image",
                        MessageContent::Audio { .. } => "audio",
                        MessageContent::Mixed { .. } => "mixed",
                    };
                    let content_json = serde_json::to_string(&m.content).unwrap_or_default();
                    let role = match m.role {
                        Role::User => "user",
                        Role::Assistant => "assistant",
                        Role::System => "system",
                        Role::Tool => "tool",
                    };

                    (
                        m.id.clone(),
                        m.parent_id.clone(),
                        role.to_string(),
                        content_type.to_string(),
                        content_json,
                        m.created_at.map(|dt| dt.timestamp_millis()),
                        m.model.clone(),
                    )
                })
                .fold(
                    (vec![], vec![], vec![], vec![], vec![], vec![], vec![]),
                    |mut acc, (id, parent, role, ct, cj, ca, model)| {
                        acc.0.push(id);
                        acc.1.push(parent);
                        acc.2.push(role);
                        acc.3.push(ct);
                        acc.4.push(cj);
                        acc.5.push(ca);
                        acc.6.push(model);
                        acc
                    },
                )
        };

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(conv_ids)) as ArrayRef,
                Arc::new(StringArray::from(conv_provider_ids)) as ArrayRef,
                Arc::new(StringArray::from(conv_titles)) as ArrayRef,
                Arc::new(
                    TimestampMillisecondArray::from(conv_created_ats)
                        .with_timezone("UTC"),
                ) as ArrayRef,
                Arc::new(
                    TimestampMillisecondArray::from(conv_updated_ats)
                        .with_timezone("UTC"),
                ) as ArrayRef,
                Arc::new(StringArray::from(conv_models)) as ArrayRef,
                Arc::new(StringArray::from(conv_project_ids)) as ArrayRef,
                Arc::new(StringArray::from(conv_project_names)) as ArrayRef,
                Arc::new(BooleanArray::from(conv_is_archiveds)) as ArrayRef,
                Arc::new(StringArray::from(msg_ids)) as ArrayRef,
                Arc::new(StringArray::from(msg_parent_ids)) as ArrayRef,
                Arc::new(StringArray::from(msg_roles)) as ArrayRef,
                Arc::new(StringArray::from(msg_content_types)) as ArrayRef,
                Arc::new(StringArray::from(msg_content_jsons)) as ArrayRef,
                Arc::new(
                    TimestampMillisecondArray::from(msg_created_ats)
                        .with_timezone("UTC"),
                ) as ArrayRef,
                Arc::new(StringArray::from(msg_models)) as ArrayRef,
            ],
        )?;

        writer
            .write(&batch)
            .map_err(|e| StorageError::Parquet(e.to_string()))?;
        writer
            .close()
            .map_err(|e| StorageError::Parquet(e.to_string()))?;

        Ok(path)
    }

    /// Read a conversation and its messages from a parquet file
    pub fn read_conversation(
        &self,
        provider: &str,
        conversation_id: &str,
    ) -> Result<Option<(Conversation, Vec<Message>)>> {
        let path = self.config.conversation_path(provider, conversation_id);

        if !path.exists() {
            return Ok(None);
        }

        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| StorageError::Parquet(e.to_string()))?;
        let mut reader = builder
            .build()
            .map_err(|e| StorageError::Parquet(e.to_string()))?;

        let mut conversation: Option<Conversation> = None;
        let mut messages: Vec<Message> = Vec::new();

        while let Some(batch_result) = reader.next() {
            let batch = batch_result?;

            // Extract conversation from first row
            if conversation.is_none() {
                let conv_id = batch
                    .column_by_name("conv_id")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .and_then(|a| a.value(0).to_string().into());

                let conv_provider_id = batch
                    .column_by_name("conv_provider_id")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .map(|a| a.value(0).to_string())
                    .unwrap_or_default();

                let conv_title = batch
                    .column_by_name("conv_title")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .map(|a| a.value(0).to_string())
                    .unwrap_or_default();

                let conv_created_at = batch
                    .column_by_name("conv_created_at")
                    .and_then(|c| c.as_any().downcast_ref::<TimestampMillisecondArray>())
                    .and_then(|a| {
                        DateTime::from_timestamp_millis(a.value(0))
                    })
                    .unwrap_or_else(Utc::now);

                let conv_updated_at = batch
                    .column_by_name("conv_updated_at")
                    .and_then(|c| c.as_any().downcast_ref::<TimestampMillisecondArray>())
                    .and_then(|a| {
                        DateTime::from_timestamp_millis(a.value(0))
                    })
                    .unwrap_or_else(Utc::now);

                let conv_model = batch
                    .column_by_name("conv_model")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .and_then(|a| {
                        if a.is_null(0) {
                            None
                        } else {
                            Some(a.value(0).to_string())
                        }
                    });

                let conv_project_id = batch
                    .column_by_name("conv_project_id")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .and_then(|a| {
                        if a.is_null(0) {
                            None
                        } else {
                            Some(a.value(0).to_string())
                        }
                    });

                let conv_project_name = batch
                    .column_by_name("conv_project_name")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .and_then(|a| {
                        if a.is_null(0) {
                            None
                        } else {
                            Some(a.value(0).to_string())
                        }
                    });

                let conv_is_archived = batch
                    .column_by_name("conv_is_archived")
                    .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
                    .map(|a| a.value(0))
                    .unwrap_or(false);

                conversation = Some(Conversation {
                    id: conv_id.unwrap_or_default(),
                    provider_id: conv_provider_id,
                    title: conv_title,
                    created_at: conv_created_at,
                    updated_at: conv_updated_at,
                    model: conv_model,
                    project_id: conv_project_id,
                    project_name: conv_project_name,
                    is_archived: conv_is_archived,
                });
            }

            // Extract messages from all rows
            let msg_ids = batch
                .column_by_name("msg_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let msg_parent_ids = batch
                .column_by_name("msg_parent_id")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let msg_roles = batch
                .column_by_name("msg_role")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let msg_content_jsons = batch
                .column_by_name("msg_content_json")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());
            let msg_created_ats = batch
                .column_by_name("msg_created_at")
                .and_then(|c| c.as_any().downcast_ref::<TimestampMillisecondArray>());
            let msg_models = batch
                .column_by_name("msg_model")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            if let (Some(ids), Some(roles), Some(content_jsons)) =
                (msg_ids, msg_roles, msg_content_jsons)
            {
                for i in 0..batch.num_rows() {
                    let msg_id = ids.value(i);
                    // Skip placeholder rows (empty message id)
                    if msg_id.is_empty() {
                        continue;
                    }

                    let role = match roles.value(i) {
                        "user" => Role::User,
                        "assistant" => Role::Assistant,
                        "system" => Role::System,
                        "tool" => Role::Tool,
                        _ => Role::User,
                    };

                    let content: MessageContent =
                        serde_json::from_str(content_jsons.value(i)).unwrap_or(
                            MessageContent::Text {
                                text: content_jsons.value(i).to_string(),
                            },
                        );

                    let parent_id = msg_parent_ids.and_then(|a| {
                        if a.is_null(i) {
                            None
                        } else {
                            Some(a.value(i).to_string())
                        }
                    });

                    let created_at = msg_created_ats.and_then(|a| {
                        if a.is_null(i) {
                            None
                        } else {
                            DateTime::from_timestamp_millis(a.value(i))
                        }
                    });

                    let model = msg_models.and_then(|a| {
                        if a.is_null(i) {
                            None
                        } else {
                            Some(a.value(i).to_string())
                        }
                    });

                    messages.push(Message {
                        id: msg_id.to_string(),
                        conversation_id: conversation_id.to_string(),
                        parent_id,
                        role,
                        content,
                        created_at,
                        model,
                    });
                }
            }
        }

        Ok(conversation.map(|c| (c, messages)))
    }

    /// List all conversation IDs for a provider
    pub fn list_conversation_ids(&self, provider: &str) -> Result<Vec<String>> {
        let dir = self.config.base_dir.join("conversations").join(provider);

        if !dir.exists() {
            return Ok(vec![]);
        }

        let mut ids = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map(|e| e == "parquet").unwrap_or(false) {
                if let Some(stem) = path.file_stem() {
                    ids.push(stem.to_string_lossy().to_string());
                }
            }
        }

        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_conversation() -> Conversation {
        Conversation {
            id: "conv-123".to_string(),
            provider_id: "chatgpt".to_string(),
            title: "Test Conversation".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: Some("gpt-4".to_string()),
            project_id: None,
            project_name: None,
            is_archived: false,
        }
    }

    fn create_test_message(conversation_id: &str, id: &str, text: &str) -> Message {
        Message {
            id: id.to_string(),
            conversation_id: conversation_id.to_string(),
            parent_id: None,
            role: Role::User,
            content: MessageContent::Text {
                text: text.to_string(),
            },
            created_at: Some(Utc::now()),
            model: None,
        }
    }

    #[test]
    fn test_write_conversation_to_parquet() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = ParquetStore::new(config.clone());

        let conv = create_test_conversation();
        let messages = vec![
            create_test_message(&conv.id, "msg-1", "Hello!"),
            create_test_message(&conv.id, "msg-2", "How are you?"),
        ];

        let path = store
            .write_conversation("user-123", &conv, &messages)
            .unwrap();

        assert!(path.exists());
        assert_eq!(
            path,
            config.conversation_path("chatgpt", "conv-123")
        );
    }

    #[test]
    fn test_read_written_parquet() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = ParquetStore::new(config);

        let conv = create_test_conversation();
        let messages = vec![
            create_test_message(&conv.id, "msg-1", "Hello!"),
            create_test_message(&conv.id, "msg-2", "How are you?"),
        ];

        store
            .write_conversation("user-123", &conv, &messages)
            .unwrap();

        let result = store.read_conversation("chatgpt", "conv-123").unwrap();
        assert!(result.is_some());

        let (read_conv, read_messages) = result.unwrap();
        assert_eq!(read_conv.id, conv.id);
        assert_eq!(read_conv.title, conv.title);
        assert_eq!(read_messages.len(), 2);
        assert_eq!(read_messages[0].id, "msg-1");
        assert_eq!(read_messages[1].id, "msg-2");
    }

    #[test]
    fn test_parquet_writer_creates_directories() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = ParquetStore::new(config.clone());

        let conv = create_test_conversation();

        store.write_conversation("user-123", &conv, &[]).unwrap();

        let expected_dir = dir.path().join("conversations").join("chatgpt");
        assert!(expected_dir.exists());
    }

    #[test]
    fn test_empty_conversation_handling() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = ParquetStore::new(config);

        let conv = create_test_conversation();

        // Write conversation with no messages
        store.write_conversation("user-123", &conv, &[]).unwrap();

        // Read it back
        let result = store.read_conversation("chatgpt", "conv-123").unwrap();
        assert!(result.is_some());

        let (read_conv, read_messages) = result.unwrap();
        assert_eq!(read_conv.id, conv.id);
        assert!(read_messages.is_empty());
    }

    #[test]
    fn test_read_nonexistent_conversation() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = ParquetStore::new(config);

        let result = store.read_conversation("chatgpt", "nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_list_conversation_ids() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = ParquetStore::new(config);

        // Write multiple conversations
        let mut conv1 = create_test_conversation();
        conv1.id = "conv-1".to_string();
        let mut conv2 = create_test_conversation();
        conv2.id = "conv-2".to_string();
        let mut conv3 = create_test_conversation();
        conv3.id = "conv-3".to_string();

        store.write_conversation("user-123", &conv1, &[]).unwrap();
        store.write_conversation("user-123", &conv2, &[]).unwrap();
        store.write_conversation("user-123", &conv3, &[]).unwrap();

        let ids = store.list_conversation_ids("chatgpt").unwrap();
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&"conv-1".to_string()));
        assert!(ids.contains(&"conv-2".to_string()));
        assert!(ids.contains(&"conv-3".to_string()));
    }

    #[test]
    fn test_list_conversation_ids_empty_provider() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = ParquetStore::new(config);

        let ids = store.list_conversation_ids("nonexistent_provider").unwrap();
        assert!(ids.is_empty());
    }

    #[test]
    fn test_message_content_types() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = ParquetStore::new(config);

        let conv = create_test_conversation();
        let messages = vec![
            Message {
                id: "msg-1".to_string(),
                conversation_id: conv.id.clone(),
                parent_id: None,
                role: Role::User,
                content: MessageContent::Text {
                    text: "Hello".to_string(),
                },
                created_at: Some(Utc::now()),
                model: None,
            },
            Message {
                id: "msg-2".to_string(),
                conversation_id: conv.id.clone(),
                parent_id: Some("msg-1".to_string()),
                role: Role::Assistant,
                content: MessageContent::Code {
                    language: "rust".to_string(),
                    code: "fn main() {}".to_string(),
                },
                created_at: Some(Utc::now()),
                model: Some("gpt-4".to_string()),
            },
        ];

        store
            .write_conversation("user-123", &conv, &messages)
            .unwrap();

        let result = store.read_conversation("chatgpt", "conv-123").unwrap();
        let (_, read_messages) = result.unwrap();

        assert_eq!(read_messages.len(), 2);

        // Check first message
        assert!(matches!(
            read_messages[0].content,
            MessageContent::Text { .. }
        ));

        // Check second message
        assert!(matches!(
            read_messages[1].content,
            MessageContent::Code { .. }
        ));
        assert_eq!(read_messages[1].parent_id, Some("msg-1".to_string()));
        assert_eq!(read_messages[1].role, Role::Assistant);
    }

    #[test]
    fn test_overwrite_existing_conversation() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = ParquetStore::new(config);

        let conv = create_test_conversation();
        let messages1 = vec![create_test_message(&conv.id, "msg-1", "First version")];
        let messages2 = vec![
            create_test_message(&conv.id, "msg-1", "Updated first"),
            create_test_message(&conv.id, "msg-2", "New second"),
        ];

        // Write first version
        store
            .write_conversation("user-123", &conv, &messages1)
            .unwrap();

        // Overwrite with second version
        store
            .write_conversation("user-123", &conv, &messages2)
            .unwrap();

        // Read and verify
        let result = store.read_conversation("chatgpt", "conv-123").unwrap();
        let (_, read_messages) = result.unwrap();

        assert_eq!(read_messages.len(), 2);
    }
}
