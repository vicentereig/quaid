use quaid_core::Store;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

pub fn run(path: &Path, format: &str, provider: Option<&str>, store: &Store) -> anyhow::Result<()> {
    let accounts = store.list_accounts()?;

    if accounts.is_empty() {
        anyhow::bail!("No accounts configured. Use `quaid <provider> auth` first.");
    }

    // Collect all conversations to export
    let mut all_conversations = Vec::new();

    for account in accounts {
        if let Some(p) = provider {
            if account.provider.0 != p {
                continue;
            }
        }

        let conversations = store.list_conversations(&account.id)?;
        for conv in conversations {
            let messages = store.get_messages(&conv.id)?;
            all_conversations.push((account.clone(), conv, messages));
        }
    }

    if all_conversations.is_empty() {
        anyhow::bail!("No conversations to export.");
    }

    println!(
        "Exporting {} conversations to {} format...",
        all_conversations.len(),
        format
    );

    match format {
        "jsonl" => export_jsonl(path, &all_conversations)?,
        "markdown" | "md" => export_markdown(path, &all_conversations)?,
        "json" => export_json(path, &all_conversations)?,
        _ => anyhow::bail!(
            "Unknown format: {}. Supported: jsonl, markdown, json",
            format
        ),
    }

    println!("Exported to: {}", path.display());
    Ok(())
}

fn export_jsonl(
    path: &Path,
    conversations: &[(
        quaid_core::providers::Account,
        quaid_core::providers::Conversation,
        Vec<quaid_core::providers::Message>,
    )],
) -> anyhow::Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    for (account, conv, messages) in conversations {
        let record = serde_json::json!({
            "account": {
                "id": account.id,
                "provider": account.provider.0,
                "email": account.email,
            },
            "conversation": {
                "id": conv.id,
                "title": conv.title,
                "created_at": conv.created_at.to_rfc3339(),
                "updated_at": conv.updated_at.to_rfc3339(),
                "model": conv.model,
            },
            "messages": messages,
        });

        serde_json::to_writer(&mut writer, &record)?;
        writeln!(writer)?;
    }

    Ok(())
}

fn export_markdown(
    path: &Path,
    conversations: &[(
        quaid_core::providers::Account,
        quaid_core::providers::Conversation,
        Vec<quaid_core::providers::Message>,
    )],
) -> anyhow::Result<()> {
    if conversations.len() > 1 {
        let output_dir = ensure_markdown_output_dir(path)?;

        for (_, conv, messages) in conversations {
            let filename = sanitize_filename(&conv.title);
            let file_path = output_dir.join(format!("{}.md", filename));
            export_single_markdown(&file_path, conv, messages)?;
        }
    } else if let Some((_, conv, messages)) = conversations.first() {
        export_single_markdown(path, conv, messages)?;
    }

    Ok(())
}

fn ensure_markdown_output_dir(path: &Path) -> anyhow::Result<PathBuf> {
    if path.exists() {
        if path.is_dir() {
            return Ok(path.to_path_buf());
        }

        anyhow::bail!(
            "Markdown export for multiple conversations requires a directory path, got file: {}",
            path.display()
        );
    }

    if path.extension().is_some() {
        anyhow::bail!(
            "Markdown export for multiple conversations requires a directory path, not a file-like path: {}",
            path.display()
        );
    }

    std::fs::create_dir_all(path)?;
    Ok(path.to_path_buf())
}

fn export_single_markdown(
    path: &Path,
    conv: &quaid_core::providers::Conversation,
    messages: &[quaid_core::providers::Message],
) -> anyhow::Result<()> {
    let mut content = String::new();

    // Frontmatter
    content.push_str("---\n");
    content.push_str(&format!("title: \"{}\"\n", conv.title.replace('"', "\\\"")));
    content.push_str(&format!("created: {}\n", conv.created_at.to_rfc3339()));
    content.push_str(&format!("updated: {}\n", conv.updated_at.to_rfc3339()));
    if let Some(model) = &conv.model {
        content.push_str(&format!("model: {}\n", model));
    }
    content.push_str("---\n\n");

    // Title
    content.push_str(&format!("# {}\n\n", conv.title));

    // Messages
    for msg in messages {
        let role = match msg.role {
            quaid_core::providers::Role::User => "You",
            quaid_core::providers::Role::Assistant => "Assistant",
            quaid_core::providers::Role::System => "System",
            quaid_core::providers::Role::Tool => "Tool",
        };

        content.push_str(&format!("## {}\n\n", role));

        match &msg.content {
            quaid_core::providers::MessageContent::Text { text } => {
                content.push_str(text);
                content.push_str("\n\n");
            }
            quaid_core::providers::MessageContent::Code { language, code } => {
                content.push_str(&format!("```{}\n{}\n```\n\n", language, code));
            }
            quaid_core::providers::MessageContent::Image { url, alt } => {
                let alt_text = alt.as_deref().unwrap_or("image");
                content.push_str(&format!("![{}]({})\n\n", alt_text, url));
            }
            quaid_core::providers::MessageContent::Audio { transcript, .. } => {
                if let Some(t) = transcript {
                    content.push_str(&format!("*[Audio transcript]* {}\n\n", t));
                } else {
                    content.push_str("*[Audio]*\n\n");
                }
            }
            quaid_core::providers::MessageContent::Mixed { parts } => {
                for part in parts {
                    match part {
                        quaid_core::providers::MessageContent::Text { text } => {
                            content.push_str(text);
                            content.push_str("\n");
                        }
                        quaid_core::providers::MessageContent::Image { url, alt } => {
                            let alt_text = alt.as_deref().unwrap_or("image");
                            content.push_str(&format!("![{}]({})\n", alt_text, url));
                        }
                        _ => {}
                    }
                }
                content.push('\n');
            }
        }
    }

    std::fs::write(path, content)?;
    Ok(())
}

fn export_json(
    path: &Path,
    conversations: &[(
        quaid_core::providers::Account,
        quaid_core::providers::Conversation,
        Vec<quaid_core::providers::Message>,
    )],
) -> anyhow::Result<()> {
    let data: Vec<_> = conversations
        .iter()
        .map(|(account, conv, messages)| {
            serde_json::json!({
                "account": {
                    "id": account.id,
                    "provider": account.provider.0,
                    "email": account.email,
                },
                "conversation": conv,
                "messages": messages,
            })
        })
        .collect();

    let json = serde_json::to_string_pretty(&data)?;
    std::fs::write(path, json)?;

    Ok(())
}

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => c,
        })
        .take(100)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{ensure_markdown_output_dir, export_markdown};
    use chrono::Utc;
    use quaid_core::providers::{Account, Conversation, Message, MessageContent, ProviderId, Role};
    use tempfile::tempdir;

    fn sample_account() -> Account {
        Account {
            id: "account-1".to_string(),
            provider: ProviderId("chatgpt".to_string()),
            email: "user@example.com".to_string(),
            name: Some("Test User".to_string()),
            avatar_url: None,
        }
    }

    fn sample_conversation(id: &str, title: &str) -> Conversation {
        Conversation {
            id: id.to_string(),
            provider_id: "chatgpt".to_string(),
            title: title.to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: Some("gpt-4.1".to_string()),
            project_id: None,
            project_name: None,
            is_archived: false,
        }
    }

    fn sample_message(conversation_id: &str, text: &str) -> Message {
        Message {
            id: format!("msg-{conversation_id}"),
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

    fn sample_export_data(id: &str, title: &str) -> (Account, Conversation, Vec<Message>) {
        let account = sample_account();
        let conversation = sample_conversation(id, title);
        let messages = vec![sample_message(id, "hello world")];
        (account, conversation, messages)
    }

    #[test]
    fn test_export_markdown_writes_single_conversation_to_file() {
        let dir = tempdir().unwrap();
        let output = dir.path().join("conversation.md");
        let conversations = vec![sample_export_data("conv-1", "Single Export")];

        export_markdown(&output, &conversations).unwrap();

        assert!(output.is_file());
        let content = std::fs::read_to_string(output).unwrap();
        assert!(content.contains("# Single Export"));
    }

    #[test]
    fn test_export_markdown_creates_directory_for_multiple_conversations() {
        let dir = tempdir().unwrap();
        let output = dir.path().join("markdown-export");
        let conversations = vec![
            sample_export_data("conv-1", "First Export"),
            sample_export_data("conv-2", "Second Export"),
        ];

        export_markdown(&output, &conversations).unwrap();

        assert!(output.is_dir());
        assert!(output.join("First Export.md").exists());
        assert!(output.join("Second Export.md").exists());
    }

    #[test]
    fn test_export_markdown_rejects_file_like_directory_path() {
        let dir = tempdir().unwrap();
        let output = dir.path().join("backup.md");

        let err = ensure_markdown_output_dir(&output).unwrap_err();

        assert!(err
            .to_string()
            .contains("requires a directory path, not a file-like path"));
    }

    #[test]
    fn test_export_markdown_rejects_existing_file_for_multiple_conversations() {
        let dir = tempdir().unwrap();
        let output = dir.path().join("backup");
        std::fs::write(&output, "existing file").unwrap();

        let err = ensure_markdown_output_dir(&output).unwrap_err();

        assert!(err
            .to_string()
            .contains("requires a directory path, got file"));
    }
}
