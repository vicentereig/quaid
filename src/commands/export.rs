use quaid_core::Store;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub fn run(
    path: &Path,
    format: &str,
    provider: Option<&str>,
    store: &Store,
) -> anyhow::Result<()> {
    let accounts = store.list_accounts()?;

    if accounts.is_empty() {
        anyhow::bail!("No accounts configured. Use `quaid auth <provider>` first.");
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
        _ => anyhow::bail!("Unknown format: {}. Supported: jsonl, markdown, json", format),
    }

    println!("Exported to: {}", path.display());
    Ok(())
}

fn export_jsonl(
    path: &Path,
    conversations: &[(quaid_core::providers::Account, quaid_core::providers::Conversation, Vec<quaid_core::providers::Message>)],
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
    conversations: &[(quaid_core::providers::Account, quaid_core::providers::Conversation, Vec<quaid_core::providers::Message>)],
) -> anyhow::Result<()> {
    // Create directory if exporting multiple files
    if conversations.len() > 1 {
        std::fs::create_dir_all(path)?;

        for (_, conv, messages) in conversations {
            let filename = sanitize_filename(&conv.title);
            let file_path = path.join(format!("{}.md", filename));
            export_single_markdown(&file_path, conv, messages)?;
        }
    } else if let Some((_, conv, messages)) = conversations.first() {
        export_single_markdown(path, conv, messages)?;
    }

    Ok(())
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
    conversations: &[(quaid_core::providers::Account, quaid_core::providers::Conversation, Vec<quaid_core::providers::Message>)],
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
