use quaid_core::{
    providers::{chatgpt::ChatGptProvider, claude::ClaudeProvider},
    Provider, Store,
};
use std::path::Path;

pub async fn run(
    provider: Option<&str>,
    all: bool,
    store: &Store,
    data_dir: &Path,
) -> anyhow::Result<()> {
    if all {
        // Pull from all configured providers
        let accounts = store.list_accounts()?;
        if accounts.is_empty() {
            println!("No accounts configured. Use `quaid auth <provider>` first.");
            return Ok(());
        }

        for account in accounts {
            println!("Pulling from {} ({})...", account.provider, account.email);
            pull_provider(&account.provider.0, &account.id, store, data_dir).await?;
        }
    } else if let Some(provider) = provider {
        // Pull from specific provider
        let accounts: Vec<_> = store
            .list_accounts()?
            .into_iter()
            .filter(|a| a.provider.0 == provider)
            .collect();

        if accounts.is_empty() {
            anyhow::bail!(
                "No {} account configured. Use `quaid auth {}` first.",
                provider,
                provider
            );
        }

        for account in accounts {
            pull_provider(provider, &account.id, store, data_dir).await?;
        }
    } else {
        println!("Usage: quaid pull <provider> or quaid pull --all");
        println!("Providers: chatgpt, claude, gemini");
    }

    Ok(())
}

async fn pull_provider(
    provider: &str,
    account_id: &str,
    store: &Store,
    data_dir: &Path,
) -> anyhow::Result<()> {
    match provider {
        "chatgpt" => pull_chatgpt(account_id, store, data_dir).await,
        "claude" => pull_claude(account_id, store, data_dir).await,
        "gemini" => {
            println!("Gemini provider not yet implemented");
            Ok(())
        }
        _ => {
            println!("Unknown provider: {}", provider);
            Ok(())
        }
    }
}

async fn pull_chatgpt(account_id: &str, store: &Store, data_dir: &Path) -> anyhow::Result<()> {
    // TODO: Retrieve token from keyring
    // For now, we need to re-authenticate
    println!("Fetching conversations from ChatGPT...");

    let provider = ChatGptProvider::new();

    // Check if we need to authenticate
    if !provider.is_authenticated().await {
        println!("Not authenticated. Please run `quaid auth chatgpt` first.");
        return Ok(());
    }

    // Fetch all conversations
    let conversations = provider.conversations().await?;
    println!("Found {} conversations", conversations.len());

    let mut synced = 0;
    let mut failed = 0;

    for (i, conv) in conversations.iter().enumerate() {
        print!("\r[{}/{}] Syncing: {}...", i + 1, conversations.len(), truncate(&conv.title, 40));

        match provider.conversation(&conv.id).await {
            Ok((full_conv, messages)) => {
                // Save conversation
                store.save_conversation(account_id, &full_conv)?;

                // Save messages
                for mut msg in messages {
                    msg.conversation_id = conv.id.clone();
                    store.save_message(&msg)?;
                }

                synced += 1;
            }
            Err(e) => {
                eprintln!("\nError syncing {}: {}", conv.id, e);
                failed += 1;
            }
        }

        // Rate limiting - be nice to the API
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    println!("\n\nSync complete: {} synced, {} failed", synced, failed);

    // Download pending attachments
    let pending = store.get_pending_attachments()?;
    if !pending.is_empty() {
        println!("\nDownloading {} attachments...", pending.len());

        let attachments_dir = data_dir.join("attachments").join(account_id);
        std::fs::create_dir_all(&attachments_dir)?;

        for attachment in pending {
            let path = attachments_dir.join(&attachment.filename);
            match provider.download_attachment(&attachment, &path).await {
                Ok(_) => {
                    store.mark_attachment_downloaded(&attachment.id, path.to_str().unwrap_or(""))?;
                    println!("  Downloaded: {}", attachment.filename);
                }
                Err(e) => {
                    eprintln!("  Failed to download {}: {}", attachment.filename, e);
                }
            }
        }
    }

    Ok(())
}

async fn pull_claude(account_id: &str, store: &Store, data_dir: &Path) -> anyhow::Result<()> {
    println!("Fetching conversations from Claude...");

    let provider = ClaudeProvider::new();

    // Check if we need to authenticate
    if !provider.is_authenticated().await {
        println!("Not authenticated. Please run `quaid auth claude` first.");
        return Ok(());
    }

    // Fetch all conversations
    let conversations = provider.conversations().await?;
    println!("Found {} conversations", conversations.len());

    let mut synced = 0;
    let mut failed = 0;

    for (i, conv) in conversations.iter().enumerate() {
        print!(
            "\r[{}/{}] Syncing: {}...",
            i + 1,
            conversations.len(),
            truncate(&conv.title, 40)
        );

        match provider.conversation(&conv.id).await {
            Ok((full_conv, messages)) => {
                // Save conversation
                store.save_conversation(account_id, &full_conv)?;

                // Save messages
                for msg in messages {
                    store.save_message(&msg)?;
                }

                synced += 1;
            }
            Err(e) => {
                eprintln!("\nError syncing {}: {}", conv.id, e);
                failed += 1;
            }
        }

        // Rate limiting - be nice to the API
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    println!("\n\nSync complete: {} synced, {} failed", synced, failed);

    // Download pending attachments
    let pending = store.get_pending_attachments()?;
    if !pending.is_empty() {
        println!("\nDownloading {} attachments...", pending.len());

        let attachments_dir = data_dir.join("attachments").join(account_id);
        std::fs::create_dir_all(&attachments_dir)?;

        for attachment in pending {
            let path = attachments_dir.join(&attachment.filename);
            match provider.download_attachment(&attachment, &path).await {
                Ok(_) => {
                    store.mark_attachment_downloaded(&attachment.id, path.to_str().unwrap_or(""))?;
                    println!("  Downloaded: {}", attachment.filename);
                }
                Err(e) => {
                    eprintln!("  Failed to download {}: {}", attachment.filename, e);
                }
            }
        }
    }

    Ok(())
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
