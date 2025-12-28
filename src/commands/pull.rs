use chrono::{DateTime, Utc};
use quaid_core::{
    providers::{
        chatgpt::ChatGptProvider, claude::ClaudeProvider, fathom::FathomProvider,
        granola::GranolaProvider,
    },
    Provider, Store,
};
use std::path::Path;

pub async fn run(
    provider: Option<&str>,
    new_only: bool,
    store: &Store,
    data_dir: &Path,
) -> anyhow::Result<()> {
    if let Some(provider) = provider {
        // Pull from specific provider
        let accounts: Vec<_> = store
            .list_accounts()?
            .into_iter()
            .filter(|a| a.provider.0 == provider)
            .collect();

        if accounts.is_empty() {
            anyhow::bail!(
                "No {} account configured. Use `quaid {} auth` first.",
                provider,
                provider
            );
        }

        for account in accounts {
            pull_provider(provider, &account.id, new_only, store, data_dir).await?;
        }
    } else {
        // Pull from all configured providers
        pull_all(new_only, store, data_dir).await?;
    }

    Ok(())
}

/// Check if we should skip this conversation based on updated_at
fn should_skip(
    conv_id: &str,
    remote_updated_at: DateTime<Utc>,
    new_only: bool,
    store: &Store,
) -> bool {
    if !new_only {
        return false;
    }

    if let Ok(Some(local_updated_at)) = store.get_conversation_updated_at(conv_id) {
        // Skip if remote hasn't been updated since our last sync
        remote_updated_at <= local_updated_at
    } else {
        // New conversation, don't skip
        false
    }
}

/// Pull from all configured providers
async fn pull_all(new_only: bool, store: &Store, data_dir: &Path) -> anyhow::Result<()> {
    let accounts = store.list_accounts()?;
    if accounts.is_empty() {
        println!("No accounts configured. Use `quaid <provider> auth` first.");
        println!("Providers: chatgpt, claude, fathom, granola");
        return Ok(());
    }

    println!("Pulling from {} providers...\n", accounts.len());

    for account in &accounts {
        println!("\n--- {} ({}) ---", account.provider.0, account.email);
        if let Err(e) =
            pull_provider(&account.provider.0, &account.id, new_only, store, data_dir).await
        {
            eprintln!("Error: {}", e);
        }
    }

    println!("\nPull complete. Run `quaid stats` to see totals.");
    Ok(())
}

async fn pull_provider(
    provider: &str,
    account_id: &str,
    new_only: bool,
    store: &Store,
    data_dir: &Path,
) -> anyhow::Result<()> {
    match provider {
        "chatgpt" => pull_chatgpt(account_id, new_only, store, data_dir).await,
        "claude" => pull_claude(account_id, new_only, store, data_dir).await,
        "fathom" => pull_fathom(account_id, new_only, store).await,
        "granola" => pull_granola(account_id, new_only, store).await,
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

async fn pull_chatgpt(
    account_id: &str,
    new_only: bool,
    store: &Store,
    data_dir: &Path,
) -> anyhow::Result<()> {
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
    let mut skipped = 0;
    let mut failed = 0;

    for (i, conv) in conversations.iter().enumerate() {
        // Check if we should skip this conversation
        if should_skip(&conv.id, conv.updated_at, new_only, store) {
            skipped += 1;
            continue;
        }

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

    if skipped > 0 {
        println!(
            "\n\nSync complete: {} synced, {} skipped (unchanged), {} failed",
            synced, skipped, failed
        );
    } else {
        println!("\n\nSync complete: {} synced, {} failed", synced, failed);
    }

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

async fn pull_claude(
    account_id: &str,
    new_only: bool,
    store: &Store,
    data_dir: &Path,
) -> anyhow::Result<()> {
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
    let mut skipped = 0;
    let mut failed = 0;

    for (i, conv) in conversations.iter().enumerate() {
        // Check if we should skip this conversation
        if should_skip(&conv.id, conv.updated_at, new_only, store) {
            skipped += 1;
            continue;
        }

        print!(
            "\r[{}/{}] Syncing: {}...",
            i + 1,
            conversations.len(),
            truncate(&conv.title, 40)
        );

        match provider.conversation_with_attachments(&conv.id).await {
            Ok((full_conv, messages, attachments)) => {
                // Save conversation
                store.save_conversation(account_id, &full_conv)?;

                // Save messages
                for msg in messages {
                    store.save_message(&msg)?;
                }

                // Save attachments for later download
                for attachment in attachments {
                    store.save_attachment(&attachment)?;
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

    if skipped > 0 {
        println!(
            "\n\nSync complete: {} synced, {} skipped (unchanged), {} failed",
            synced, skipped, failed
        );
    } else {
        println!("\n\nSync complete: {} synced, {} failed", synced, failed);
    }

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

async fn pull_fathom(account_id: &str, new_only: bool, store: &Store) -> anyhow::Result<()> {
    println!("Fetching meetings from Fathom (with transcripts)...");

    let provider = FathomProvider::new();

    if !provider.is_authenticated().await {
        println!("Not authenticated. Please run `quaid auth fathom` first.");
        return Ok(());
    }

    // Fetch all meetings with transcripts in one batch (more efficient)
    let meetings = provider.fetch_all_meetings_with_transcripts().await?;
    println!("Found {} meetings", meetings.len());

    let mut synced = 0;
    let mut skipped = 0;

    for (i, meeting) in meetings.iter().enumerate() {
        let (conv, messages) = provider.meeting_to_data(meeting);

        // Check if we should skip this conversation
        if should_skip(&conv.id, conv.updated_at, new_only, store) {
            skipped += 1;
            continue;
        }

        print!(
            "\r[{}/{}] Syncing: {}...",
            i + 1,
            meetings.len(),
            truncate(&meeting.display_title(), 40)
        );

        store.save_conversation(account_id, &conv)?;
        for msg in messages {
            store.save_message(&msg)?;
        }
        synced += 1;
    }

    if skipped > 0 {
        println!(
            "\n\nSync complete: {} synced, {} skipped (unchanged)",
            synced, skipped
        );
    } else {
        println!("\n\nSync complete: {} meetings synced", synced);
    }
    Ok(())
}

async fn pull_granola(account_id: &str, new_only: bool, store: &Store) -> anyhow::Result<()> {
    println!("Fetching meeting notes from Granola...");

    let provider = GranolaProvider::new();

    if !provider.is_authenticated().await {
        println!("Not authenticated. Please run `quaid auth granola` first.");
        println!("(Make sure you're logged into the Granola desktop app)");
        return Ok(());
    }

    let conversations = provider.conversations().await?;
    println!("Found {} documents", conversations.len());

    let mut synced = 0;
    let mut skipped = 0;
    let mut failed = 0;

    for (i, conv) in conversations.iter().enumerate() {
        // Check if we should skip this conversation
        if should_skip(&conv.id, conv.updated_at, new_only, store) {
            skipped += 1;
            continue;
        }

        print!(
            "\r[{}/{}] Syncing: {}...",
            i + 1,
            conversations.len(),
            truncate(&conv.title, 40)
        );

        match provider.conversation(&conv.id).await {
            Ok((full_conv, messages)) => {
                store.save_conversation(account_id, &full_conv)?;
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

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    if skipped > 0 {
        println!(
            "\n\nSync complete: {} synced, {} skipped (unchanged), {} failed",
            synced, skipped, failed
        );
    } else {
        println!("\n\nSync complete: {} synced, {} failed", synced, failed);
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
