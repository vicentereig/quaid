use quaid_core::Store;

pub fn run(provider: Option<&str>, _archived: bool, store: &Store) -> anyhow::Result<()> {
    let accounts = store.list_accounts()?;

    if accounts.is_empty() {
        println!("No accounts configured. Use `quaid auth <provider>` first.");
        return Ok(());
    }

    for account in accounts {
        // Filter by provider if specified
        if let Some(p) = provider {
            if account.provider.0 != p {
                continue;
            }
        }

        println!("\n{} ({})", account.provider, account.email);
        println!("{}", "-".repeat(60));

        let conversations = store.list_conversations(&account.id)?;

        if conversations.is_empty() {
            println!("  No conversations yet. Use `quaid pull {}` to sync.", account.provider);
            continue;
        }

        for conv in conversations.iter().take(20) {
            let date = conv.updated_at.format("%Y-%m-%d %H:%M");
            let model = conv.model.as_deref().unwrap_or("unknown");
            println!(
                "  {} | {:40} | {}",
                date,
                truncate(&conv.title, 40),
                model
            );
        }

        if conversations.len() > 20 {
            println!("  ... and {} more", conversations.len() - 20);
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
