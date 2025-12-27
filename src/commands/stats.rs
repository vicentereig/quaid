use quaid_core::Store;

pub fn run(store: &Store) -> anyhow::Result<()> {
    let stats = store.stats()?;

    println!("Quaid Statistics");
    println!("================");
    println!();
    println!("Accounts:      {}", stats.accounts);
    println!("Conversations: {}", stats.conversations);
    println!("Messages:      {}", stats.messages);
    println!("Attachments:   {}", stats.attachments);

    // Show per-account breakdown
    let accounts = store.list_accounts()?;
    if !accounts.is_empty() {
        println!();
        println!("By Account:");
        println!("-----------");

        for account in accounts {
            let convs = store.list_conversations(&account.id)?;
            let msg_count: usize = convs
                .iter()
                .map(|c| store.get_messages(&c.id).map(|m| m.len()).unwrap_or(0))
                .sum();

            println!(
                "  {} ({}): {} conversations, {} messages",
                account.provider,
                account.email,
                convs.len(),
                msg_count
            );
        }
    }

    Ok(())
}
