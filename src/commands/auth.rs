use quaid_core::{
    providers::{
        chatgpt::ChatGptProvider, claude::ClaudeProvider, fathom::FathomProvider,
        granola::GranolaProvider,
    },
    Provider, Store,
};

pub async fn run(provider: &str, store: &Store) -> anyhow::Result<()> {
    match provider {
        "chatgpt" => {
            println!("Authenticating with ChatGPT...");
            println!("A browser window will open. Please log in to your ChatGPT account.");

            let mut provider = ChatGptProvider::new();
            let account = provider.authenticate().await?;

            // Save account to store
            store.save_account(&account)?;

            println!("\nAuthenticated as: {} ({})", account.email, account.id);
            println!("Account saved. You can now use `quaid pull chatgpt` to sync your conversations.");

            Ok(())
        }
        "claude" => {
            println!("Authenticating with Claude...");
            println!("A browser window will open. Please log in to your Claude account.");

            let mut provider = ClaudeProvider::new();
            let account = provider.authenticate().await?;

            // Save account to store
            store.save_account(&account)?;

            println!("\nAuthenticated as: {} ({})", account.email, account.id);
            println!("Account saved. You can now use `quaid pull claude` to sync your conversations.");

            Ok(())
        }
        "gemini" => {
            anyhow::bail!("Gemini provider not yet implemented");
        }
        "fathom" => {
            println!("Authenticating with Fathom...");

            let mut provider = FathomProvider::new();
            let account = provider.authenticate().await?;

            // Save account to store
            store.save_account(&account)?;

            println!("\nAuthenticated as: {} ({})", account.email, account.id);
            println!(
                "Account saved. You can now use `quaid pull fathom` to sync your meetings."
            );

            Ok(())
        }
        "granola" => {
            println!("Authenticating with Granola...");

            let mut provider = GranolaProvider::new();
            let account = provider.authenticate().await?;

            // Save account to store
            store.save_account(&account)?;

            println!("\nAuthenticated as: {} ({})", account.email, account.id);
            println!(
                "Account saved. You can now use `quaid pull granola` to sync your meeting notes."
            );

            Ok(())
        }
        _ => {
            anyhow::bail!(
                "Unknown provider: {}. Supported: chatgpt, claude, fathom, granola",
                provider
            );
        }
    }
}
