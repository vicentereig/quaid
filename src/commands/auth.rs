use quaid_core::{
    providers::{chatgpt::ChatGptProvider, claude::ClaudeProvider},
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
        _ => {
            anyhow::bail!("Unknown provider: {}. Supported: chatgpt, claude, gemini", provider);
        }
    }
}
