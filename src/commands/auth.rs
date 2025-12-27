use quaid_core::{providers::chatgpt::ChatGptProvider, Provider, Store};

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

            // TODO: Store token securely in keyring
            Ok(())
        }
        "claude" => {
            anyhow::bail!("Claude provider not yet implemented");
        }
        "gemini" => {
            anyhow::bail!("Gemini provider not yet implemented");
        }
        _ => {
            anyhow::bail!("Unknown provider: {}. Supported: chatgpt, claude, gemini", provider);
        }
    }
}
