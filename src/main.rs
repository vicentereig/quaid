mod commands;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "quaid")]
#[command(about = "Get your chats back", long_about = None)]
#[command(version)]
struct Cli {
    /// Path to the data directory
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// ChatGPT provider commands
    Chatgpt {
        #[command(subcommand)]
        action: ProviderAction,
    },

    /// Claude provider commands
    Claude {
        #[command(subcommand)]
        action: ProviderAction,
    },

    /// Fathom.video provider commands
    Fathom {
        #[command(subcommand)]
        action: ProviderAction,
    },

    /// Granola provider commands
    Granola {
        #[command(subcommand)]
        action: ProviderAction,
    },

    /// Pull from all configured providers (or specify one with quaid <provider> pull)
    Pull {
        /// Only pull new or updated conversations
        #[arg(long)]
        new_only: bool,
    },

    /// List local conversations
    List {
        /// Filter by provider
        #[arg(long)]
        provider: Option<String>,

        /// Show archived conversations
        #[arg(long)]
        archived: bool,
    },

    /// Search conversations
    Search {
        /// Search query
        query: String,

        /// Maximum number of results
        #[arg(long, default_value = "20")]
        limit: usize,

        /// Use semantic (vector) search
        #[arg(long)]
        semantic: bool,

        /// Use hybrid search (FTS + semantic)
        #[arg(long)]
        hybrid: bool,
    },

    /// Export conversations
    Export {
        /// Output path
        path: PathBuf,

        /// Export format (jsonl, markdown, json)
        #[arg(long, default_value = "jsonl")]
        format: String,

        /// Filter by provider
        #[arg(long)]
        provider: Option<String>,
    },

    /// Show statistics
    Stats,
}

/// Actions available for each provider
#[derive(Subcommand)]
enum ProviderAction {
    /// Authenticate with this provider
    Auth,

    /// Pull conversations from this provider
    Pull {
        /// Only pull new or updated conversations
        #[arg(long)]
        new_only: bool,
    },
}

fn get_data_dir(cli_path: Option<PathBuf>) -> PathBuf {
    cli_path.unwrap_or_else(|| {
        dirs::data_dir()
            .map(|p| p.join("quaid"))
            .unwrap_or_else(|| PathBuf::from(".quaid"))
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let data_dir = get_data_dir(cli.data_dir);

    // Ensure data directory exists
    std::fs::create_dir_all(&data_dir)?;

    let db_path = data_dir.join("quaid.db");
    let store = quaid_core::Store::open(&db_path)?;

    match cli.command {
        Commands::Chatgpt { action } => match action {
            ProviderAction::Auth => {
                commands::auth::run("chatgpt", &store).await?;
            }
            ProviderAction::Pull { new_only } => {
                commands::pull::run(Some("chatgpt"), new_only, &store, &data_dir).await?;
            }
        },
        Commands::Claude { action } => match action {
            ProviderAction::Auth => {
                commands::auth::run("claude", &store).await?;
            }
            ProviderAction::Pull { new_only } => {
                commands::pull::run(Some("claude"), new_only, &store, &data_dir).await?;
            }
        },
        Commands::Fathom { action } => match action {
            ProviderAction::Auth => {
                commands::auth::run("fathom", &store).await?;
            }
            ProviderAction::Pull { new_only } => {
                commands::pull::run(Some("fathom"), new_only, &store, &data_dir).await?;
            }
        },
        Commands::Granola { action } => match action {
            ProviderAction::Auth => {
                commands::auth::run("granola", &store).await?;
            }
            ProviderAction::Pull { new_only } => {
                commands::pull::run(Some("granola"), new_only, &store, &data_dir).await?;
            }
        },
        Commands::Pull { new_only } => {
            commands::pull::run(None, new_only, &store, &data_dir).await?;
        }
        Commands::List { provider, archived } => {
            commands::list::run(provider.as_deref(), archived, &store)?;
        }
        Commands::Search {
            query,
            limit,
            semantic,
            hybrid,
        } => {
            commands::search::run(&query, limit, semantic, hybrid, &store, &data_dir)?;
        }
        Commands::Export {
            path,
            format,
            provider,
        } => {
            commands::export::run(&path, &format, provider.as_deref(), &store)?;
        }
        Commands::Stats => {
            commands::stats::run(&store)?;
        }
    }

    Ok(())
}
