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
    /// Authenticate with a provider
    Auth {
        /// Provider to authenticate with (chatgpt, claude, gemini)
        provider: String,
    },

    /// Pull conversations from a provider
    Pull {
        /// Provider to pull from (chatgpt, claude, gemini, or --all)
        provider: Option<String>,

        /// Pull from all configured providers
        #[arg(long)]
        all: bool,
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
        Commands::Auth { provider } => {
            commands::auth::run(&provider, &store).await?;
        }
        Commands::Pull { provider, all } => {
            commands::pull::run(provider.as_deref(), all, &store, &data_dir).await?;
        }
        Commands::List { provider, archived } => {
            commands::list::run(provider.as_deref(), archived, &store)?;
        }
        Commands::Search { query, limit } => {
            commands::search::run(&query, limit, &store)?;
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
