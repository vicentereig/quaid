use quaid_core::embeddings::{EmbeddingModel, Embedder};
use quaid_core::storage::duckdb::DuckDbQuery;
use quaid_core::storage::ParquetStorageConfig;
use quaid_core::Store;
use std::path::Path;

pub fn run(
    query: &str,
    limit: usize,
    semantic: bool,
    hybrid: bool,
    store: &Store,
    data_dir: &Path,
) -> anyhow::Result<()> {
    if semantic || hybrid {
        run_semantic_search(query, limit, hybrid, store, data_dir)
    } else {
        run_fts_search(query, limit, store)
    }
}

/// Full-text search using SQLite FTS
fn run_fts_search(query: &str, limit: usize, store: &Store) -> anyhow::Result<()> {
    println!("Searching for: {}\n", query);

    let results = store.search(query, limit)?;

    if results.is_empty() {
        println!("No results found.");
        return Ok(());
    }

    println!("Found {} results:\n", results.len());

    for (conv_id, snippet) in results {
        if let Ok(Some(conv)) = store.get_conversation(&conv_id) {
            println!("📝 {}", conv.title);
            println!("   {}", snippet);
            println!("   ID: {}", conv.id);
            println!();
        }
    }

    Ok(())
}

/// Semantic or hybrid search using embeddings
fn run_semantic_search(
    query: &str,
    limit: usize,
    hybrid: bool,
    store: &Store,
    data_dir: &Path,
) -> anyhow::Result<()> {
    let mode = if hybrid { "hybrid" } else { "semantic" };
    println!("Searching ({}) for: {}\n", mode, query);

    // Load the embedding model
    let models_dir = data_dir.join("models");
    let embedder = match EmbeddingModel::load_or_download(&models_dir) {
        Ok(model) => model,
        Err(e) => {
            eprintln!("Failed to load embedding model: {}", e);
            eprintln!("Run `quaid pull` first to download the model.");
            return Ok(());
        }
    };

    // Generate query embedding
    let query_embedding = match embedder.embed(query) {
        Ok(emb) => emb,
        Err(e) => {
            eprintln!("Failed to generate query embedding: {}", e);
            return Ok(());
        }
    };

    // Create DuckDB query interface
    let config = ParquetStorageConfig::new(data_dir);
    let duckdb = match DuckDbQuery::new(config) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Failed to create query interface: {}", e);
            return Ok(());
        }
    };

    // Perform search
    let results = if hybrid {
        duckdb.search_hybrid(query, &query_embedding, limit)?
    } else {
        duckdb.search_semantic(&query_embedding, limit)?
    };

    if results.is_empty() {
        println!("No results found.");
        println!("\nTip: Run `quaid pull` to index your conversations first.");
        return Ok(());
    }

    println!("Found {} results:\n", results.len());

    for result in results {
        // Get conversation details
        if let Ok(Some(conv)) = store.get_conversation(&result.conversation_id) {
            println!("📝 {} (score: {:.3})", conv.title, result.score);
            println!("   {}", truncate(&result.chunk_text, 80));
            println!("   ID: {}", conv.id);
            println!();
        } else {
            // Conversation not in SQLite, show basic info
            println!("📝 (score: {:.3})", result.score);
            println!("   {}", truncate(&result.chunk_text, 80));
            println!("   ID: {}", result.conversation_id);
            println!();
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
