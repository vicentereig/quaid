use quaid_core::storage::ParquetStorageConfig;
use quaid_core::EmbeddingsCompactor;
use std::path::Path;

pub fn run(data_dir: &Path) -> anyhow::Result<()> {
    let config = ParquetStorageConfig::new(data_dir);
    let compactor = EmbeddingsCompactor::new(config.clone());

    // Show current status
    println!("Checking embeddings status...\n");

    let statuses = compactor.status()?;
    if statuses.is_empty() {
        println!("No embeddings found. Run `quaid pull` first to index conversations.");
        return Ok(());
    }

    let needs_compaction: Vec<_> = statuses.iter().filter(|s| !s.is_consolidated).collect();

    if needs_compaction.is_empty() {
        println!("All embeddings are already consolidated:");
        for status in &statuses {
            println!(
                "  ✓ {} ({} rows)",
                status.provider, status.total_rows
            );
        }
        return Ok(());
    }

    // Show what will be compacted
    println!("Found {} provider(s) to compact:\n", needs_compaction.len());
    for status in &needs_compaction {
        println!(
            "  {} - {} files to merge",
            status.provider, status.file_count
        );
    }
    println!();

    // Perform compaction
    println!("Compacting embeddings...\n");
    let results = compactor.compact_all()?;

    for result in &results {
        println!(
            "  ✓ {} - merged {} files → {} rows",
            result.provider, result.files_merged, result.total_rows
        );
    }

    println!("\nDone! Semantic search will now use consolidated files.");
    Ok(())
}
