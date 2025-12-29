//! Embeddings compactor
//!
//! Consolidates per-conversation parquet files into a single file per provider
//! to reduce file handle usage during semantic search.

use super::{ParquetStorageConfig, Result, StorageError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};

/// Compactor for consolidating embeddings parquet files
pub struct EmbeddingsCompactor {
    config: ParquetStorageConfig,
}

/// Result of a compaction operation
#[derive(Debug)]
pub struct CompactionResult {
    pub provider: String,
    pub files_merged: usize,
    pub total_rows: usize,
    pub output_path: std::path::PathBuf,
}

impl EmbeddingsCompactor {
    pub fn new(config: ParquetStorageConfig) -> Self {
        Self { config }
    }

    /// Compact all providers' embeddings
    pub fn compact_all(&self) -> Result<Vec<CompactionResult>> {
        let providers = self
            .config
            .list_embedding_providers()
            .map_err(StorageError::Io)?;

        let mut results = Vec::new();
        for provider in providers {
            if let Some(result) = self.compact_provider(&provider)? {
                results.push(result);
            }
        }
        Ok(results)
    }

    /// Compact embeddings for a single provider
    ///
    /// Reads all parquet files in embeddings/{provider}/*.parquet and
    /// writes them to embeddings/{provider}.parquet
    pub fn compact_provider(&self, provider: &str) -> Result<Option<CompactionResult>> {
        let source_dir = self.config.embeddings_dir(provider);
        let output_path = self.config.consolidated_embeddings_path(provider);

        // Check if source directory exists
        if !source_dir.exists() {
            // Check if already consolidated
            if output_path.exists() {
                return Ok(None); // Already consolidated, nothing to do
            }
            return Ok(None); // No embeddings for this provider
        }

        // Collect all parquet files
        let parquet_files: Vec<_> = fs::read_dir(&source_dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "parquet").unwrap_or(false))
            .collect();

        if parquet_files.is_empty() {
            return Ok(None);
        }

        // Create output file
        let output_file = File::create(&output_path)?;

        // Read first file to get schema
        let first_file = File::open(&parquet_files[0])?;
        let first_reader = ParquetRecordBatchReaderBuilder::try_new(first_file)
            .map_err(|e| StorageError::Parquet(e.to_string()))?;
        let schema = first_reader.schema().clone();

        // Create writer
        let props = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .build();

        let mut writer = ArrowWriter::try_new(output_file, schema, Some(props))
            .map_err(|e| StorageError::Parquet(e.to_string()))?;

        let mut total_rows = 0;
        let files_merged = parquet_files.len();

        // Read and write all files
        for file_path in &parquet_files {
            let file = File::open(file_path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| StorageError::Parquet(e.to_string()))?;
            let reader = builder
                .build()
                .map_err(|e| StorageError::Parquet(e.to_string()))?;

            for batch_result in reader {
                let batch = batch_result?;
                total_rows += batch.num_rows();
                writer
                    .write(&batch)
                    .map_err(|e| StorageError::Parquet(e.to_string()))?;
            }
        }

        writer
            .close()
            .map_err(|e| StorageError::Parquet(e.to_string()))?;

        // Remove old directory after successful write
        fs::remove_dir_all(&source_dir)?;

        Ok(Some(CompactionResult {
            provider: provider.to_string(),
            files_merged,
            total_rows,
            output_path,
        }))
    }

    /// Check if a provider has per-conversation embeddings that can be compacted
    pub fn needs_compaction(&self, provider: &str) -> bool {
        let source_dir = self.config.embeddings_dir(provider);
        source_dir.exists() && source_dir.is_dir()
    }

    /// Get compaction status for all providers
    pub fn status(&self) -> Result<Vec<ProviderStatus>> {
        let providers = self
            .config
            .list_embedding_providers()
            .map_err(StorageError::Io)?;

        let mut statuses = Vec::new();
        for provider in providers {
            let source_dir = self.config.embeddings_dir(&provider);
            let consolidated_path = self.config.consolidated_embeddings_path(&provider);

            let status = if consolidated_path.exists() {
                let file = File::open(&consolidated_path)?;
                let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|e| StorageError::Parquet(e.to_string()))?;
                let metadata = reader.metadata();
                ProviderStatus {
                    provider: provider.clone(),
                    is_consolidated: true,
                    file_count: 1,
                    total_rows: metadata.file_metadata().num_rows() as usize,
                }
            } else if source_dir.exists() {
                let file_count = fs::read_dir(&source_dir)?
                    .filter_map(|e| e.ok())
                    .filter(|e| {
                        e.path()
                            .extension()
                            .map(|ext| ext == "parquet")
                            .unwrap_or(false)
                    })
                    .count();
                ProviderStatus {
                    provider: provider.clone(),
                    is_consolidated: false,
                    file_count,
                    total_rows: 0, // Would need to scan all files
                }
            } else {
                continue;
            };

            statuses.push(status);
        }

        Ok(statuses)
    }
}

/// Status of a provider's embeddings
#[derive(Debug)]
pub struct ProviderStatus {
    pub provider: String,
    pub is_consolidated: bool,
    pub file_count: usize,
    pub total_rows: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::EmbeddingsStore;
    use tempfile::tempdir;

    fn create_test_embedding() -> Vec<f32> {
        (0..384).map(|i| i as f32 / 384.0).collect()
    }

    fn create_test_chunk(msg_id: &str, index: usize, text: &str) -> crate::embeddings::Chunk {
        crate::embeddings::Chunk {
            text: text.to_string(),
            message_id: msg_id.to_string(),
            chunk_index: index,
            total_chunks: 1,
        }
    }

    #[test]
    fn test_compact_provider() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());

        // Write some embeddings
        let store = EmbeddingsStore::new(config.clone());
        for i in 0..5 {
            let chunks = vec![create_test_chunk(&format!("msg-{}", i), 0, "Hello world")];
            let embeddings = vec![create_test_embedding()];
            store
                .write_embeddings(&format!("conv-{}", i), "test_provider", &chunks, &embeddings)
                .unwrap();
        }

        // Verify files exist
        let source_dir = config.embeddings_dir("test_provider");
        assert!(source_dir.exists());
        assert_eq!(fs::read_dir(&source_dir).unwrap().count(), 5);

        // Compact
        let compactor = EmbeddingsCompactor::new(config.clone());
        let result = compactor.compact_provider("test_provider").unwrap();

        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.files_merged, 5);
        assert_eq!(result.total_rows, 5);

        // Verify consolidated file exists
        assert!(config.consolidated_embeddings_path("test_provider").exists());

        // Verify source directory is removed
        assert!(!source_dir.exists());
    }

    #[test]
    fn test_compact_nonexistent_provider() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let compactor = EmbeddingsCompactor::new(config);

        let result = compactor.compact_provider("nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_needs_compaction() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());

        // Create a provider directory
        let provider_dir = config.embeddings_dir("test");
        fs::create_dir_all(&provider_dir).unwrap();

        let compactor = EmbeddingsCompactor::new(config);
        assert!(compactor.needs_compaction("test"));
        assert!(!compactor.needs_compaction("nonexistent"));
    }
}
