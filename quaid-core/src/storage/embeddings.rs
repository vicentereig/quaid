//! Embeddings storage using Parquet format
//!
//! Stores chunk embeddings for semantic search capabilities.

use super::{ParquetStorageConfig, Result, StorageError};
use crate::embeddings::Chunk;
use arrow::array::{ArrayRef, FixedSizeListArray, Float32Array, Int32Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::sync::Arc;

/// Embedding dimension for multilingual-e5-small
pub const EMBEDDING_DIM: i32 = 384;

/// Store for embeddings in Parquet format
pub struct EmbeddingsStore {
    config: ParquetStorageConfig,
}

impl EmbeddingsStore {
    pub fn new(config: ParquetStorageConfig) -> Self {
        Self { config }
    }

    /// Write embeddings for a conversation to Parquet
    pub fn write_embeddings(
        &self,
        conversation_id: &str,
        provider_id: &str,
        chunks: &[Chunk],
        embeddings: &[Vec<f32>],
    ) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        if chunks.len() != embeddings.len() {
            return Err(StorageError::Serialization(format!(
                "Chunk count {} != embedding count {}",
                chunks.len(),
                embeddings.len()
            )));
        }

        // Validate embedding dimensions
        for (i, emb) in embeddings.iter().enumerate() {
            if emb.len() != EMBEDDING_DIM as usize {
                return Err(StorageError::Serialization(format!(
                    "Embedding {} has dimension {}, expected {}",
                    i,
                    emb.len(),
                    EMBEDDING_DIM
                )));
            }
        }

        let path = self.config.embeddings_path(provider_id, conversation_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let schema = self.embeddings_schema();
        let batch = self.create_record_batch(conversation_id, chunks, embeddings, &schema)?;

        let file = File::create(&path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();

        let mut writer = ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| StorageError::Parquet(e.to_string()))?;

        writer
            .write(&batch)
            .map_err(|e| StorageError::Parquet(e.to_string()))?;

        writer
            .close()
            .map_err(|e| StorageError::Parquet(e.to_string()))?;

        Ok(())
    }

    fn embeddings_schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("chunk_id", DataType::Utf8, false),
            Field::new("conversation_id", DataType::Utf8, false),
            Field::new("message_id", DataType::Utf8, false),
            Field::new("chunk_index", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, false)),
                    EMBEDDING_DIM,
                ),
                false,
            ),
        ]))
    }

    fn create_record_batch(
        &self,
        conversation_id: &str,
        chunks: &[Chunk],
        embeddings: &[Vec<f32>],
        schema: &Arc<Schema>,
    ) -> Result<RecordBatch> {
        let num_rows = chunks.len();

        // Build arrays
        let mut chunk_ids = StringBuilder::new();
        let mut conv_ids = StringBuilder::new();
        let mut msg_ids = StringBuilder::new();
        let mut chunk_indices: Vec<i32> = Vec::with_capacity(num_rows);
        let mut texts = StringBuilder::new();

        for chunk in chunks {
            let chunk_id = format!("{}_{}", chunk.message_id, chunk.chunk_index);
            chunk_ids.append_value(&chunk_id);
            conv_ids.append_value(conversation_id);
            msg_ids.append_value(&chunk.message_id);
            chunk_indices.push(chunk.chunk_index as i32);
            texts.append_value(&chunk.text);
        }

        // Create embedding array (FixedSizeList of Float32)
        let flat_embeddings: Vec<f32> = embeddings.iter().flatten().copied().collect();
        let values = Float32Array::from(flat_embeddings);
        let embedding_array = FixedSizeListArray::try_new(
            Arc::new(Field::new("item", DataType::Float32, false)),
            EMBEDDING_DIM,
            Arc::new(values),
            None,
        )
        .map_err(|e| StorageError::Parquet(e.to_string()))?;

        let columns: Vec<ArrayRef> = vec![
            Arc::new(chunk_ids.finish()),
            Arc::new(conv_ids.finish()),
            Arc::new(msg_ids.finish()),
            Arc::new(Int32Array::from(chunk_indices)),
            Arc::new(texts.finish()),
            Arc::new(embedding_array),
        ];

        RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| StorageError::Parquet(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_chunk(msg_id: &str, index: usize, text: &str) -> Chunk {
        Chunk {
            text: text.to_string(),
            message_id: msg_id.to_string(),
            chunk_index: index,
            total_chunks: 1,
        }
    }

    fn create_test_embedding() -> Vec<f32> {
        (0..EMBEDDING_DIM).map(|i| i as f32 / EMBEDDING_DIM as f32).collect()
    }

    #[test]
    fn test_write_embeddings() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = EmbeddingsStore::new(config.clone());

        let chunks = vec![
            create_test_chunk("msg-1", 0, "Hello world"),
            create_test_chunk("msg-2", 0, "How are you?"),
        ];

        let embeddings = vec![create_test_embedding(), create_test_embedding()];

        store
            .write_embeddings("conv-1", "chatgpt", &chunks, &embeddings)
            .unwrap();

        // Verify file was created
        let path = config.embeddings_path("chatgpt", "conv-1");
        assert!(path.exists());
    }

    #[test]
    fn test_write_empty_embeddings() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = EmbeddingsStore::new(config);

        let result = store.write_embeddings("conv-1", "chatgpt", &[], &[]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_mismatched_chunks_embeddings() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = EmbeddingsStore::new(config);

        let chunks = vec![create_test_chunk("msg-1", 0, "Hello")];
        let embeddings: Vec<Vec<f32>> = vec![]; // Empty - mismatch!

        let result = store.write_embeddings("conv-1", "chatgpt", &chunks, &embeddings);
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_embedding_dimension() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = EmbeddingsStore::new(config);

        let chunks = vec![create_test_chunk("msg-1", 0, "Hello")];
        let embeddings = vec![vec![0.1, 0.2, 0.3]]; // Wrong dimension!

        let result = store.write_embeddings("conv-1", "chatgpt", &chunks, &embeddings);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_chunks_same_message() {
        let dir = tempdir().unwrap();
        let config = ParquetStorageConfig::new(dir.path());
        let store = EmbeddingsStore::new(config.clone());

        let chunks = vec![
            Chunk {
                text: "First part".to_string(),
                message_id: "msg-1".to_string(),
                chunk_index: 0,
                total_chunks: 3,
            },
            Chunk {
                text: "Second part".to_string(),
                message_id: "msg-1".to_string(),
                chunk_index: 1,
                total_chunks: 3,
            },
            Chunk {
                text: "Third part".to_string(),
                message_id: "msg-1".to_string(),
                chunk_index: 2,
                total_chunks: 3,
            },
        ];

        let embeddings: Vec<Vec<f32>> = (0..3).map(|_| create_test_embedding()).collect();

        store
            .write_embeddings("conv-1", "chatgpt", &chunks, &embeddings)
            .unwrap();

        let path = config.embeddings_path("chatgpt", "conv-1");
        assert!(path.exists());
    }
}
