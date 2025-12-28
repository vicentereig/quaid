//! ONNX embedding model wrapper
//!
//! Provides embedding generation using ONNX Runtime with multilingual models.

use super::Result;
use std::path::{Path, PathBuf};

/// Configuration for the embedding model
#[derive(Debug, Clone)]
pub struct EmbeddingModelConfig {
    /// Path to the ONNX model file
    pub model_path: PathBuf,
    /// Path to the tokenizer JSON file
    pub tokenizer_path: PathBuf,
    /// Maximum sequence length
    pub max_length: usize,
    /// Embedding dimension (384 for e5-small)
    pub embedding_dim: usize,
}

impl EmbeddingModelConfig {
    /// Create config for multilingual-e5-small
    pub fn multilingual_e5_small(models_dir: impl AsRef<Path>) -> Self {
        let models_dir = models_dir.as_ref();
        Self {
            model_path: models_dir.join("multilingual-e5-small.onnx"),
            tokenizer_path: models_dir.join("multilingual-e5-small-tokenizer.json"),
            max_length: 512,
            embedding_dim: 384,
        }
    }
}

/// Trait for embedding models (allows mocking)
pub trait Embedder: Send + Sync {
    /// Get the embedding dimension
    fn embedding_dim(&self) -> usize;

    /// Generate embedding for a single text
    fn embed(&self, text: &str) -> Result<Vec<f32>>;

    /// Generate embeddings for a batch of texts
    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>>;
}

/// ONNX-based embedding model
///
/// Uses multilingual-e5-small for English/Spanish text embedding.
pub struct EmbeddingModel {
    #[allow(dead_code)]
    config: EmbeddingModelConfig,
    // Session and tokenizer will be added when model files are available
    mock: MockEmbeddingModel,
}

impl EmbeddingModel {
    /// Load an embedding model from files
    ///
    /// If model files don't exist, falls back to mock embeddings.
    pub fn load(config: EmbeddingModelConfig) -> Result<Self> {
        // TODO: Implement real ONNX loading when model files are available
        // For now, use mock embeddings for development
        let mock = MockEmbeddingModel::new(config.embedding_dim);

        Ok(Self { config, mock })
    }

    /// Load with auto-download if model doesn't exist
    pub fn load_or_download(models_dir: impl AsRef<Path>) -> Result<Self> {
        let config = EmbeddingModelConfig::multilingual_e5_small(&models_dir);

        // TODO: Implement model download from HuggingFace
        // For now, just use mock
        Self::load(config)
    }

    /// Compute mean pooled embedding from multiple embeddings (for conversation-level)
    pub fn mean_pool(embeddings: &[Vec<f32>]) -> Vec<f32> {
        if embeddings.is_empty() {
            return vec![];
        }

        let dim = embeddings[0].len();
        let mut result = vec![0.0f32; dim];

        for embedding in embeddings {
            for (i, &v) in embedding.iter().enumerate() {
                if i < dim {
                    result[i] += v;
                }
            }
        }

        let n = embeddings.len() as f32;
        result.iter_mut().for_each(|x| *x /= n);

        // L2 normalize
        let norm: f32 = result.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            result.iter_mut().for_each(|x| *x /= norm);
        }

        result
    }
}

impl Embedder for EmbeddingModel {
    fn embedding_dim(&self) -> usize {
        self.config.embedding_dim
    }

    fn embed(&self, text: &str) -> Result<Vec<f32>> {
        // TODO: Use real ONNX inference when implemented
        Ok(self.mock.embed(text))
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        // TODO: Use real ONNX batch inference when implemented
        Ok(self.mock.embed_batch(texts))
    }
}

/// Mock embedding model for testing (returns deterministic embeddings)
#[derive(Clone)]
pub struct MockEmbeddingModel {
    dim: usize,
}

impl MockEmbeddingModel {
    pub fn new(dim: usize) -> Self {
        Self { dim }
    }

    /// Generate a deterministic embedding based on text hash
    pub fn embed(&self, text: &str) -> Vec<f32> {
        let hash = text.bytes().fold(0u64, |acc, b| {
            acc.wrapping_mul(31).wrapping_add(b as u64)
        });

        // Generate deterministic values based on hash
        let mut embedding: Vec<f32> = (0..self.dim)
            .map(|i| {
                let val = ((hash.wrapping_mul(i as u64 + 1)) % 1000) as f32 / 1000.0 - 0.5;
                val / (self.dim as f32).sqrt()
            })
            .collect();

        // L2 normalize
        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            embedding.iter_mut().for_each(|x| *x /= norm);
        }

        embedding
    }

    pub fn embed_batch(&self, texts: &[&str]) -> Vec<Vec<f32>> {
        texts.iter().map(|t| self.embed(t)).collect()
    }
}

impl Embedder for MockEmbeddingModel {
    fn embedding_dim(&self) -> usize {
        self.dim
    }

    fn embed(&self, text: &str) -> Result<Vec<f32>> {
        Ok(MockEmbeddingModel::embed(self, text))
    }

    fn embed_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        Ok(MockEmbeddingModel::embed_batch(self, texts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_embedding_deterministic() {
        let model = MockEmbeddingModel::new(384);

        let embedding1 = model.embed("Hello world");
        let embedding2 = model.embed("Hello world");

        assert_eq!(embedding1, embedding2);
    }

    #[test]
    fn test_mock_embedding_dimension() {
        let model = MockEmbeddingModel::new(384);

        let embedding = model.embed("Test text");

        assert_eq!(embedding.len(), 384);
    }

    #[test]
    fn test_mock_embedding_normalized() {
        let model = MockEmbeddingModel::new(384);

        let embedding = model.embed("Test text");

        let norm: f32 = embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.01, "Embedding should be L2 normalized");
    }

    #[test]
    fn test_mock_embedding_different_texts() {
        let model = MockEmbeddingModel::new(384);

        let embedding1 = model.embed("Hello");
        let embedding2 = model.embed("World");

        assert_ne!(embedding1, embedding2);
    }

    #[test]
    fn test_mock_embedding_batch() {
        let model = MockEmbeddingModel::new(384);

        let embeddings = model.embed_batch(&["Hello", "World"]);

        assert_eq!(embeddings.len(), 2);
        assert_eq!(embeddings[0].len(), 384);
        assert_eq!(embeddings[1].len(), 384);
    }

    #[test]
    fn test_embedder_trait() {
        let model: Box<dyn Embedder> = Box::new(MockEmbeddingModel::new(384));

        let embedding = model.embed("Test").unwrap();
        assert_eq!(embedding.len(), 384);
    }

    #[test]
    fn test_mean_pool() {
        let embedding1 = vec![1.0, 0.0, 0.0];
        let embedding2 = vec![0.0, 1.0, 0.0];

        let pooled = EmbeddingModel::mean_pool(&[embedding1, embedding2]);

        assert_eq!(pooled.len(), 3);
        // Should be normalized
        let norm: f32 = pooled.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_mean_pool_empty() {
        let pooled = EmbeddingModel::mean_pool(&[]);
        assert!(pooled.is_empty());
    }

    #[test]
    fn test_embedding_model_config() {
        let config = EmbeddingModelConfig::multilingual_e5_small("/tmp/models");

        assert_eq!(config.embedding_dim, 384);
        assert_eq!(config.max_length, 512);
        assert!(config
            .model_path
            .to_string_lossy()
            .contains("multilingual-e5-small.onnx"));
    }

    #[test]
    fn test_embedding_model_load_mock() {
        let config = EmbeddingModelConfig::multilingual_e5_small("/nonexistent");
        let model = EmbeddingModel::load(config).unwrap();

        // Should work with mock fallback
        let embedding = model.embed("Test").unwrap();
        assert_eq!(embedding.len(), 384);
    }
}
