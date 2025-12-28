//! Embeddings module for semantic search
//!
//! Provides text chunking and ONNX-based embedding generation.

pub mod chunker;
pub mod model;

pub use chunker::{Chunk, ChunkerConfig, MessageChunker};
pub use model::{Embedder, EmbeddingModel, EmbeddingModelConfig, MockEmbeddingModel};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum EmbeddingError {
    #[error("Model error: {0}")]
    Model(String),

    #[error("Tokenizer error: {0}")]
    Tokenizer(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Download error: {0}")]
    Download(String),

    #[error("ONNX runtime error: {0}")]
    Ort(#[from] ort::Error),
}

pub type Result<T> = std::result::Result<T, EmbeddingError>;
