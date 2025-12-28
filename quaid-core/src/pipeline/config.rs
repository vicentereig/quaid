//! Pipeline configuration

use std::path::{Path, PathBuf};

/// Configuration for the processing pipeline
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Base directory for data storage
    pub data_dir: PathBuf,
    /// Number of fetch workers (Stage 1)
    pub fetch_workers: usize,
    /// Number of media download workers (Stage 2)
    pub media_workers: usize,
    /// Number of embed/persist workers (Stage 3)
    pub embed_workers: usize,
    /// Channel buffer capacity
    pub channel_capacity: usize,
}

impl PipelineConfig {
    /// Create a new config with default worker counts based on CPU count
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        let cpus = num_cpus::get();
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            fetch_workers: cpus,
            media_workers: cpus / 2,
            embed_workers: cpus / 2,
            channel_capacity: 100,
        }
    }

    /// Create with custom worker counts
    pub fn with_workers(
        data_dir: impl AsRef<Path>,
        fetch: usize,
        media: usize,
        embed: usize,
    ) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            fetch_workers: fetch.max(1),
            media_workers: media.max(1),
            embed_workers: embed.max(1),
            channel_capacity: 100,
        }
    }

    /// Get models directory
    pub fn models_dir(&self) -> PathBuf {
        self.data_dir.join("models")
    }

    /// Get conversations directory
    pub fn conversations_dir(&self) -> PathBuf {
        self.data_dir.join("conversations")
    }

    /// Get embeddings directory
    pub fn embeddings_dir(&self) -> PathBuf {
        self.data_dir.join("embeddings")
    }

    /// Get media directory
    pub fn media_dir(&self) -> PathBuf {
        self.data_dir.join("media")
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self::new(".")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default_workers() {
        let config = PipelineConfig::new("/tmp/test");

        assert!(config.fetch_workers >= 1);
        assert!(config.media_workers >= 1);
        assert!(config.embed_workers >= 1);
    }

    #[test]
    fn test_config_custom_workers() {
        let config = PipelineConfig::with_workers("/tmp/test", 8, 4, 2);

        assert_eq!(config.fetch_workers, 8);
        assert_eq!(config.media_workers, 4);
        assert_eq!(config.embed_workers, 2);
    }

    #[test]
    fn test_config_min_workers() {
        let config = PipelineConfig::with_workers("/tmp/test", 0, 0, 0);

        assert_eq!(config.fetch_workers, 1);
        assert_eq!(config.media_workers, 1);
        assert_eq!(config.embed_workers, 1);
    }

    #[test]
    fn test_config_directories() {
        let config = PipelineConfig::new("/data/quaid");

        assert_eq!(config.models_dir(), PathBuf::from("/data/quaid/models"));
        assert_eq!(
            config.conversations_dir(),
            PathBuf::from("/data/quaid/conversations")
        );
        assert_eq!(
            config.embeddings_dir(),
            PathBuf::from("/data/quaid/embeddings")
        );
        assert_eq!(config.media_dir(), PathBuf::from("/data/quaid/media"));
    }
}
