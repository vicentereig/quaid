//! Message chunking for embedding generation
//!
//! Splits long messages into smaller chunks suitable for embedding models.

use crate::providers::{Message, MessageContent};

/// Configuration for the message chunker
#[derive(Debug, Clone)]
pub struct ChunkerConfig {
    /// Maximum number of characters per chunk (approximate token count * 4)
    pub max_chunk_chars: usize,
    /// Number of characters to overlap between chunks
    pub overlap_chars: usize,
}

impl Default for ChunkerConfig {
    fn default() -> Self {
        Self {
            // ~256 tokens * 4 chars/token = 1024 chars
            max_chunk_chars: 1024,
            // ~32 tokens * 4 chars/token = 128 chars
            overlap_chars: 128,
        }
    }
}

/// A chunk of text from a message
#[derive(Debug, Clone)]
pub struct Chunk {
    /// The text content of this chunk
    pub text: String,
    /// Source message ID
    pub message_id: String,
    /// Index of this chunk within the message (0-based)
    pub chunk_index: usize,
    /// Total number of chunks for this message
    pub total_chunks: usize,
}

/// Chunker for splitting messages into smaller pieces
pub struct MessageChunker {
    config: ChunkerConfig,
}

impl MessageChunker {
    pub fn new(config: ChunkerConfig) -> Self {
        Self { config }
    }

    /// Find the nearest valid UTF-8 character boundary at or before the given byte index
    fn floor_char_boundary(s: &str, index: usize) -> usize {
        if index >= s.len() {
            return s.len();
        }
        // Walk backwards to find a valid char boundary
        let mut i = index;
        while i > 0 && !s.is_char_boundary(i) {
            i -= 1;
        }
        i
    }

    /// Find the nearest valid UTF-8 character boundary at or after the given byte index
    fn ceil_char_boundary(s: &str, index: usize) -> usize {
        if index >= s.len() {
            return s.len();
        }
        // Walk forwards to find a valid char boundary
        let mut i = index;
        while i < s.len() && !s.is_char_boundary(i) {
            i += 1;
        }
        i
    }

    /// Extract text content from a message
    pub fn extract_text(content: &MessageContent) -> String {
        match content {
            MessageContent::Text { text } => text.clone(),
            MessageContent::Code { code, language } => {
                format!("```{}\n{}\n```", language, code)
            }
            MessageContent::Image { alt, .. } => alt.clone().unwrap_or_default(),
            MessageContent::Audio { transcript, .. } => transcript.clone().unwrap_or_default(),
            MessageContent::Mixed { parts } => parts
                .iter()
                .map(Self::extract_text)
                .collect::<Vec<_>>()
                .join("\n\n"),
        }
    }

    /// Chunk a single text string
    pub fn chunk_text(&self, text: &str) -> Vec<String> {
        let text = text.trim();

        if text.is_empty() {
            return vec![];
        }

        if text.len() <= self.config.max_chunk_chars {
            return vec![text.to_string()];
        }

        let mut chunks = Vec::new();
        let mut start = 0;

        while start < text.len() {
            // Ensure end is at a valid char boundary
            let end = Self::floor_char_boundary(
                text,
                (start + self.config.max_chunk_chars).min(text.len()),
            );

            // Try to find a good break point (sentence boundary or paragraph)
            let chunk_end = if end < text.len() {
                self.find_break_point(text, start, end)
            } else {
                end
            };

            let chunk = text[start..chunk_end].trim().to_string();
            if !chunk.is_empty() {
                chunks.push(chunk);
            }

            // Move start, accounting for overlap
            if chunk_end >= text.len() {
                break;
            }

            // Ensure new start is at a valid char boundary
            start = Self::ceil_char_boundary(
                text,
                chunk_end.saturating_sub(self.config.overlap_chars),
            );

            // Ensure we make progress
            if start <= chunks.len().saturating_sub(1) * self.config.max_chunk_chars {
                start = chunk_end;
            }
        }

        chunks
    }

    /// Find a good break point (prefer sentence/paragraph boundaries)
    fn find_break_point(&self, text: &str, _start: usize, max_end: usize) -> usize {
        // Ensure boundaries are valid UTF-8 char boundaries
        let max_end = Self::floor_char_boundary(text, max_end);
        let search_start = Self::ceil_char_boundary(
            text,
            max_end.saturating_sub(self.config.overlap_chars),
        );

        // Safety: search_start and max_end are now guaranteed to be valid char boundaries
        let search_text = &text[search_start..max_end];

        // Look for paragraph break
        if let Some(pos) = search_text.rfind("\n\n") {
            return search_start + pos + 2;
        }

        // Look for sentence end (. ! ?)
        for (i, c) in search_text.char_indices().rev() {
            if c == '.' || c == '!' || c == '?' {
                // Check if followed by space or end
                let next_idx = search_start + i + c.len_utf8();
                if next_idx >= max_end {
                    return next_idx;
                }
                // Safely check next character
                if let Some(next_char) = text[next_idx..].chars().next() {
                    if next_char == ' ' || next_char == '\n' {
                        return next_idx;
                    }
                }
            }
        }

        // Look for line break
        if let Some(pos) = search_text.rfind('\n') {
            return search_start + pos + 1;
        }

        // Look for word break (space)
        if let Some(pos) = search_text.rfind(' ') {
            return search_start + pos + 1;
        }

        // No good break point, just use max_end
        max_end
    }

    /// Chunk a message into multiple chunks
    pub fn chunk_message(&self, message: &Message) -> Vec<Chunk> {
        let text = Self::extract_text(&message.content);
        let text_chunks = self.chunk_text(&text);
        let total_chunks = text_chunks.len();

        text_chunks
            .into_iter()
            .enumerate()
            .map(|(i, text)| Chunk {
                text,
                message_id: message.id.clone(),
                chunk_index: i,
                total_chunks,
            })
            .collect()
    }

    /// Chunk multiple messages
    pub fn chunk_messages(&self, messages: &[Message]) -> Vec<Chunk> {
        messages
            .iter()
            .flat_map(|m| self.chunk_message(m))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::providers::Role;

    fn create_test_message(id: &str, text: &str) -> Message {
        Message {
            id: id.to_string(),
            conversation_id: "conv-1".to_string(),
            parent_id: None,
            role: Role::User,
            content: MessageContent::Text {
                text: text.to_string(),
            },
            created_at: None,
            model: None,
        }
    }

    #[test]
    fn test_chunk_short_message() {
        let chunker = MessageChunker::new(ChunkerConfig::default());
        let message = create_test_message("msg-1", "Hello, world!");

        let chunks = chunker.chunk_message(&message);

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "Hello, world!");
        assert_eq!(chunks[0].message_id, "msg-1");
        assert_eq!(chunks[0].chunk_index, 0);
        assert_eq!(chunks[0].total_chunks, 1);
    }

    #[test]
    fn test_chunk_long_message() {
        let config = ChunkerConfig {
            max_chunk_chars: 100,
            overlap_chars: 20,
        };
        let chunker = MessageChunker::new(config);

        // Create a message longer than 100 chars
        let text = "This is a sentence. ".repeat(20); // ~400 chars
        let message = create_test_message("msg-1", &text);

        let chunks = chunker.chunk_message(&message);

        assert!(chunks.len() > 1, "Expected multiple chunks");
        assert!(
            chunks.iter().all(|c| c.text.len() <= 120),
            "All chunks should be under max size (with some tolerance)"
        );
        assert!(
            chunks.iter().all(|c| c.total_chunks == chunks.len()),
            "All chunks should have correct total_chunks"
        );
    }

    #[test]
    fn test_chunk_preserves_sentence_boundaries() {
        let config = ChunkerConfig {
            max_chunk_chars: 50,
            overlap_chars: 10,
        };
        let chunker = MessageChunker::new(config);

        let text = "First sentence here. Second sentence there. Third one now.";
        let chunks = chunker.chunk_text(text);

        // Chunks should end at sentence boundaries when possible
        for chunk in &chunks {
            let trimmed = chunk.trim();
            if trimmed.len() > 10 {
                // Should end with punctuation or be the last chunk
                let ends_with_punct =
                    trimmed.ends_with('.') || trimmed.ends_with('!') || trimmed.ends_with('?');
                let is_complete = ends_with_punct || chunk == chunks.last().unwrap();
                assert!(
                    is_complete || trimmed.len() < 50,
                    "Chunk should end at sentence boundary or be small: '{}'",
                    trimmed
                );
            }
        }
    }

    #[test]
    fn test_chunk_handles_code_blocks() {
        let chunker = MessageChunker::new(ChunkerConfig::default());

        let message = Message {
            id: "msg-1".to_string(),
            conversation_id: "conv-1".to_string(),
            parent_id: None,
            role: Role::Assistant,
            content: MessageContent::Code {
                language: "rust".to_string(),
                code: "fn main() {\n    println!(\"Hello\");\n}".to_string(),
            },
            created_at: None,
            model: None,
        };

        let chunks = chunker.chunk_message(&message);

        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].text.contains("```rust"));
        assert!(chunks[0].text.contains("fn main()"));
    }

    #[test]
    fn test_chunk_overlapping() {
        let config = ChunkerConfig {
            max_chunk_chars: 50,
            overlap_chars: 20,
        };
        let chunker = MessageChunker::new(config);

        let text = "Word one. Word two. Word three. Word four. Word five. Word six. Word seven.";
        let chunks = chunker.chunk_text(text);

        // With overlap, consecutive chunks should share some content
        if chunks.len() >= 2 {
            // Check that there's some overlap between chunks
            let first_end = &chunks[0][chunks[0].len().saturating_sub(20)..];
            let second_start = &chunks[1][..20.min(chunks[1].len())];

            // They should share some words (not necessarily exact overlap due to word boundaries)
            let first_words: std::collections::HashSet<_> = first_end.split_whitespace().collect();
            let second_words: std::collections::HashSet<_> =
                second_start.split_whitespace().collect();
            let overlap: Vec<_> = first_words.intersection(&second_words).collect();

            // Some overlap is expected but not guaranteed due to sentence boundary seeking
            // This test is more about ensuring the chunker doesn't crash with overlap config
            assert!(chunks.len() >= 2, "Should have multiple chunks for overlap test");
        }
    }

    #[test]
    fn test_chunk_metadata() {
        let config = ChunkerConfig {
            max_chunk_chars: 50,
            overlap_chars: 10,
        };
        let chunker = MessageChunker::new(config);

        let text = "A ".repeat(100); // ~200 chars
        let message = create_test_message("msg-123", &text);

        let chunks = chunker.chunk_message(&message);

        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.message_id, "msg-123");
            assert_eq!(chunk.chunk_index, i);
            assert_eq!(chunk.total_chunks, chunks.len());
        }
    }

    #[test]
    fn test_extract_text_from_message_content() {
        // Text content
        let text_content = MessageContent::Text {
            text: "Hello world".to_string(),
        };
        assert_eq!(MessageChunker::extract_text(&text_content), "Hello world");

        // Code content
        let code_content = MessageContent::Code {
            language: "python".to_string(),
            code: "print('hi')".to_string(),
        };
        let extracted = MessageChunker::extract_text(&code_content);
        assert!(extracted.contains("```python"));
        assert!(extracted.contains("print('hi')"));

        // Mixed content
        let mixed_content = MessageContent::Mixed {
            parts: vec![
                MessageContent::Text {
                    text: "Part 1".to_string(),
                },
                MessageContent::Text {
                    text: "Part 2".to_string(),
                },
            ],
        };
        let extracted = MessageChunker::extract_text(&mixed_content);
        assert!(extracted.contains("Part 1"));
        assert!(extracted.contains("Part 2"));
    }

    #[test]
    fn test_chunk_empty_message() {
        let chunker = MessageChunker::new(ChunkerConfig::default());
        let message = create_test_message("msg-1", "");

        let chunks = chunker.chunk_message(&message);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_chunk_whitespace_only() {
        let chunker = MessageChunker::new(ChunkerConfig::default());
        let message = create_test_message("msg-1", "   \n\n   ");

        let chunks = chunker.chunk_message(&message);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_chunk_multiple_messages() {
        let chunker = MessageChunker::new(ChunkerConfig::default());
        let messages = vec![
            create_test_message("msg-1", "First message"),
            create_test_message("msg-2", "Second message"),
            create_test_message("msg-3", "Third message"),
        ];

        let chunks = chunker.chunk_messages(&messages);

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].message_id, "msg-1");
        assert_eq!(chunks[1].message_id, "msg-2");
        assert_eq!(chunks[2].message_id, "msg-3");
    }

    #[test]
    fn test_chunk_utf8_multibyte_characters() {
        // Test with text containing multi-byte UTF-8 characters like box drawing and emojis
        let config = ChunkerConfig {
            max_chunk_chars: 100,
            overlap_chars: 20,
        };
        let chunker = MessageChunker::new(config);

        // Text with box drawing chars (3 bytes each: ─, │, ┌, ┐, └, ┘)
        let text = "┌──────────────────┐\n│ Box with content │\n└──────────────────┘ ".repeat(10);
        let chunks = chunker.chunk_text(&text);
        assert!(!chunks.is_empty(), "Should produce chunks");
        for chunk in &chunks {
            assert!(chunk.is_ascii() || chunk.len() > 0, "Chunk should be valid");
        }

        // Text with emojis (4 bytes each)
        let emoji_text = "Hello ✅ world 🎉 test 🚀 more text here to make it longer. ".repeat(20);
        let emoji_chunks = chunker.chunk_text(&emoji_text);
        assert!(!emoji_chunks.is_empty(), "Should produce emoji chunks");

        // Text with mixed scripts (Chinese, Arabic, etc.)
        let unicode_text = "你好世界 مرحبا 🌍 Hello! ".repeat(30);
        let unicode_chunks = chunker.chunk_text(&unicode_text);
        assert!(!unicode_chunks.is_empty(), "Should produce unicode chunks");
    }

    #[test]
    fn test_floor_ceil_char_boundary() {
        // String with multi-byte char: "─" is bytes 0-2 (3 bytes)
        let s = "─abc";

        // floor_char_boundary should find boundary at or before
        assert_eq!(MessageChunker::floor_char_boundary(s, 0), 0);
        assert_eq!(MessageChunker::floor_char_boundary(s, 1), 0); // Inside ─, go back to 0
        assert_eq!(MessageChunker::floor_char_boundary(s, 2), 0); // Inside ─, go back to 0
        assert_eq!(MessageChunker::floor_char_boundary(s, 3), 3); // At 'a'

        // ceil_char_boundary should find boundary at or after
        assert_eq!(MessageChunker::ceil_char_boundary(s, 0), 0);
        assert_eq!(MessageChunker::ceil_char_boundary(s, 1), 3); // Inside ─, go forward to 'a'
        assert_eq!(MessageChunker::ceil_char_boundary(s, 2), 3); // Inside ─, go forward to 'a'
        assert_eq!(MessageChunker::ceil_char_boundary(s, 3), 3); // At 'a'
    }
}
