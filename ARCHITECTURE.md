# Quaid Architecture

This document describes the system architecture, data flow, and concurrency model of quaid.

## System Overview

Quaid is a CLI tool for exporting and indexing AI conversations from multiple providers. It uses a hybrid storage approach with a parallel processing pipeline.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              quaid CLI                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  Commands: auth, pull, search, export, stats                            │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            quaid-core                                    │
├─────────────────┬─────────────────┬─────────────────┬───────────────────┤
│    Providers    │    Pipeline     │    Storage      │    Embeddings     │
│  ─────────────  │  ────────────   │  ────────────   │  ──────────────   │
│  • ChatGPT      │  • Fetch Stage  │  • SQLite       │  • Chunker        │
│  • Claude       │  • Media Stage  │  • Parquet      │  • ONNX Model     │
│  • Fathom       │  • Embed Stage  │  • DuckDB       │  • Mean Pooling   │
│  • Granola      │                 │                 │                   │
└─────────────────┴─────────────────┴─────────────────┴───────────────────┘
```

## Directory Structure

```
~/.quaid/
├── quaid.db                              # SQLite: accounts, auth, FTS catalog
├── conversations/
│   └── {provider}/
│       └── {conversation_id}.parquet    # One file per conversation
├── embeddings/
│   └── {provider}/
│       └── {conversation_id}.parquet    # Embedding vectors
├── media/
│   └── {provider}/
│       └── {conversation_id}/
│           └── {filename}               # Downloaded attachments
└── models/
    └── multilingual-e5-small/           # ONNX model (~118MB)
        ├── model.onnx
        └── tokenizer.json
```

## Storage Architecture

Quaid uses a hybrid storage model optimizing for different access patterns:

### SQLite (quaid.db)

- **Accounts**: Provider credentials and sync state
- **Authentication**: OAuth tokens (encrypted in keychain)
- **FTS Catalog**: Full-text search index across all providers

### Parquet Files

Each conversation is stored as a single Parquet file with ZSTD compression:

```
conversations/{provider}/{conversation_id}.parquet

Schema:
├── conversation_id: String
├── provider_id: String
├── title: String
├── model: String (nullable)
├── project_id: String (nullable)
├── project_name: String (nullable)
├── is_archived: Boolean
├── created_at: Timestamp (UTC)
├── updated_at: Timestamp (UTC)
└── messages: List<Message>
    ├── message_id: String
    ├── parent_id: String (nullable)
    ├── role: String (user/assistant/system/tool)
    ├── content_type: String (text/code/multipart/tool_use/tool_result)
    ├── content: String
    └── created_at: Timestamp (nullable)
```

### DuckDB Queries

DuckDB queries Parquet files directly using glob patterns:

```sql
-- List all conversations from ChatGPT
SELECT * FROM read_parquet('~/.quaid/conversations/chatgpt/*.parquet');

-- Search across all providers
SELECT * FROM read_parquet('~/.quaid/conversations/*/*.parquet')
WHERE content ILIKE '%kubernetes%';
```

## Pipeline Architecture

The sync pipeline uses a three-stage design with crossbeam channels for inter-stage communication.

### Stage Diagram

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────────────┐
│   Fetch Stage    │────▶│   Media Stage    │────▶│      Embed Stage         │
│                  │     │                  │     │                          │
│ • Receive convos │     │ • Download       │     │ • Chunk messages         │
│ • From providers │     │   attachments    │     │ • Generate embeddings    │
│                  │     │ • Store to disk  │     │ • Write to Parquet       │
└──────────────────┘     └──────────────────┘     └──────────────────────────┘
    1 thread                 N threads                    M threads
         │                       │                            │
         └───────────────────────┴────────────────────────────┘
                        crossbeam bounded channels
```

### Channel Communication

```
                    channel_capacity (default: 100)
                              │
┌───────┐    ┌────────────────▼─────────────────┐    ┌───────┐
│ Fetch │───▶│  bounded channel (backpressure)  │───▶│ Media │
└───────┘    └──────────────────────────────────┘    └───────┘
                              │
┌───────┐    ┌────────────────▼─────────────────┐    ┌───────┐
│ Media │───▶│  bounded channel (backpressure)  │───▶│ Embed │
└───────┘    └──────────────────────────────────┘    └───────┘
                              │
┌───────┐    ┌────────────────▼─────────────────┐    ┌─────────┐
│ Embed │───▶│  bounded channel (results)       │───▶│ Collect │
└───────┘    └──────────────────────────────────┘    └─────────┘
```

### Worker Configuration

Workers are auto-configured based on CPU count:

```rust
PipelineConfig {
    fetch_workers: num_cpus::get(),      // CPU count
    media_workers: num_cpus::get() / 2,  // Half CPU count (I/O bound)
    embed_workers: num_cpus::get() / 2,  // Half CPU count (CPU bound)
    channel_capacity: 100,               // Bounded for backpressure
}
```

### Message Types

```rust
enum PipelineMessage {
    // Stage 1 → Stage 2
    ConversationFetched {
        account_id: String,
        conversation: Conversation,
        messages: Vec<Message>,
    },

    // Stage 2 → Stage 3
    MediaDownloaded {
        account_id: String,
        conversation: Conversation,
        messages: Vec<Message>,
        attachments: Vec<DownloadedAttachment>,
    },

    // Stage 3 → Collector
    Complete {
        conversation_id: String,
        messages_count: usize,
        chunks_count: usize,
    },

    // Error handling
    Error {
        conversation_id: String,
        stage: String,
        message: String,
    },

    // Graceful shutdown
    Shutdown,
}
```

## Embeddings Architecture

### Chunking Strategy

Long messages are split into chunks for embedding:

```
┌─────────────────────────────────────────────────────────────┐
│                    Original Message                          │
│  "This is a very long message that needs to be split..."    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│    Chunk 1      │ │    Chunk 2      │ │    Chunk 3      │
│  (~256 tokens)  │ │  (~256 tokens)  │ │  (remaining)    │
│                 │ │                 │ │                 │
│ ◀──── overlap ──▶│◀──── overlap ───▶│                 │
│   (~32 tokens)   │   (~32 tokens)   │                 │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

Configuration:
- `max_chunk_chars`: 1024 (~256 tokens)
- `overlap_chars`: 128 (~32 tokens)
- Sentence boundary detection for clean splits

### Model: multilingual-e5-small

- **Dimensions**: 384
- **Languages**: English, Spanish (100+ supported)
- **Size**: ~118MB
- **Format**: ONNX Runtime

### Embedding Levels

```
┌─────────────────────────────────────────────────────────────┐
│                     Conversation                             │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Message 1   │  │  Message 2   │  │  Message 3   │       │
│  │              │  │              │  │              │       │
│  │ ┌─────┬─────┐│  │ ┌─────┐     │  │ ┌─────┬─────┐│       │
│  │ │Chunk│Chunk││  │ │Chunk│     │  │ │Chunk│Chunk││       │
│  │ │  1  │  2  ││  │ │  1  │     │  │ │  1  │  2  ││       │
│  │ └──┬──┴──┬──┘│  │ └──┬──┘     │  │ └──┬──┴──┬──┘│       │
│  └────┼─────┼───┘  └────┼────────┘  └────┼─────┼───┘       │
│       │     │           │                │     │            │
│       ▼     ▼           ▼                ▼     ▼            │
│      [384] [384]       [384]            [384] [384]         │
│         \   /            │                 \   /             │
│          \ /             │                  \ /              │
│       mean pool      (single)            mean pool          │
│           │              │                   │               │
│          [384]         [384]               [384]             │
│            \             │                 /                 │
│             \            │                /                  │
│              ────────────┼───────────────                   │
│                          │                                   │
│                      mean pool                               │
│                          │                                   │
│                        [384]                                 │
│                  Conversation Embedding                      │
│                   (L2 normalized)                            │
└─────────────────────────────────────────────────────────────┘
```

## Sequence Diagrams

### Pull Command Flow

```
User          CLI           Pipeline        Provider        Storage
 │             │               │               │               │
 │──pull──────▶│               │               │               │
 │             │──fetch convos─▶               │               │
 │             │               │──list convos──▶               │
 │             │               │◀──conv list───│               │
 │             │               │               │               │
 │             │               │──────────────────────────────▶│
 │             │               │  write conversation.parquet   │
 │             │               │◀─────────────────────────────│
 │             │               │               │               │
 │             │◀──result──────│               │               │
 │◀──summary───│               │               │               │
 │             │               │               │               │
```

### Pipeline Concurrency Flow

```
Time ─────────────────────────────────────────────────────────────────▶

Fetch Thread (1):
├─[Conv 1]─┼─[Conv 2]─┼─[Conv 3]─┼─[Conv 4]─┼─[Conv 5]─┤

Media Thread 1:          Media Thread 2:
├───[Conv 1]───┼───[Conv 3]───┤    ├───[Conv 2]───┼───[Conv 4]───┤

Embed Thread 1:          Embed Thread 2:
    ├───[Conv 1]───┼───[Conv 3]───┤      ├───[Conv 2]───┤

                                   ▲
                                   │
                              Backpressure
                         (bounded channels wait)
```

### Backpressure Example

```
┌────────────────────────────────────────────────────────────────┐
│ Scenario: Embed stage slower than Media stage                  │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  Media Thread 1:  ████████████ (fast)                         │
│                        │                                       │
│                        ▼                                       │
│  Channel [███████████] ← FULL (capacity 100)                  │
│                        │                                       │
│                   BLOCKED                                      │
│                        │                                       │
│  Embed Thread 1:  ░░░░░░░░░░░░░░░░░░░░░ (slow)                │
│                        │                                       │
│                    processes                                   │
│                        │                                       │
│  Channel [█████████░░] ← Space freed                          │
│                        │                                       │
│  Media Thread 1:  ████ ← Unblocked, continues                 │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

## Error Handling

### Pipeline Error Propagation

```
┌──────────┐        ┌──────────┐        ┌──────────┐
│  Fetch   │───────▶│  Media   │───────▶│  Embed   │
└──────────┘        └──────────┘        └──────────┘
     │                   │                   │
     │ Error?            │ Error?            │ Error?
     ▼                   ▼                   ▼
┌──────────────────────────────────────────────────┐
│              Error Channel                        │
│  PipelineMessage::Error {                        │
│      conversation_id,                            │
│      stage: "fetch" | "media" | "embed",         │
│      message: "description",                     │
│  }                                               │
└──────────────────────────────────────────────────┘
                       │
                       ▼
              Collected in PipelineResult.errors
```

### Graceful Shutdown

```
┌──────────┐        ┌──────────┐        ┌──────────┐
│  Fetch   │───────▶│  Media   │───────▶│  Embed   │
└──────────┘        └──────────┘        └──────────┘
     │                   │                   │
  Channel               │                   │
  closes                │                   │
     │                  │                   │
     ▼                  │                   │
  for msg in rx         │                   │
  loop ends            ─┘                   │
     │                  │                   │
     │              Channel                 │
     │              closes                  │
     │                  │                   │
     │                  ▼                   │
     │              for msg in rx           │
     │              loop ends              ─┘
     │                  │                   │
     │                  │              Channel
     │                  │              closes
     │                  │                   │
     ▼                  ▼                   ▼
  Thread exits      Thread exits      Thread exits
```

## Performance Characteristics

### Storage

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Write conversation | O(n) | n = messages, sequential writes |
| Read conversation | O(1) | Direct file access |
| List all conversations | O(n) | DuckDB glob scan |
| Full-text search | O(n) | DuckDB ILIKE across files |
| Semantic search | O(k log n) | k-NN with VSS index |

### Pipeline Throughput

| Stage | Bottleneck | Typical Rate |
|-------|------------|--------------|
| Fetch | Network I/O | 10-50 conv/sec |
| Media | Network I/O | 5-20 attachments/sec |
| Embed | CPU (ONNX) | 100-500 chunks/sec |

### Memory Usage

| Component | Memory |
|-----------|--------|
| ONNX model | ~200MB |
| Per conversation | ~1-10MB |
| Channel buffers | ~50MB (100 * 500KB) |

## Search Architecture

### Vector Search (Implemented)

Semantic search uses L2 distance on embeddings stored in Parquet files:

```sql
-- Query embeddings with L2 distance
SELECT
    conversation_id,
    chunk_text,
    list_sum(list_transform(
        list_zip(embedding, $query_embedding),
        x -> power(x[1] - x[2], 2)
    )) as distance
FROM read_parquet('~/.quaid/embeddings/*/*.parquet')
ORDER BY distance
LIMIT 10;
```

Usage: `quaid search "how to deploy apps" --semantic`

### Hybrid Search (Implemented)

Combines FTS and semantic search using Reciprocal Rank Fusion (RRF):

```
Query: "kubernetes deployment"
         │
         ├──────────────────┬──────────────────┐
         │                  │                  │
         ▼                  ▼                  ▼
    ┌─────────┐       ┌──────────┐      ┌───────────┐
    │   FTS   │       │ Semantic │      │   RRF     │
    │ DuckDB  │       │ DuckDB   │      │  Fusion   │
    └────┬────┘       └────┬─────┘      └─────┬─────┘
         │                 │                  │
         ▼                 ▼                  ▼
    [doc1, doc3]     [doc2, doc3]      1/(k+rank)
         │                 │                  │
         └─────────────────┴──────────────────┘
                           │
                           ▼
                    [doc3, doc1, doc2]
                    (re-ranked by RRF score)
```

Usage: `quaid search "kubernetes" --hybrid`

## Future Enhancements

- [ ] Gemini provider support
- [ ] HNSW index for faster k-NN search at scale
- [ ] Conversation-level embeddings (mean pooled)
