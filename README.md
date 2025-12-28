# quaid

![quaid](assets/quaid-logo.png)

*"Get your ass to Mars!"* I mean, **get your chats back!**

Export and backup your AI conversations locally.

## Features

- **ChatGPT sync** — Pull all conversations via browser auth
- **Claude sync** — Pull all conversations via browser auth
- **Fathom sync** — Meeting transcripts via API key
- **Granola sync** — Meeting notes from local app
- **Attachment downloads** — Images and files from ChatGPT and Claude
- **Incremental sync** — Only pull new/updated conversations with `--new-only`
- **Parallel pipeline** — Multi-threaded sync with configurable worker pools
- **Parquet storage** — Columnar format for efficient querying with DuckDB
- **Semantic search** — ONNX-powered embeddings (multilingual-e5-small)
- **Full-text search** — SQLite FTS across all providers
- **Secure credentials** — Tokens stored in system keychain
- **Export** — JSONL, JSON, or Markdown formats

## Install

```bash
cargo install --path .
```

## Usage

```bash
# Authenticate (opens browser)
quaid chatgpt auth
quaid claude auth
quaid fathom auth
quaid granola auth

# Pull conversations from all providers
quaid pull

# Pull from specific provider
quaid chatgpt pull

# Pull only new/updated conversations
quaid pull --new-only

# Search across all chats
quaid search "kubernetes deployment"

# Export to file
quaid export backup.jsonl --format jsonl

# View stats
quaid stats
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed system design, pipeline diagrams, and concurrency model.

## Coming Soon

- [ ] Gemini support
- [ ] Semantic search CLI (`quaid search --semantic`)
- [ ] Vector similarity with DuckDB VSS extension

## License

MIT
