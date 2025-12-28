# quaid

<p align="center">
  <img src="assets/quaid-logo.png" alt="quaid" width="200">
</p>

**Get your chats back.** Export and backup your AI conversations locally.

## Features

- **ChatGPT sync** — Pull all conversations via browser auth
- **Claude sync** — Pull all conversations via browser auth
- **Local SQLite storage** — Full-text search across all providers
- **Secure credentials** — Tokens stored in system keychain

## Install

```bash
cargo install --path .
```

## Usage

```bash
# Authenticate (opens browser)
quaid auth chatgpt
quaid auth claude

# Sync conversations
quaid pull chatgpt
quaid pull claude
quaid pull --all

# Search across all chats
quaid search "kubernetes deployment"

# View stats
quaid stats
```

## Coming Soon

- [ ] Gemini support
- [ ] Markdown/JSON export
- [ ] Attachment downloads
- [ ] Incremental sync

## License

MIT
