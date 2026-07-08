# Changelog

> Auto-generated from git history. Prefer conventional commits.

## v7.8 — 2026-07-08

> **Note:** The original repository was taken down. This is a fresh reupload with the same core functionality.

### Features

- `feat` Add dynamic emoji prefixes to digest titles based on content (🚨 strikes, 🛡️ defense, 🕊️ diplomacy, etc.)
- `feat` Enhance digest prompts with premium style examples and quality checklist
- `feat` Add time-of-day aware quiet period messaging
- `feat` Always initialize auth manager (fixes Groq/OpenRouter crash)

### Fixes

- `fix` Auth manager not initialized error when using Groq or OpenRouter LLM provider

## 2026-07-04

### Fixes

- `c074197` fix: narrow suppress(Exception) to specific exception types
- `7062432` fix: handle HTTP errors in RSS news fetch instead of unhandled crash
- `8be495c` fix: log channel entity resolution failures instead of silent skip
- `071c776` fix: log health status DB failures instead of silent fallback
- `b226473` fix: close lock file handle on metadata write failure
- `a4445d1` fix: log when runtime event view build fails instead of silent drop
- `a85a62c` fix: log webhook parse failures and safe env access

### Features

- `85fb717` feat: rich terminal UI with panels, spinners, and styled output (9/n)
- `4de63c7` feat: configurable HTTP timeouts via env vars (6/n)
- `94a14e0` feat: add LLM circuit breaker with automatic fallback to local NLP (4/n)

### Other

- `4d337b3` clean: remove unused imports from main.py (8/n)
- `4e90746` clean: remove dead functions from main.py (7/n)

### Refactors

- `92007e7` refactor: split main() into 5 orchestration phases with RuntimeState (5/n)

### Chores

- `69a2fa4` chore: graceful SIGTERM/SIGINT shutdown, narrow bare except handlers (3/n)
- `69ba96c` chore: initial commit - TeleUserBot with Groq LLM provider

### Performance

- `1ef0d5f` perf: add HTTP connection pool limits (shared_http.py)
