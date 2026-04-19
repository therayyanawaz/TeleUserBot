# TeleUserBot - Copilot Instructions

> AI-friendly workspace guidance for the TeleUserBot development environment.

**Project**: Real-time Telegram news aggregator with duplicate suppression, breaking story routing, digest generation, and private query assistance.  
**Language**: Python 3.11+  
**Key Dependencies**: Telethon (Telegram), Codex OAuth (AI), SQLite (persistence)

---

## 🚀 Development Setup & Commands

### Virtual Environment

```bash
# Windows PowerShell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1

# macOS/Linux
python3 -m venv .venv
source .venv/bin/activate
```

### Install Dependencies

```bash
# Core dependencies
pip install -r requirements.txt

# Development & testing
pip install -r requirements-dev.txt
```

### Run & Test

```bash
# Start the main bot
python main.py

# Run test suite
pytest                      # All tests
pytest -xvs tests/         # Verbose with stops on first failure
pytest tests/test_db_*.py  # Specific test module

# With asyncio support
pytest -p pytest_asyncio --asyncio-mode=auto
```

### Quick Test of Specific Functionality

```bash
# Test configuration loading
python -c "import config; print(config.TELEGRAM_API_ID)"

# Inspect database schema and state
python -c "import db; db.init_db()"
```

---

## 🏗️ Architecture & Component Boundaries

### Core Pipeline

```
Telegram Input
  ↓
[Triage] - Duplicate detection
  ↓
[AI Decision] - Codex severity classification, categorization, filtering
  ↓
[Routing]
  ├→ High severity: Immediate delivery (breaking news)
  ├→ Medium: Queue for hourly/daily digest
  └→ Low/Skip: Archive/skip
  ↓
[Digest] - Summarization, rendering, formatting
  ↓
[Delivery] - Telegram HTML output to destination
```

### Key Modules

| Module | Purpose | Key Classes/Functions |
|--------|---------|----------------------|
| **main.py** | Entry point, Telegram client lifecycle, event handlers | `TelegramClient`, `_process_message()`, digest scheduling |
| **config.py** | Environment configuration without external deps | `_env_str()`, `_env_int()`, `_env_bool()`, `_env_list()` |
| **auth.py** | OpenAI OAuth PKCE flow, token refresh, env bootstrap | `AuthManager`, `ensure_runtime_dir()`, `write_auth_payload_to_env_file()` |
| **ai_filter.py** | Codex classification, caching, fallback logic | `decide_filter_action()`, `create_digest_summary()`, `FilterDecision` dataclass |
| **db.py** | SQLite persistence: deduplication, digest queue, decision cache | `enqueue_inbound_job()`, `ai_decision_cache_get/set()` |
| **breaking_story.py** | Breaking news clustering, topic extraction, context evidence | `build_breaking_story_candidate()`, `resolve_breaking_story_cluster()` |
| **news_taxonomy.py** | News category matching and taxonomy versioning | `match_news_category()`, `get_news_taxonomy()` |
| **severity_classifier.py** | Severity scoring (high/medium/low) and history tracking | `classify_message_severity()` |
| **utils.py** | Utilities: deduplication fingerprints, HTML sanitization | `build_dupe_fingerprint()`, `sanitize_telegram_html()` |
| **web_server.py** | Optional health/status HTTP endpoint | `/health`, `/status` |

### State Location

Runtime state lives **outside the repo** in `~/.tg_userbot/`:
- `seen.db` - SQLite deduplication and queue
- `auth.json` - OAuth token payload  
- `auth.env-cache.json` - Cached token for env-only mode
- `errors.log` - Application errors

---

## 🧪 Testing & Quality

### Test Organization

```
tests/
├── conftest.py                    # Shared fixtures (isolated_db, monkeypatch)
├── test_main_auth_startup.py     # Auth bootstrap and failure modes  
├── test_auth_manager.py          # OAuth token lifecycle
├── test_db_pipeline.py           # Job queue and cache operations
├── test_ai_filter_decisions.py   # Codex integration and caching
├── test_breaking_story_clusters.py  # Topic clustering and deduplication
├── test_breaking_voice.py        # Breaking news formatting
├── test_news_taxonomy.py         # Category matching and precedence
└── test_feed_quality.py          # Severity classification
```

### Key Test Patterns

**Isolated Database**: Use the `isolated_db` fixture to avoid test pollution:
```python
def test_something(isolated_db):
    db.enqueue_inbound_job(...)  # Uses isolated tmp database
    assert db.load_inbound_jobs(...) == expected
```

**Async Tests**: Mark with `@pytest.mark.asyncio`:
```python
@pytest.mark.asyncio
async def test_auth_flow():
    manager = AuthManager()
    token = await manager.get_access_token()
```

**Monkeypatching**: Mock config, functions, and modules:
```python
def test_with_config(monkeypatch):
    monkeypatch.setattr(config, "DIGEST_MODE", True)
    monkeypatch.setattr(main, "_include_source_tags", lambda: False)
```

---

## 🔑 Key Conventions & Patterns

### Configuration Loading

- **No external dependencies**: `config.py` uses only stdlib (`ast`, `json`, `os`)
- **Environment precedence**: Shell/process env vars override `.env` file
- **Type-specific loaders**: `_env_str()`, `_env_int()`, `_env_bool()`, `_env_list()`
- **List parsing**: Supports JSON, Python literal, and comma-separated formats

```python
from config import DIGEST_INTERVAL_MINUTES, SOURCES, ENABLE_WEB_SERVER
```

### Database Transactions

Always use context manager for safe transactions:
```python
from db import _transaction

with _transaction() as conn:
    conn.execute("INSERT INTO ...", ...)
    # Implicit commit on success, rollback on exception
```

### Telegram HTML Output

- Use `<b>`, `<i>`, `<u>`, `<code>` tags only (safe for Telegram)
- Sanitize user input with `sanitize_telegram_html()`
- Strip HTML with `ai_filter.strip_telegram_html()` for plain text
- **Never include**: Raw angle brackets, scripts, inline styles

```python
safe = sanitize_telegram_html(user_text)
html = f"<b>{safe}</b><br><br>Content here"
```

### Codex API Integration

- Calls go through `ai_filter._call_codex()` (handles streaming, retries, errors)
- Responses are JSON stringified dicts with required keys: `action`, `severity`, `summary_html`
- **Fallback**: If API fails, use `_fallback_filter_decision()` (heuristic scoring)
- **Caching**: Decisions cached by normalized text hash + prompt version + model

```python
decision = await ai_filter.decide_filter_action(text, auth_manager)
# Returns: FilterDecision(action, severity, summary_html, confidence, ...)
```

### Breaking News Topics

Topics are extracted from headlines using location and target patterns:
- **Location patterns**: "in CITY", "near LOCATION", "over PLACE"
- **Target patterns**: "hit OBJECT", "struck TARGET", "impact on THING"
- Topics become the cluster key for grouping related breaking updates

```python
candidate = build_breaking_story_candidate(text, headline, timestamp=now)
cluster_key = compute_breaking_story_cluster_key(candidate)
```

### Async/Await Patterns

- Main event loop uses Telethon's `TelegramClient.run()`
- Background operations (OAuth refresh) use `asyncio.create_task()`
- All Codex calls are async (`await _call_codex()`)
- Tests use `@pytest.mark.asyncio` with `pytest-asyncio`

---

## ⚠️ Common Pitfalls & Edge Cases

### Memory/Resource Management

**Low-RAM Deployments** (2GB VPS):
- Keep `DUPE_USE_SENTENCE_TRANSFORMERS=false` to avoid ~500MB model load

**SQLite WAL Mode**:
- Database uses `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL`
- This allows concurrent reads during writes, but requires both `-wal` and `-shm` files
- Clean shutdown is important; `pragma checkpoint` on application exit

### OAuth Token Handling

- **Token refresh**: Happens automatically before expiry in `auth.py`
- **Env-only mode** (`OPENAI_AUTH_ENV_ONLY=true`): Requires valid token in `TG_USERBOT_AUTH_JSON` or `TG_USERBOT_AUTH_JSON_B64`
- **Stale token error**: If refresh fails, user must run `python auth.py login --env-file .env`
- **Bootstrap in cloud** (Replit): Use `python auth.py bootstrap-env` to generate env payload

### Duplicate Detection

- Fingerprint = hash of normalized text (whitespace collapsed, lowercase)
- **Media hashing**: PIL images get SHA256 hash; re-compressed versions detected via visual hashing
- **Album deduplication**: Message IDs grouped by parent album ID
- **Time window**: `DUPE_HISTORY_HOURS=4` (configurable), older msgs not checked
- **Merge option**: `DUPE_MERGE_INSTEAD_OF_SKIP=true` can combine related messages

### Filter Decision Validation

- Invalid responses from Codex fall back to heuristic scoring
- HTML is sanitized; any `<script>`, `<div>`, `<style>` stripped
- Confidence clamped to [0.0, 1.0]
- Severity mapped to {high, medium, low}
- Topic keys normalized: spaces→underscores, lowercase only

---

## 🔍 Important Files to Review

- [README.md](../../README.md) — Feature overview, setup instructions, architecture overview
- [.env.example](../../.env.example) — All configurable environment variables with defaults
- [news_taxonomy.json](../../news_taxonomy.json) — Category definitions, phrases, severity biases
- [requirements.txt](../../requirements.txt) — Core dependencies (Telethon, httpx, rapidfuzz)
- [conftest.py](../../tests/conftest.py) — Test fixtures (especially `isolated_db`)

---

## 🎯 When Working on Features

### Adding a New Configuration Option

1. Add to [.env.example](../../.env.example) with comment explaining purpose
2. Add loader in [config.py](../../config.py) using `_env_*` helper
3. Reference in [README.md](../../README.md) if user-facing
4. Use in relevant module (e.g., `from config import NEW_OPTION`)

### Adding a News Taxonomy Category

1. Edit [news_taxonomy.json](../../news_taxonomy.json)
2. Follow existing structure: key, label, domain, severity_bias, phrases
3. Run existing tests to validate:
   ```bash
   pytest tests/test_news_taxonomy.py -xvs
   ```
4. Add test case in `test_each_category_has_positive_and_negative_fixture`

### Fixing a Duplicate Detection Issue

1. Check fingerprinting logic in `utils.build_dupe_fingerprint()`
2. Review cache hit/miss in `db.ai_decision_cache_*`
3. Test with `pytest tests/test_db_pipeline.py -xvs`
4. Consider media hashing (PIL) or visual similarity if needed

### Debugging Codex API Failures

1. Check auth token: `python -c "from auth import AuthManager; await AuthManager().get_access_token()"`
2. Verify URL/model in config: `CODEX_BASE_URL`, `CODEX_MODEL`
3. Review Codex response in error logs: `~/.tg_userbot/errors.log`
4. Test fallback heuristic: `ai_filter._fallback_filter_decision(text)`

---

## 📝 Git & Commit Guidelines

- Commit messages should reference the module changed: `auth: fix token refresh`, `db: add job retry`
- Tests must pass before committing: `pytest -xvs`
- Keep `.venv`, `__pycache__`, `.env`, auth tokens out of git (`.gitignore` enforced)
- Runtime state (`~/.tg_userbot/`) is always local; never committed

---

## 🤝 Communication with AI Agents

**When asking for changes:**
1. Specify the module(s) affected: `main.py`, `ai_filter.py`, etc.
2. Reference test file if adding tests: `test_db_pipeline.py`
3. Include environment context: config values, async/sync scope, error scenarios
4. Link related issues or adjacent code if architectural

**When reporting bugs:**
1. Include steps to reproduce
2. Share error logs from `~/.tg_userbot/errors.log`
3. Mention the relevant module (auth, database, Telegram, AI filtering, etc.)
4. Include version of Python and key dependencies

---

## ✨ Quick Reference

| Task | Command |
|------|---------|
| Setup | `py -3 -m venv .venv && .\.venv\Scripts\Activate.ps1 && pip install -r requirements.txt` |
| Test | `pytest -xvs` |
| Start bot | `python main.py` |
| Check config | `python -c "import config; print(config.TELEGRAM_API_ID)"` |
| View logs | `cat ~/.tg_userbot/errors.log` (macOS/Linux) or `type %USERPROFILE%\.tg_userbot\errors.log` (Windows) |
| Rebuild DB | `rm ~/.tg_userbot/seen.db && python main.py` |
| Auth bootstrap | `python auth.py bootstrap-env --env-file .env` |
