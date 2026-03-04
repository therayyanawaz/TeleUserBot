# ⚡ NetworkSlutter — Telegram News Intelligence Engine

> Human-like war/OSINT intelligence feed powered by your **personal Telegram account**, strict filtering, smart dedupe, and premium-grade digest delivery.

---

## ✨ Why This Project

Most Telegram aggregators become noise cannons.
**NetworkSlutter** is built to do the opposite:

- 🧠 Keep only high-signal updates
- 🧹 Kill duplicates and paraphrased echoes
- 🚨 Send only truly critical breaking alerts immediately
- 📰 Deliver clean scheduled digests instead of spam floods
- 🔐 Keep auth/token/session handling secure and persistent

---

## 🧩 Core Features

- 👤 **Telethon userbot mode** (runs on your personal account, not a bot token)
- 📡 **Live source intake** from Telegram folder invite links (`t.me/addlist/...`) + manual extra sources
- 🖼️ **Full media support**: text, photo/video/document, albums (`grouped_id` buffering)
- ⛔ **Forward-restricted chat handling** via re-download + re-send
- 🧠 **Codex OAuth backend** via PKCE + secure token refresh (no API key in `.env`)
- 🧮 **Strict multi-factor severity engine** (explainable scoring + source cooldowns)
- 🧬 **Hybrid dedupe** (semantic + lexical + fuzzy + persistent recent memory)
- 🧵 **Breaking topic threading** (related breaking updates can reply in-thread)
- 📰 **Digest mode with transactional SQLite queue**
- ⏱️ **Schedulers**:
  - hourly digest
  - daily digest at `00:00`
  - queue clear every `10` minutes
- 💬 **Query assistant mode** (outgoing private queries in allowed peers)
- 🎬 **Streaming response UX** with incremental edits
- 🎨 **Telegram HTML formatting + premium emoji mapping support**
- 🛡️ **Robust runtime behavior**: flood waits, retries, restore on failure, graceful shutdown

---

## 🏗️ Architecture At A Glance

```text
Telegram Sources
      |
      v
NewMessage Intake (Telethon)
      |
      +--> Seen check + Hybrid dedupe
      |
      +--> Severity classifier (scored, explainable)
      |         |
      |         +--> HIGH -> immediate BREAKING delivery
      |         |
      |         +--> MEDIUM/LOW -> SQLite digest_queue
      |
      v
Digest Schedulers
  - hourly flush
  - daily 00:00 flush
  - queue clear every 10m
      |
      v
Codex summary generation (SSE)
      |
      v
Destination delivery
  - Telethon destination OR Bot API destination
```

---

## 📁 Project Layout

```text
TeleUserBot/
  main.py                # runtime, routing, schedulers, event handlers
  config.py              # env loader + typed config defaults
  auth.py                # OAuth PKCE token flow + refresh
  ai_filter.py           # Codex calls, streaming, summary generation
  severity_classifier.py # strict multi-factor severity scoring
  db.py                  # SQLite seen + queue + metadata ops
  utils.py               # dedupe, formatting, helpers
  prompts.py             # prompt templates
  .env.example           # full config template
  requirements.txt
  README.md
```

Runtime data is stored outside the repo:

- `~/.tg_userbot/auth.json` (OAuth tokens)
- `~/.tg_userbot/seen.db` (queue + seen + runtime meta)
- `~/.tg_userbot/errors.log` (error traces)

---

## 🚀 Quick Start

### 1) Prerequisites

- Python `3.11+`
- Telegram account
- Telegram API credentials (`api_id`, `api_hash`) from `https://my.telegram.org`

### 2) Install

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional for stronger semantic dedupe:

```bash
pip install sentence-transformers
```

### 3) Configure

```bash
cp .env.example .env
```

Edit `.env` with your values.

### 4) Run

```bash
python main.py
```

On first run, the bot will:

1. Prompt for missing required config values
2. Launch OAuth browser login (PKCE)
3. Store token securely in `~/.tg_userbot/auth.json`
4. Start user session and source listeners
5. Start schedulers (digest + daily + queue clear)

---

## 🔧 Minimum Required `.env`

Set at least:

- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- source strategy:
  - `FOLDER_INVITE_LINK` **or** `EXTRA_SOURCES`
- destination strategy:
  - `DESTINATION` (Telethon mode) **or**
  - `BOT_DESTINATION_TOKEN` + `BOT_DESTINATION_CHAT_ID` (Bot API mode)

---

## ⏰ Scheduling Defaults (Current)

- `DIGEST_MODE=true`
- `DIGEST_INTERVAL_MINUTES=60` → hourly digest
- `DIGEST_DAILY_TIMES=["00:00"]` → daily digest at midnight
- `DIGEST_QUEUE_CLEAR_INTERVAL_MINUTES=10` → queue cleanup every 10 minutes
- `DIGEST_QUEUE_CLEAR_INCLUDE_INFLIGHT=true`

Important: frequent queue clearing reduces digest volume. Tune this based on how much history you want in digests.

---

## 🧠 Severity System (High-Signal First)

The severity engine is now deterministic + explainable:

- source tier weighting
- urgency phrase/emoji signals
- style/format behavior
- contextual/temporal cues
- negative penalties (recap/analysis/thread/long-form)
- hard guards + per-source high-rate cooldown

This sharply reduces false “breaking” floods and pushes normal updates into digest.

---

## 🧬 Dedupe System

Duplicate killer combines:

- semantic similarity (if `sentence-transformers` available)
- TF-based lexical similarity
- fuzzy matching
- persistent recent-breaking memory

Result: near-duplicate reposts are suppressed aggressively.

---

## 📤 Destination Modes

### Mode A: Telethon Destination

Set:

- `DESTINATION=@your_channel_or_chat`

### Mode B: Bot API Destination

Set:

- `BOT_DESTINATION_TOKEN=...`
- `BOT_DESTINATION_CHAT_ID=...`

Bot mode includes API retry/backoff logic and caption/media fallbacks.

---

## 💬 Query Assistant Mode

When enabled, outgoing private queries can trigger AI answers from your monitored source context.

Key controls:

- `QUERY_MODE_ENABLED=true`
- `QUERY_MAX_MESSAGES=50`
- `QUERY_DEFAULT_HOURS_BACK=24`
- `QUERY_ALLOWED_PEER_IDS=[]`

---

## 🎨 HTML + Premium Emoji Rendering

Controls:

- `ENABLE_HTML_FORMATTING=true`
- `ENABLE_PREMIUM_EMOJI=true`
- `PREMIUM_EMOJI_MAP_FILE="nezami_emoji_map.json"`

Custom emoji map file is intentionally gitignored by default in production setups.

---

## 🧪 Operational Commands

Use from your account:

- `/digest_status`

Returns mode, queue stats, next run ETA, quota health, dedupe/severity state, and scheduler info.

---

## 🛡️ Reliability & Failure Handling

Built-in protections include:

- Flood wait handling with sleep/retry
- Restricted-forward fallback via media re-send
- Digest claim/ack/restore transactions
- Exponential retry backoff on digest failure
- Structured event logging
- Graceful Ctrl+C shutdown of all running tasks

---

## 🔐 Security Practices

- Never commit `.env`, session files, or OAuth tokens
- Keep `.env` local and private
- Rotate any accidentally exposed credentials immediately
- Restrict destination bot permissions to minimum required

Gitignored by default:

- `.env`
- `userbot.session*`
- `nezami_emoji_map.json`
- runtime state directories

---

## 🧯 Troubleshooting

### OAuth login opens repeatedly

- stop all old bot processes
- retry once
- if needed, remove `~/.tg_userbot/auth.json` and re-login

### `sentence-transformers unavailable`

Non-fatal. Bot falls back to lexical/fuzzy dedupe.

### Session DB locked

Another bot instance is running. Stop it, then restart.

### No sources resolved

Check folder invite access or add manual entries in `EXTRA_SOURCES`.

### Destination errors

Verify bot token/chat ID or Telethon destination permissions.

---

## ⚙️ Production Run Tips

- Run inside `tmux`, `screen`, or `systemd`
- Keep one process only per session DB
- Check `/digest_status` after deploys
- Monitor `~/.tg_userbot/errors.log`
- Tune severity + schedule based on your signal tolerance

---

## 📌 Disclaimer

Use responsibly and follow Telegram/platform/local regulations.
This project is for personal automation and intelligence monitoring workflows.

---

## 💥 Final Note

If your feed still feels noisy, do not loosen dedupe.
Tighten severity thresholds first, then adjust source tiers.
Signal quality beats volume. Every time.
