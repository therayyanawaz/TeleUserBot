# Telegram News Intelligence Userbot

Production-ready Telegram userbot for:
- multi-channel intake
- strict duplicate suppression
- severity-aware routing
- hourly/daily digest delivery
- private query assistant mode

Runs on your **Telegram user account** (Telethon), with optional Bot API destination delivery.

## Features

- Real-time intake from shared folder (`FOLDER_INVITE_LINK`) + extra sources
- Media-safe processing: text, captioned media, media-only posts, albums
- OCR translation for media-only posts:
  - images: only OCR text, only translated when non-English
  - videos: best-effort first-frame OCR translation
  - no visual descriptions or invented captions
- Hybrid duplicate suppression:
  - no-HF semantic proxy (token/bigram/chargram/anchor overlap)
  - TF-IDF cosine
  - rapidfuzz lexical similarity
- Severity routing:
  - high => immediate alert
  - medium/low => queued for digest
- Breaking follow-up threading:
  - similar breaking updates reply under prior same-topic alert
  - duplicate echoes are merged/suppressed
- Digest system:
  - interval digest (default 60 min)
  - daily digest at configurable times (default `00:00`)
  - queue clearing disabled by default (preserve full flow)
- Codex OAuth backend integration (no API key required)
- Query assistant in private contexts
- Query web fallback (news RSS) when channel evidence is weak
- Telegram HTML formatting + optional premium emoji mapping
- Health web server (`/health`, `/status`, `/`) for Replit/UptimeRobot

## Project Layout

```text
TeleUserBot/
  main.py
  config.py
  auth.py
  ai_filter.py
  db.py
  utils.py
  prompts.py
  severity_classifier.py
  web_server.py
  .env.example
  requirements.txt
```

Runtime data is stored in `~/.tg_userbot/` (SQLite, tokens, logs).

## Requirements

- Python 3.11+
- Telegram API credentials (`api_id`, `api_hash`) from my.telegram.org
- Linux/macOS shell (Windows PowerShell also supported)

Core dependencies are in `requirements.txt`:
- `telethon`
- `httpx`
- `rapidfuzz`

Optional quality/accuracy dependencies are in `requirements.optional.txt`:
- `sentence-transformers` (optional; disabled by default via `DUPE_USE_SENTENCE_TRANSFORMERS=false`)
- `Pillow` and `pytesseract` (optional OCR for image/video frame text)

## Quick Start (Local)

1. Clone and enter project:

```bash
git clone <your-repo-url>
cd TeleUserBot
```

2. Create virtual environment:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
```

3. Upgrade tooling and install dependencies:

```bash
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

Optional extras:

```bash
pip install -r requirements.optional.txt
```

If you want OCR translation for media-only posts, also install system packages:

```bash
sudo apt-get update
sudo apt-get install -y tesseract-ocr ffmpeg
```

4. Configure environment:

```bash
cp .env.example .env
```

Fill `.env` with your values (minimum set shown below).

5. Run:

```bash
python main.py
```

## Minimum `.env` Required

You must set all of the following before startup:

```env
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# Source config: use folder link OR extra sources
FOLDER_INVITE_LINK="https://t.me/addlist/xxxxxxxxxx"
# EXTRA_SOURCES=["@channel1","https://t.me/+privateInviteHash"]

# Destination: choose one mode
# 1) Telethon destination
DESTINATION="@your_private_channel_or_chat"

# OR 2) Bot API destination
# BOT_DESTINATION_TOKEN="123456:ABCDEF..."
# BOT_DESTINATION_CHAT_ID="7777826640"
```

If both bot destination and Telethon destination are set, bot mode wins.

## OpenAI / Codex Auth

Two supported modes:

### Mode A: Env-only (recommended for servers/Replit)

Set:

```env
OPENAI_AUTH_ENV_ONLY=true
TG_USERBOT_AUTH_JSON_B64="..."
```

Generate these from a local machine with browser access:

```bash
python auth.py bootstrap-env
```

or auto-write to `.env`:

```bash
python auth.py setup-env
```

### Mode B: Interactive browser OAuth (local)

Set:

```env
OPENAI_AUTH_ENV_ONLY=false
```

First run opens browser, completes PKCE login, stores token in runtime dir.

### Auth lifecycle commands

Local login to `~/.tg_userbot/auth.json`:

```bash
python auth.py login
```

Env-mode login and `.env` update:

```bash
python auth.py login --env-file .env
```

Show current auth source/status:

```bash
python auth.py status
```

Logout local auth:

```bash
python auth.py logout
```

Logout and clear `.env` secrets too:

```bash
python auth.py logout --env-file .env
```

## Media OCR Translation

Media OCR is intentionally narrow:
- captioned media keeps using the real Telegram caption text
- media-only images get a caption only when OCR finds non-English text and translation succeeds
- media-only videos get a caption only when first-frame OCR finds non-English text and translation succeeds
- English OCR text is ignored
- failed OCR/translation adds nothing

Config:

```env
MEDIA_TEXT_OCR_ENABLED=true
MEDIA_TEXT_OCR_VIDEO_ENABLED=true
MEDIA_TEXT_OCR_MIN_CHARS=12
MEDIA_TEXT_OCR_MAX_CHARS=1600
MEDIA_TEXT_OCR_VIDEO_MAX_MB=25
```

## Recommended Digest Setup

```env
DIGEST_MODE=true
DIGEST_INTERVAL_MINUTES=60
DIGEST_DAILY_TIMES=["00:00"]
DIGEST_DAILY_WINDOW_HOURS=24
DIGEST_QUEUE_CLEAR_INTERVAL_MINUTES=0
OUTPUT_LANGUAGE="English"
DUPE_USE_SENTENCE_TRANSFORMERS=false
```

## Query Web Fallback (News-only)

When query mode finds too little Telegram evidence, bot can fetch recent web-news
snippets and answer with cited links.

```env
QUERY_WEB_FALLBACK_ENABLED=true
QUERY_WEB_MIN_TELEGRAM_RESULTS=3
QUERY_WEB_MAX_RESULTS=12
QUERY_WEB_MAX_HOURS_BACK=24
QUERY_WEB_REQUIRE_RECENT=true
QUERY_WEB_REQUIRE_MIN_SOURCES=2
QUERY_WEB_ALLOWED_DOMAINS=["reuters.com","apnews.com","bbc.com","aljazeera.com","cnn.com","nytimes.com","washingtonpost.com","bloomberg.com","ft.com","theguardian.com","dw.com","france24.com","aa.com.tr","npr.org"]
```

Notes:
- Telegram data is always preferred.
- Web fallback is only used when Telegram hits are below threshold.
- If web source diversity is too low, fallback evidence is discarded.
- High-risk queries (leadership/death/succession/nuclear) are auto cross-checked and require multi-source evidence.

Queue clearing is disabled in runtime to preserve all queued updates for digest processing.

## Replit / UptimeRobot Setup

Use:
- Build command: `pip install -r requirements.txt`
- Run command: `python main.py`
- Port: `8080`

If you want semantic dedupe in Replit, add this to build command too:
- `pip install -r requirements.optional.txt`

Set:

```env
ENABLE_WEB_SERVER=true
WEB_SERVER_HOST="0.0.0.0"
WEB_SERVER_PORT=8080
HOLD_ON_STARTUP_ERROR=true
OPENAI_AUTH_ENV_ONLY=true
TG_USERBOT_AUTH_JSON_B64="..."
```

Health endpoints:
- `/health`
- `/status`
- `/`

## Operational Command

Send this from your allowed query context:

```text
/digest_status
```

Shows mode, queue counts, next run ETA, quota state, and feature flags.

## Troubleshooting

### 1) `sentence-transformers unavailable`

Not an error in no-HF mode. If you explicitly want sentence-transformers:

```bash
source .venv/bin/activate
pip install -r requirements.optional.txt
```

### 2) `database is locked` (Telethon session)

Another process is using the same `userbot.session`.
Stop duplicate process and run only one instance.

### 3) Repeated OAuth login prompts on server

Use env-only auth (`OPENAI_AUTH_ENV_ONLY=true`) and set `TG_USERBOT_AUTH_JSON` or `_B64`.

### 4) No digest output

Check:
- source resolution succeeded
- destination is valid
- pending queue increases
- digest scheduler task is running

### 5) Bot API `reply_to_message_id not found`

Use latest code; query sender now retries without reply threading when needed.

### 6) Only media-only posts arrive, text breaking alerts are missing

Check:
- `ENABLE_SEVERITY_ROUTING=true`
- `IMMEDIATE_HIGH=true`
- `BREAKING_NEWS_KEYWORDS` includes urgent terms used by your sources
- bot is running long enough for hourly digest if alerts are classified medium/low

## Security Checklist

Never commit:
- `.env`
- `userbot.session*`
- `~/.tg_userbot/*` secrets
- private token dumps

Rotate credentials immediately if exposed.

## Upgrade Workflow

When pulling updates:

```bash
git pull
source .venv/bin/activate
pip install -r requirements.txt --upgrade
pip install -r requirements.optional.txt --upgrade  # optional
python main.py
```

## License / Usage

Use responsibly and comply with Telegram terms and local laws.
