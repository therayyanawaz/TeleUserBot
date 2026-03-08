# Telegram News Intelligence Userbot

A production-grade Telegram userbot for real-time conflict/news monitoring, high-signal filtering, digest publishing, and private query/search.

It runs on your **personal Telegram account** with **Telethon**, while optionally delivering output through the **Telegram Bot API** to a destination chat, channel, or group.

## Why This Project Exists

Most Telegram news feeds have the same problems:
- too much noise
- too many duplicate reposts
- too many weak alerts marked as urgent
- no clean way to ask questions against recent coverage

This project fixes that by combining:
- real-time Telegram intake
- duplicate suppression
- severity routing
- digest generation
- private search/query mode
- Codex OAuth-backed summarization

The result is a bot that can act like a private newsroom desk instead of a raw repost machine.

## Highlights

- Real-time intake from:
  - shared Telegram folder invite links (`FOLDER_INVITE_LINK`)
  - manual extra sources (`EXTRA_SOURCES`)
- Runs as a **userbot**, not a bot-token listener
- Optional **Bot API destination mode** for clean delivery to a private feed chat
- Strong duplicate suppression:
  - text duplicate detection
  - media duplicate detection
  - visual media hashing for same-image/same-video reposts
- Severity routing:
  - high-severity updates can go out immediately
  - medium/low updates go to digest
- Digest system:
  - hourly digest
  - 24-hour digest
  - optional digest pin rotation (`pinChatMessage` / `unpinChatMessage`)
- Query assistant:
  - ask questions in **Saved Messages** or your own bot PM
  - Telegram-first evidence search
  - optional trusted web fallback
- OCR translation for media-only posts:
  - image OCR
  - first-frame video OCR
  - translate only when non-English text is found
- Reply-thread preservation:
  - source reply chains can be carried into destination output
  - follow-up media can stay attached to the right story thread
- Telegram HTML output + optional premium emoji mapping
- Replit/UptimeRobot-ready status server

## Project Structure

```text
TeleUserBot/
├── main.py
├── config.py
├── auth.py
├── ai_filter.py
├── db.py
├── utils.py
├── prompts.py
├── severity_classifier.py
├── web_server.py
├── requirements.txt
├── requirements.optional.txt
├── .env.example
└── README.md
```

Runtime state is stored outside the repo in:

```text
~/.tg_userbot/
```

That includes:
- SQLite runtime DB
- OAuth state
- logs
- other runtime metadata

## Core Modes

### 1. Feed / Alert Mode

The bot listens to source channels and decides what to do with each update:
- suppress duplicate
- send immediately as breaking/high-severity
- queue for digest
- attach to an earlier related thread when appropriate

### 2. Digest Mode

Instead of dumping every post, the bot can collect recent updates and produce:
- an hourly digest
- a daily digest

You can also pin those digests automatically.

### 3. Query Mode

You can ask questions like:
- `latest tehran news`
- `what happened in last 24 hours`
- `who died recently in iran`

The bot searches recent Telegram evidence first, then optionally trusted web news if Telegram evidence is weak.

## Requirements

- Python **3.11+**
- Telegram `api_id` and `api_hash` from `my.telegram.org`
- A Telegram user account for Telethon login
- Optional bot token if you want Bot API delivery or bot-PM query mode
- Linux/macOS shell recommended

## Install

### 1. Clone

```bash
git clone https://github.com/therayyanawaz/TeleUserBot.git
cd TeleUserBot
```

### 2. Create and activate virtual environment

```bash
python3.11 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3. Install core dependencies

```bash
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

### 4. Optional extras

```bash
pip install -r requirements.optional.txt
```

This enables optional capabilities like:
- OCR dependencies (`Pillow`, `pytesseract`)
- optional `sentence-transformers` runtime if you explicitly turn it on

## System Packages

If you want OCR for media-only images or videos:

```bash
sudo apt-get update
sudo apt-get install -y tesseract-ocr ffmpeg
```

For multilingual OCR, also install language packs:

```bash
sudo apt-get install -y tesseract-ocr-ara tesseract-ocr-fas tesseract-ocr-urd tesseract-ocr-rus
```

## Configuration

Create a working config file:

```bash
cp .env.example .env
```

### Minimum required `.env`

```env
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

FOLDER_INVITE_LINK="https://t.me/addlist/xxxxxxxxxx"
# EXTRA_SOURCES=["@channel1","https://t.me/+privateInviteHash"]

# Choose one destination mode:
DESTINATION="@your_private_channel_or_chat"
# OR
# BOT_DESTINATION_TOKEN="123456:ABCDEF..."
# BOT_DESTINATION_CHAT_ID="7777826640"
```

Important:
- if both `DESTINATION` and bot destination values are set, **bot destination mode wins**
- `FOLDER_INVITE_LINK` is optional if you use `EXTRA_SOURCES`
- `BOT_DESTINATION_CHAT_ID` should be a real destination feed chat, not your bot PM, unless you intentionally want to mix them

## OpenAI / Codex Auth

This project uses **Codex-style OAuth**, not a normal API key flow.

### Server / Replit mode

Recommended for hosted deployments:

```env
OPENAI_AUTH_ENV_ONLY=true
TG_USERBOT_AUTH_JSON_B64="..."
```

Generate env auth locally:

```bash
python auth.py bootstrap-env
```

Or write it directly into `.env`:

```bash
python auth.py setup-env
```

### Local interactive mode

```env
OPENAI_AUTH_ENV_ONLY=false
```

Then log in locally with browser OAuth:

```bash
python auth.py login
```

### Auth commands

Local login:

```bash
python auth.py login
```

Env login and `.env` update:

```bash
python auth.py login --env-file .env
```

Check auth source and status:

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

## Running the Bot

Single entrypoint:

```bash
python main.py
```

The bot will:
1. validate config
2. ensure only one instance is running
3. initialize runtime DB and caches
4. validate auth
5. connect Telegram user session
6. resolve source channels/folder chats
7. start feed/query/digest pipelines
8. optionally start web status server

## Recommended Local Workflow

```bash
source .venv/bin/activate
python main.py
```

If you do not want to activate manually each time, use the venv Python directly:

```bash
./.venv/bin/python main.py
```

## Digest Configuration

Recommended baseline:

```env
DIGEST_MODE=true
DIGEST_INTERVAL_MINUTES=60
DIGEST_DAILY_TIMES=["00:00"]
DIGEST_DAILY_WINDOW_HOURS=24
DIGEST_MAX_POSTS=80
DIGEST_QUEUE_CLEAR_INTERVAL_MINUTES=0
OUTPUT_LANGUAGE="English"
```

### Digest pin rotation

```env
DIGEST_PIN_HOURLY=false
DIGEST_PIN_DAILY=false
```

When enabled:
- the newest digest of that type is pinned
- the older pinned digest of the same type is unpinned
- failure to pin does **not** block digest delivery

## OCR Translation for Media-only Posts

OCR is intentionally narrow and conservative.

Behavior:
- captioned media uses the real Telegram caption
- media-only image posts get a caption **only if** OCR finds non-English text and translation succeeds
- media-only videos use first-frame OCR only
- English OCR text is ignored
- failed OCR adds nothing
- no invented visual descriptions are generated

Config:

```env
MEDIA_TEXT_OCR_ENABLED=true
MEDIA_TEXT_OCR_VIDEO_ENABLED=true
MEDIA_TEXT_OCR_MIN_CHARS=12
MEDIA_TEXT_OCR_MAX_CHARS=1600
MEDIA_TEXT_OCR_VIDEO_MAX_MB=25
MEDIA_TEXT_OCR_LANGS=eng+ara+fas+urd+rus
```

## Duplicate Suppression

The bot uses multiple layers of duplicate defense.

### Text-level duplicate suppression
- normalized text fingerprinting
- hybrid duplicate scoring
- recent breaking cache

### Media-level duplicate suppression
- visual media hashing for same images / recompressed reposts
- album-level media signatures
- persistent dedupe memory in SQLite

### Reply-thread continuity
When a media-only follow-up is shared as a reply in the source channel, the bot can preserve that relationship in the destination feed.

## Severity Routing

High-level flow:
- `high` -> immediate alert
- `medium` / `low` -> digest queue

You can tune routing with config values in `config.py` / `.env`, including severity toggles and duplicate behavior.

## Query Assistant

The query assistant is intentionally restricted.

Allowed contexts:
- **Saved Messages**
- **private chat with your own bot account**

Not allowed:
- groups
- channels
- random private chats with other users

Typical queries:
- `latest tehran news`
- `what happened in last 24 hours`
- `recent beirut updates`
- `who died recently in iran`

## Query Web Fallback

When Telegram evidence is weak, the bot can search trusted web news sources.

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
- Telegram evidence is preferred
- web fallback is only used when Telegram evidence is too weak
- high-risk queries are treated more conservatively

## Replit / UptimeRobot

Recommended hosted settings:

```env
ENABLE_WEB_SERVER=true
WEB_SERVER_HOST="0.0.0.0"
WEB_SERVER_PORT=8080
HOLD_ON_STARTUP_ERROR=true
OPENAI_AUTH_ENV_ONLY=true
TG_USERBOT_AUTH_JSON_B64="..."
```

Suggested Replit commands:
- Build command:
  - `pip install -r requirements.txt`
- Optional extras:
  - `pip install -r requirements.optional.txt`
- Run command:
  - `python main.py`

Health endpoints:
- `/health`
- `/status`
- `/`

## Runtime Data

Stored outside the repo in:

```text
~/.tg_userbot/
```

Typical runtime files:
- DB
- token cache
- logs
- runtime metadata

## Operator Commands

Digest status command:

```text
/digest_status
```

This reports queue state, scheduler status, and runtime health details.

## Troubleshooting

### `sentence-transformers unavailable`

Not an error if you intentionally run no-HF mode.

Install optional extras only if you want that backend:

```bash
pip install -r requirements.optional.txt
```

### `database is locked`

Another process is using the same Telethon session or runtime DB.

Fix:
- stop duplicate processes
- run only one active instance

### Repeated OAuth login prompts on a server

Use env-only auth:

```env
OPENAI_AUTH_ENV_ONLY=true
TG_USERBOT_AUTH_JSON_B64="..."
```

### `PhoneNumberInvalidError`

The number format may look valid, but Telegram rejected it.
Use full E.164 format with country code.

Example:

```text
+15551234567
```

### Media-only post has no useful caption

Check:
- OCR is enabled
- Tesseract is installed
- language packs are installed
- the media actually contains readable non-English text

### Digest header count looks wrong

Latest code distinguishes:
- digest headlines published
- raw updates reviewed

### Query reply thread is wrong or missing

Use latest code and keep query mode in:
- Saved Messages
- or your own bot PM

### Bot API upload timeout / connection reset

Latest code retries transient transport errors automatically once.
If it still fails often, check:
- network stability
- VPS quality
- proxy/VPN path
- oversized media uploads

## Security

Never commit:
- `.env`
- `userbot.session*`
- `~/.tg_userbot/*` secrets
- exported auth JSON
- private token dumps

If anything sensitive was exposed, rotate it immediately.

## Upgrade Workflow

```bash
git pull
source .venv/bin/activate
pip install -r requirements.txt --upgrade
pip install -r requirements.optional.txt --upgrade  # optional
python main.py
```

## Practical Deployment Advice

Best separation:
- one chat for **feed delivery**
- one private bot PM or Saved Messages for **queries**

Do not mix high-volume feed output with your interactive query workflow unless you intentionally accept a noisier UX.

## License / Usage

Use responsibly and comply with Telegram terms, local laws, and the rules of the sources you monitor.
