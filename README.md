# ✨ TeleUserBot

> 🛰️ A newsroom-grade Telegram userbot for real-time news monitoring, duplicate suppression, breaking alert routing, smart digests, and private evidence-first queries.

TeleUserBot turns a noisy pile of Telegram channels into a cleaner, sharper, more useful intelligence feed.

It runs on your **personal Telegram account** with **Telethon**, listens to channels and folder feeds in real time, filters weak or repeated posts, pushes urgent developments fast, rolls everything else into polished digests, and lets you ask private questions against recent coverage.

If you want something that feels closer to a private monitoring desk than a repost bot, this is what the project is built for. 💎

## 🌍 Why TeleUserBot Exists

Most Telegram monitoring setups break in the same places:

- 🔁 the same update gets reposted everywhere
- 🚨 weak signals get dressed up as breaking news
- 🧱 raw channel dumps are hard to read at scale
- 🔎 searching recent coverage inside Telegram is painful
- 🧠 media-only posts often carry important text that never gets surfaced

TeleUserBot fixes that by combining:

- real-time Telegram intake
- multi-layer duplicate suppression
- severity-aware routing
- breaking-story continuity
- hourly and daily digest generation
- OCR translation for media-only posts
- private query mode with Telegram-first evidence search
- mandatory trusted web cross-check for query answers
- Telegram HTML output with optional premium emoji rendering

## 🧠 Core Capabilities

### ⚡ Real-Time Intake

- Listen from a shared Telegram folder invite via `FOLDER_INVITE_LINK`
- Add manual sources through `EXTRA_SOURCES`
- Run as a real **userbot**, not only a bot-token listener
- Deliver output to a user destination or through the Telegram Bot API

### 🛡️ Strong Duplicate Defense

- Text fingerprinting and hybrid duplicate scoring
- Media signature checks for reposted images and albums
- Visual media hashing for same-image or recompressed media
- SQLite-backed memory so duplicate defense survives restarts

### 🚨 Breaking News Routing

- High-severity posts can go out immediately
- Medium and low priority updates can be queued for digest
- Breaking follow-ups can stay attached to the same evolving story
- Optional opinionated breaking style via `BREAKING_STYLE_MODE`

### 📰 Digest Publishing

- 30-minute rolling digest mode
- Daily 24-hour digest mode
- SQLite-backed queue/archive storage with restart-safe window claiming
- Multi-message digest delivery when one Telegram message is not enough
- Optional pin rotation for latest digest posts

### 🔎 Private Query Assistant

- Ask questions in **Saved Messages**
- Or use a **private chat with your own bot**
- Search recent Telegram evidence first
- Always cross-check against trusted web coverage before answering

### 🕒 Stable Timezone

- Set one runtime timezone in `.env` so `today`, `yesterday`, daily digests, and other local-time logic stay stable across restarts and servers
- Use an IANA timezone name such as `Asia/Kolkata` for IST

### 🖼️ OCR for Media-Only Posts

- Image OCR for posts without captions
- First-frame video OCR for media-only videos
- Translation only when non-English text is detected
- No invented visual descriptions, no fake captions

### 🎨 Clean Telegram Output

- Telegram HTML formatting
- Optional premium emoji mapping
- Reply-thread continuity when source posts are part of a thread
- Delivery tuned for feed readability instead of channel spam

## 🏗️ Project Structure

```text
TeleUserBot/
├── main.py
├── config.py
├── auth.py
├── ai_filter.py
├── breaking_story.py
├── db.py
├── news_signals.py
├── news_taxonomy.py
├── severity_classifier.py
├── utils.py
├── web_server.py
├── tests/
├── install-all.ps1
├── install-all-ubuntu.sh
├── .env.example
└── README.md
```

Runtime state lives outside the repo in:

```text
~/.tg_userbot/
```

That directory stores runtime metadata such as:

- SQLite state
- auth payloads and caches
- logs
- delivery and pipeline metadata

### Runtime Logs

- `~/.tg_userbot/runtime.log` captures full runtime activity at `DEBUG` as a readable operator transcript, including third-party library logs.
- `~/.tg_userbot/errors.log` keeps the error-only stream in the same readable format for faster triage.
- Both log files are wiped on every fresh `python main.py` start.
- During a single run, each file rotates at `10 MB` with up to `5` files kept.
- Startup, shutdown, worker lifecycle, and low-memory transitions emit structured `memory_snapshot` log events with process memory usage fields.

## 🧭 How It Works

1. TeleUserBot connects to your Telegram account.
2. It resolves sources from your shared folder and extra channels.
3. Incoming posts pass through duplicate, OCR, and severity logic.
4. High-signal updates can be delivered instantly.
5. Everything else is organized into digest workflows and searchable history.

## ✅ Requirements

- Python **3.11+**
- Latest available Python 3 release is preferred
- Telegram `api_id` and `api_hash` from `https://my.telegram.org`
- A Telegram account for Telethon login
- Optional bot token for Bot API delivery or bot-PM query mode
- Optional OCR system packages if you want image/video text extraction

## 🚀 Quick Start

### 1. Clone the repo

```bash
git clone https://github.com/therayyanawaz/TeleUserBot.git
cd TeleUserBot
```

### 2. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

The examples above intentionally use the default Python 3 launcher behavior so your fork can pick up the newest installed Python 3 version, while still expecting **3.11 or newer**.

### 3. Install dependencies

```bash
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

### 4. Install optional extras

```bash
pip install -r requirements.optional.txt
```

Optional extras enable heavier features like OCR helpers and `sentence-transformers` support if you explicitly choose to use them.

### 5. Copy the environment template

```bash
cp .env.example .env
```

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

### 6. Start the bot

```bash
python main.py
```

## ⚙️ One-Command Install Scripts

If you want the faster path:

### Windows

```powershell
.\install-all.ps1
```

This script:

- selects the newest installed Python 3.11+ interpreter, or installs the newest available Python 3 package if needed
- creates `.venv`
- installs `requirements.txt` and `requirements.optional.txt`
- installs FFmpeg
- installs Tesseract OCR
- warms the `sentence-transformers` cache

### Ubuntu

```bash
bash install-all-ubuntu.sh
```

This script:

- selects the newest installed Python 3.11+ interpreter, or installs the newest available `python3.x` package when needed
- installs FFmpeg and Tesseract
- installs multilingual OCR language packs
- creates `.venv`
- installs all Python dependencies
- warms the `sentence-transformers` cache

## 🔐 Minimum Configuration

A lean starter `.env` looks like this:

```env
TELEGRAM_API_ID=123456
TELEGRAM_API_HASH="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

FOLDER_INVITE_LINK="https://t.me/addlist/xxxxxxxxxx"
# EXTRA_SOURCES=["@channel1","https://t.me/+privateInviteHash"]

# Choose one destination mode
DESTINATION="@your_private_channel_or_chat"
# OR
# BOT_DESTINATION_TOKEN="123456:ABCDEF..."
# BOT_DESTINATION_CHAT_ID="7777826640"
```

Important behavior:

- If both `DESTINATION` and bot-destination values are set, **bot destination mode wins**
- `FOLDER_INVITE_LINK` is optional if you prefer `EXTRA_SOURCES`
- `BOT_DESTINATION_CHAT_ID` should usually be your delivery chat, not your query PM

## 🤖 OpenAI / Codex Auth

This project uses a **Codex-style OAuth flow**, not a plain API key setup.

Recommended low-usage model for ChatGPT-account Codex auth:

```env
CODEX_MODEL="gpt-5.1-codex-mini"
```

### Hosted or headless mode

Recommended for Replit, servers, and long-running deployments:

```env
OPENAI_AUTH_ENV_ONLY=true
TG_USERBOT_AUTH_JSON_B64="..."
```

Bootstrap auth into environment form:

```bash
python auth.py bootstrap-env
```

Or write auth values into `.env`:

```bash
python auth.py setup-env
```

### Local interactive mode

```env
OPENAI_AUTH_ENV_ONLY=false
```

Then log in with browser OAuth:

```bash
python auth.py login
```

Local interactive startup can repair missing or stale auth automatically and continue startup in the same process.

### Useful auth commands

```bash
python auth.py login
python auth.py login --env-file .env
python auth.py status
python auth.py logout
python auth.py logout --env-file .env
```

## 🧩 Operating Modes

### 1. Feed / Alert Mode

Each incoming Telegram post is evaluated and can be:

- skipped as a duplicate
- routed as a fast breaking alert
- added to digest
- attached to an existing story thread

### 2. Digest Mode

Instead of forwarding every post as-is, TeleUserBot can publish:

- 30-minute rolling digests
- daily digests

Digest mode is designed for people who want signal density without raw-feed chaos. When `DIGEST_MODE=true`, monitored updates are queued into persistent SQLite storage and delivered through digests only.

### 3. Query Mode

Ask natural-language questions such as:

- `latest tehran news`
- `what happened in last 24 hours`
- `recent beirut updates`
- `who died recently in iran`

The assistant checks the last 24 hours of Telegram evidence first, widens to the last 7 days when coverage is thin, and then runs a trusted web cross-check before answering. If the user explicitly asks for a wider time window, Telegram and web verification both follow that same window up to 30 days.

## 📰 Digest Configuration

Recommended baseline:

```env
TIMEZONE="Asia/Kolkata"
DIGEST_MODE=true
DIGEST_INTERVAL_MINUTES=30
DIGEST_DAILY_TIMES=["00:00"]
DIGEST_DAILY_WINDOW_HOURS=24
DIGEST_MAX_POSTS=80
OUTPUT_LANGUAGE="English"
```

Notes:

- `TIMEZONE` controls local-time logic across the bot; for IST use `Asia/Kolkata`
- rolling digests are clock-aligned to `:00` and `:30`
- digest windows are claimed from SQLite, not held only in memory
- digest queue clearing is intentionally disabled; queued items are drained only by claimed digest windows
- if a digest exceeds Telegram message limits, it is delivered as sequential `Part 1/N`, `Part 2/N`, ... messages
- rolling and daily digests are forced to English output

Optional digest pin rotation:

```env
DIGEST_PIN_HOURLY=false
DIGEST_PIN_DAILY=false
```

When enabled:

- the newest digest of that type is pinned
- the previous pinned digest of that type is unpinned
- pin failure does not block digest delivery

## 🧪 Duplicate Suppression

Duplicate defense runs in layers.

### Text-level

- normalized text fingerprinting
- hybrid similarity scoring
- recent duplicate memory

### Media-level

- same-image detection
- recompressed-media detection
- album signature tracking
- persistent dedupe memory in SQLite

### Story continuity

When follow-up posts arrive as replies in the source channel, the bot can preserve that relationship in the destination feed.

## 🚨 Severity Routing

High-level flow:

- `high` → immediate alert
- `medium` / `low` → digest queue

Breaking tone can be tuned with:

```env
BREAKING_STYLE_MODE=unhinged
```

Modes:

- `unhinged` gives harder-hitting breaking formatting and adds context only when the story linkage is strong enough
- `classic` restores a more restrained layout

## 🖼️ OCR Translation for Media-Only Posts

OCR behavior is intentionally conservative.

- captioned media keeps the original Telegram caption
- image-only posts get a caption only if OCR finds meaningful non-English text and translation succeeds
- video-only posts use first-frame OCR
- English OCR text is ignored
- failed OCR adds nothing

Example config:

```env
MEDIA_TEXT_OCR_ENABLED=true
MEDIA_TEXT_OCR_VIDEO_ENABLED=true
MEDIA_TEXT_OCR_MIN_CHARS=12
MEDIA_TEXT_OCR_MAX_CHARS=1600
MEDIA_TEXT_OCR_VIDEO_MAX_MB=25
MEDIA_TEXT_OCR_LANGS=eng+ara+fas+urd+rus
```

If you want OCR on Linux:

```bash
sudo apt-get update
sudo apt-get install -y tesseract-ocr ffmpeg
sudo apt-get install -y tesseract-ocr-ara tesseract-ocr-fas tesseract-ocr-urd tesseract-ocr-rus
```

## 🔎 Query Assistant Rules

Allowed contexts:

- **Saved Messages**
- **private chat with your own bot account**

Not allowed:

- groups
- channels
- arbitrary private chats with other users

This restriction is intentional and keeps the query workflow private and predictable.

## 🌐 Query Web Cross-Check

Every query answer is verified against trusted news sites after the Telegram search:

```env
QUERY_WEB_MIN_TELEGRAM_RESULTS=3
QUERY_WEB_MAX_RESULTS=12
QUERY_WEB_REQUIRE_RECENT=true
QUERY_WEB_REQUIRE_MIN_SOURCES=2
QUERY_WEB_ALLOWED_DOMAINS=["reuters.com","apnews.com","bbc.com","aljazeera.com","cnn.com","nytimes.com","washingtonpost.com","bloomberg.com","ft.com","theguardian.com","dw.com","france24.com","aa.com.tr","npr.org"]
```

Notes:

- Telegram evidence stays the primary source
- web verification always runs after Telegram evidence gathering
- the default web verification window is 7 days to match the normal Telegram expansion contract
- if the user explicitly asks for a wider window, Telegram and web verification follow that same window up to 30 days
- higher-risk questions are handled more conservatively

## 🧵 Output and Delivery Details

TeleUserBot can deliver with:

- Telegram HTML formatting
- optional premium emoji support
- source-aware reply continuity
- digest-first readability

Useful rendering flags from `.env.example`:

```env
ENABLE_HTML_FORMATTING=true
ENABLE_PREMIUM_EMOJI=true
PREMIUM_EMOJI_MAP_FILE="nezami_emoji_map.json"
```

## 🩺 Health Checks and Hosting

For Replit or uptime-monitored deployments:

```env
ENABLE_WEB_SERVER=true
WEB_SERVER_HOST="0.0.0.0"
WEB_SERVER_PORT=8080
HOLD_ON_STARTUP_ERROR=true
OPENAI_AUTH_ENV_ONLY=true
TG_USERBOT_AUTH_JSON_B64="..."
```

Health endpoints:

- `/` renders the full operator dashboard
- `/status` renders the same dashboard view for hosted status checks in a browser
- `/health` renders a simplified health page and returns `200` when healthy or `503` when degraded

Suggested hosted commands:

- install: `pip install -r requirements.txt`
- optional extras: `pip install -r requirements.optional.txt`
- run: `python main.py`

## 🛠️ Running the Bot

Single entrypoint:

```bash
python main.py
```

Startup flow:

1. validates config
2. ensures only one instance is active
3. initializes runtime DB and caches
4. repairs auth inline when interactive mode detects stale or missing auth
5. connects your Telegram session
6. resolves sources
7. starts feed, digest, query, and optional web server pipelines

## 🧪 Tests

The repo includes test coverage for major pipeline pieces.

Run:

```bash
pytest
```

Or install dev requirements first:

```bash
pip install -r requirements.dev.txt
pytest
```

## 💬 Operator Command

Default digest status command:

```text
/digest_status
```

This reports queue state, scheduler status, and runtime health details.

## 🧯 Troubleshooting

### `sentence-transformers unavailable`

Not a problem if you intentionally run without Hugging Face support.

Install optional extras only if you want that backend:

```bash
pip install -r requirements.optional.txt
```

### `database is locked`

Another process is probably using the same Telethon session or SQLite DB.

Fix:

- stop duplicate processes
- keep only one active instance

### Repeated OAuth prompts on a server

Use env-only auth:

```env
OPENAI_AUTH_ENV_ONLY=true
TG_USERBOT_AUTH_JSON_B64="..."
```

### `PhoneNumberInvalidError`

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

### Query replies look wrong or missing

Keep query mode limited to:

- Saved Messages
- your own bot PM

### Bot API upload timeout or connection reset

The bot retries transient delivery errors once. If failures continue, check:

- network quality
- VPS stability
- proxy or VPN path
- oversized media uploads

## 🔒 Security

Never commit:

- `.env`
- `userbot.session*`
- `~/.tg_userbot/*` secrets
- exported auth payloads
- private token dumps

If anything sensitive leaks, rotate it immediately.

## 📦 Upgrade Workflow

```bash
git pull
source .venv/bin/activate
pip install -r requirements.txt --upgrade
pip install -r requirements.optional.txt --upgrade
python main.py
```

## 🧠 Practical Deployment Advice

Best results usually come from separating roles:

- one chat for **feed delivery**
- one private bot PM or Saved Messages for **queries**

Mixing both into a single high-volume chat works, but the experience becomes noisier and less controlled.

## ⚠️ Responsible Use

This project operates on a real Telegram account and may process content from many sources. Use it responsibly, follow Telegram rules, respect local laws, and handle monitored content with care.

---

## 💫 Summary

TeleUserBot is for operators who want Telegram monitoring to feel sharper, calmer, and more intelligent:

- fewer duplicates
- better urgency control
- cleaner digests
- stronger private search
- more useful media handling

If your current setup feels like chaos in a trench coat, this is the upgrade. ✨
