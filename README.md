# 🚀 Telegram News Intelligence Userbot

Production-grade Telegram userbot for high-signal news monitoring, strict duplicate suppression, breaking-event routing, and scheduled digest delivery.

## 🎯 What It Does

This project runs on your personal Telegram account (Telethon user session), monitors many source channels/groups, and delivers curated output to one destination.

Core outcomes:
- Filters noise and duplicate reposts aggressively
- Sends only true high-severity items immediately
- Produces clean scheduled digests instead of spam floods
- Supports query-style interaction for your own private chats

## ✅ Current Feature Set

- **Telethon userbot mode** (personal account, not bot-token polling)
- **Source intake** from `FOLDER_INVITE_LINK` + `EXTRA_SOURCES`
- **Media-safe handling** for text, files, captions, and albums (`grouped_id` buffering)
- **Codex OAuth backend integration** (`https://chatgpt.com/backend-api/codex/responses`)
- **OAuth env bootstrap** (`TG_USERBOT_AUTH_JSON` / `TG_USERBOT_AUTH_JSON_B64`)
- **Hybrid dedupe** (semantic + lexical + fuzzy + persistent history)
- **Severity routing** (high -> immediate, medium/low -> digest queue)
- **Breaking topic threading** support
- **Hourly digest** from transactional queue
- **Daily 24h digest at 00:00** from persistent archive window
- **Queue cleanup scheduler** every 10 min with configurable scope
- **Streaming output** with live edits
- **Telegram HTML output pipeline** + optional premium emoji mapping
- **Bot API destination fallback** (`BOT_DESTINATION_TOKEN` + `BOT_DESTINATION_CHAT_ID`)
- **Health web server** for Replit/UptimeRobot (`/`, `/status`, `/health`)
- **Graceful shutdown + failure-safe queue restore**

## 🧠 Runtime Flow

```text
Telegram Sources
   -> intake handler
      -> seen check + hybrid dedupe
      -> severity classification
         -> HIGH => immediate breaking send
         -> MED/LOW => digest_queue insert + digest_archive insert

Schedulers:
- Hourly digest: claims digest_queue transactionally, summarizes, sends, acks
- Daily digest (00:00): loads last 24h from digest_archive, summarizes, sends
- Queue clear (10m): clears by scope (recommended: inflight only)
```

## 🗂️ Data Persistence

Runtime state is stored in `~/.tg_userbot/`:
- `seen.db` (SQLite)
  - `seen_messages`
  - `digest_queue`
  - `digest_archive`
  - `recent_breaking`
  - `digest_meta`
- `errors.log`

Session file is local project file:
- `userbot.session`

## ⚙️ Configuration

Copy and edit env:

```bash
cp .env.example .env
```

### Required minimum

- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- Source config:
  - `FOLDER_INVITE_LINK` **or** `EXTRA_SOURCES`
- Destination config (choose one):
  - `DESTINATION`
  - or `BOT_DESTINATION_TOKEN` + `BOT_DESTINATION_CHAT_ID`

### Recommended digest/scheduler settings

```env
DIGEST_MODE=true
DIGEST_INTERVAL_MINUTES=60
DIGEST_DAILY_TIMES=["00:00"]
DIGEST_DAILY_WINDOW_HOURS=24
DIGEST_QUEUE_CLEAR_INTERVAL_MINUTES=10
DIGEST_QUEUE_CLEAR_SCOPE="inflight"
```

`DIGEST_QUEUE_CLEAR_SCOPE` options:
- `inflight` (recommended): clears only stuck claimed rows
- `pending`: clears unsent queue rows
- `all`: clears both pending + inflight (destructive)

## 🔐 OpenAI/Codex Auth Modes

### 1) Env-only mode (recommended for servers/Replit)

Set:
- `OPENAI_AUTH_ENV_ONLY=true`
- `TG_USERBOT_AUTH_JSON` **or** `TG_USERBOT_AUTH_JSON_B64`

This avoids interactive browser login during deploy/runtime.

### 2) Browser OAuth mode (local interactive)

Set:
- `OPENAI_AUTH_ENV_ONLY=false`

Then first run can perform PKCE browser flow.

### Optional auth endpoint overrides

- `OPENAI_AUTH_BASE`
- `OPENAI_AUTH_ENDPOINT`
- `OPENAI_TOKEN_ENDPOINT`

## 🏃 Local Run

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

## ☁️ Replit / Deployment Notes

Use deployment settings:
- **Build command:** `pip install -r requirements.txt`
- **Run command:** `python main.py`
- **Port:** `8080`

Keep health server enabled:
- `ENABLE_WEB_SERVER=true`
- `WEB_SERVER_HOST=0.0.0.0`
- `WEB_SERVER_PORT=8080`

Endpoints:
- `/health`
- `/status`
- `/`

For headless environments, keep:
- `HOLD_ON_STARTUP_ERROR=true`

## 🧪 Operational Controls

In Telegram (outgoing command):
- `/digest_status`

Shows:
- mode
- pending/inflight queue
- next digest ETA
- daily window/times
- queue clear scope
- quota health and feature flags

## 🛡️ Reliability Guarantees

- Transactional batch claim/ack/restore for digest queue
- Exponential backoff on digest failures
- Flood-wait handling
- Restricted-forward fallback to re-send
- Scheduler locking to avoid race conditions
- Structured logs for queue/severity/digest lifecycle

## 🧰 Troubleshooting

### No digests arriving

Check `/digest_status` and verify:
- `DIGEST_MODE=true`
- pending queue increases
- destination credentials are valid
- `DIGEST_QUEUE_CLEAR_SCOPE` is not wiping pending data unexpectedly

### Daily digest empty at midnight

Daily digest reads from `digest_archive` for the last `DIGEST_DAILY_WINDOW_HOURS`.
If archive is empty, nothing was ingested in that window.

### OAuth repeated login prompts

Use env-only secrets on server environments:
- `OPENAI_AUTH_ENV_ONLY=true`
- `TG_USERBOT_AUTH_JSON_B64=<...>`

### Telethon session locked

Only one process should use `userbot.session` at a time.

## 🔒 Security Checklist

Never commit:
- `.env`
- `userbot.session*`
- token material
- private emoji mapping files (if sensitive)

Rotate credentials immediately if exposed.

## 📌 Final Notes

- Server local time controls `DIGEST_DAILY_TIMES` trigger (00:00 means server midnight).
- For best signal quality, tune source tiers and severity settings before lowering dedupe strictness.
