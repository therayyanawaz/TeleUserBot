# TeleUserBot - Production News Digest Userbot

High-signal Telegram news aggregation on your **personal Telegram account** (Telethon user session), with:

- OpenAI Codex-style OAuth (PKCE, no API key in config)
- real-time intake from many channels
- semantic dedupe + severity routing
- batch digest delivery every interval (default 30 min)
- Bot API or Telethon destination delivery

This project is built for continuous operation and low-noise output.

## Features

- Userbot mode (`Telethon`) using your own account (`userbot.session`)
- Source auto-resolution from Telegram folder invite (`t.me/addlist/...`)
- Optional extra source onboarding via username/public/private invite links
- Album buffering (`grouped_id`) with `1.5s` merge window
- Media-without-caption immediate forward (AI bypass)
- Digest queue persistence in SQLite with transactional claim/ack/restore
- Near-duplicate suppression (sentence-transformers preferred, TF-cosine fallback)
- Severity routing (`high` -> immediate breaking alert, `medium/low` -> queued digest)
- Codex backend SSE inference (`https://chatgpt.com/backend-api/codex/responses`)
- Local fallback behavior on quota/rate/API errors
- `/digest_status` runtime command
- Graceful shutdown (scheduler + album tasks + client disconnect)

## Architecture

Intake and delivery flow:

1. `events.NewMessage` receives source updates.
2. Dedup check in `seen_messages`.
3. If digest mode:
   - media-only -> immediate send
   - text/caption -> semantic dedupe -> severity classify
   - `high` + `IMMEDIATE_HIGH=True` -> immediate `BREAKING` headline
   - otherwise -> enqueue to `digest_queue`
4. Scheduler wakes every interval (or daily times), claims queue batch transactionally.
5. One Codex batch prompt produces compact digest headlines.
6. Digest is sent in chunks (rate-limit friendly), batch is ACKed.
7. On failure, batch is restored and retried with backoff.

## Repository Layout

```text
TeleUserBot/
  main.py           # Telethon runtime, routing, scheduler, delivery
  config.py         # Env loader + typed defaults
  .env.example      # Template for local secrets/config
  auth.py           # OAuth PKCE + token lifecycle
  ai_filter.py      # Codex calls, severity/headline/digest generation
  db.py             # SQLite persistence and transactional queue ops
  utils.py          # Shared helpers + near-duplicate detector
  prompts.py        # Prompt templates
  requirements.txt
  README.md
```

Runtime state is stored outside repo:

- `~/.tg_userbot/auth.json` (OAuth tokens)
- `~/.tg_userbot/seen.db` (seen + queue + metadata)
- `~/.tg_userbot/errors.log` (error traces)

## Requirements

- Python `3.11+`
- Telegram account
- OpenAI account (for OAuth login)

Install:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Optional (better semantic dedupe quality):

```bash
pip install sentence-transformers
```

If `sentence-transformers` is missing, bot auto-falls back to TF-cosine dedupe.

## Configuration

Configuration is environment-driven.
Use a local `.env` file (gitignored) or exported environment variables.

```bash
cp .env.example .env
```

Then edit `.env`.

Minimum required:

- `TELEGRAM_API_ID`
- `TELEGRAM_API_HASH`
- one source strategy:
  - `FOLDER_INVITE_LINK` or
  - `EXTRA_SOURCES` / `SOURCES`
- one destination strategy:
  - Telethon destination: `DESTINATION`
  - Bot API destination: `BOT_DESTINATION_TOKEN` + `BOT_DESTINATION_CHAT_ID`

Important runtime toggles:

- `DIGEST_MODE` (`True` recommended)
- `DIGEST_INTERVAL_MINUTES` (default `30`)
- `ENABLE_DUPE_DETECTION`
- `DUPE_THRESHOLD` (default `0.83`)
- `ENABLE_SEVERITY_ROUTING`
- `IMMEDIATE_HIGH`
- `OUTPUT_LANGUAGE` (default `English`)
- `INCLUDE_SOURCE_TAGS` (default `False`)

Security note:

- Do not commit real credentials/tokens.
- Rotate any secrets already committed.

## Getting Telegram Credentials

1. Open `https://my.telegram.org`
2. Go to **API Development Tools**
3. Create app and copy:
   - `api_id` -> `TELEGRAM_API_ID`
   - `api_hash` -> `TELEGRAM_API_HASH`

## First Run

```bash
python main.py
```

What happens:

1. Missing config fields are prompted and saved to `.env`.
2. OAuth browser flow opens once (PKCE via localhost callback).
3. Token is stored at `~/.tg_userbot/auth.json` (`chmod 600`).
4. Telethon logs in as your personal account.
5. Sources are resolved/joined.
6. Scheduler starts (if `DIGEST_MODE=True`).

## Source Onboarding

Folder mode:

- Set `FOLDER_INVITE_LINK="https://t.me/addlist/<slug>"`
- Bot resolves channels from folder and subscribes where possible.

Manual mode:

- Set `EXTRA_SOURCES` in `.env` as JSON list:
  - `["@username","https://t.me/public_channel_or_group","https://t.me/+privateInviteHash"]`
  - supported entry types:
  - `@username`
  - `https://t.me/public_channel_or_group`
  - `https://t.me/+privateInviteHash`

## Destination Modes

Telethon destination:

- Set `DESTINATION` to `@channel`, link, peer id, or `me`.

Bot API destination:

- Set `BOT_DESTINATION_TOKEN` and `BOT_DESTINATION_CHAT_ID`.
- If chat id is missing, bot tries `getUpdates` auto-detect.
- Ensure destination bot has access to target chat/channel.

## Commands

Outgoing command from your own account:

- `/digest_status`

Returns mode, queue state, interval/ETA, quota health, and dedupe/severity status.

## Operational Behavior

Error handling includes:

- `FloodWaitError` -> sleep and retry
- restricted forwards -> download + resend
- Bot API `429` -> wait `retry_after` then retry
- Codex `429` -> local fallback summarization/digest
- digest failure -> restore claimed batch + exponential backoff
- unhandled exceptions -> `~/.tg_userbot/errors.log`

Shutdown:

- `Ctrl+C` triggers graceful cancellation and disconnect.

## Troubleshooting

### Browser OAuth opens repeatedly

Likely causes:

- stale process running old code
- invalid/expired refresh token

Actions:

1. Stop all running bot instances.
2. Run again with latest code.
3. If still broken, remove `~/.tg_userbot/auth.json` and re-login once.

### `sentence-transformers unavailable`

This is non-fatal. Bot uses TF-cosine fallback dedupe.
Install optional package for better quality:

```bash
pip install sentence-transformers
```

### No sources resolved from folder

- verify folder invite slug
- ensure account can access listed chats
- add `EXTRA_SOURCES` manually as fallback

### Bot destination invalid

- verify bot token format
- verify `BOT_DESTINATION_CHAT_ID`
- start bot in DM or add as channel admin

## Production Recommendations

- Run inside `tmux`/`screen` or a process supervisor (`systemd`, `supervisord`).
- Keep `.venv` isolated and pinned.
- Back up `~/.tg_userbot/seen.db` regularly.
- Monitor `errors.log` and `/digest_status`.
- Keep source count and interval tuned to your quota and latency needs.

## License / Usage

Personal-use automation project. Review Telegram terms and local compliance requirements before operating at scale.
