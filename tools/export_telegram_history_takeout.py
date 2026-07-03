"""Export all Telegram messages from one chat over any date range.

Edit the CONFIG section, then run:
    python tools/export_telegram_history_takeout.py

Optional CLI overrides:
    python tools/export_telegram_history_takeout.py --chat @channel --start 2024-01-01 --end 2024-12-31
"""

from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, time, timezone
import json
import logging
import os
from pathlib import Path
import sqlite3
from typing import Any

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError, TakeoutInitDelayError


# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------
# Prefer environment variables for secrets:
#   TELEGRAM_API_ID=12345
#   TELEGRAM_API_HASH=abcdef...
API_ID = int(os.getenv("TELEGRAM_API_ID", "0") or "0")
API_HASH = os.getenv("TELEGRAM_API_HASH", "")

# Username, phone number, invite-resolved entity, or integer chat ID.
CHAT_IDENTIFIER: str | int = "@example_channel"

# Use YYYY-MM-DD, ISO datetime, or None.
START_DATE: str | None = None
END_DATE: str | None = None

SESSION_NAME = "telegram_history_export"
WAIT_TIME_SECONDS = 2.0
SAVE_JSONL = True
OUTPUT_JSONL = Path("messages.jsonl")
PROGRESS_DB = Path("telegram_export_progress.sqlite3")
MAX_RETRIES = 3
PHONE_NUMBER: str | None = None


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("telegram_history_export")


@dataclass(frozen=True)
class ExportConfig:
    api_id: int
    api_hash: str
    chat_identifier: str | int
    start_date: datetime | None
    end_date: datetime
    end_date_is_open: bool
    session_name: str
    wait_time: float
    save_jsonl: bool
    output_jsonl: Path
    progress_db: Path
    max_retries: int
    phone_number: str | None


def _parse_date(value: str | None, *, end_of_day: bool = False) -> datetime | None:
    if value is None or not str(value).strip():
        return None

    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid date {value!r}; use YYYY-MM-DD or ISO datetime.") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)

    if end_of_day and "T" not in text and len(text) == 10:
        parsed = datetime.combine(parsed.date(), time.max, tzinfo=timezone.utc)
    return parsed


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _connect_progress_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS export_progress (
            progress_key TEXT PRIMARY KEY,
            chat_id INTEGER,
            chat_title TEXT,
            start_date TEXT,
            end_date TEXT,
            last_message_id INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def _progress_key(chat_id: int, start_date: datetime | None, end_date: datetime, *, end_date_is_open: bool) -> str:
    start_text = start_date.astimezone(timezone.utc).isoformat() if start_date else "oldest"
    end_text = "open_end" if end_date_is_open else end_date.astimezone(timezone.utc).isoformat()
    return f"{chat_id}|{start_text}|{end_text}"


def _load_last_message_id(conn: sqlite3.Connection, key: str) -> int:
    row = conn.execute(
        "SELECT last_message_id FROM export_progress WHERE progress_key = ?",
        (key,),
    ).fetchone()
    return int(row[0]) if row else 0


def _save_last_message_id(
    conn: sqlite3.Connection,
    *,
    key: str,
    chat_id: int,
    chat_title: str,
    start_date: datetime | None,
    end_date: datetime,
    message_id: int,
) -> None:
    conn.execute(
        """
        INSERT INTO export_progress (
            progress_key, chat_id, chat_title, start_date, end_date, last_message_id, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(progress_key) DO UPDATE SET
            last_message_id = excluded.last_message_id,
            updated_at = excluded.updated_at
        """,
        (
            key,
            chat_id,
            chat_title,
            start_date.astimezone(timezone.utc).isoformat() if start_date else "",
            end_date.astimezone(timezone.utc).isoformat(),
            int(message_id),
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()


def _message_to_record(message: Any, *, chat_id: int, chat_title: str) -> dict[str, Any]:
    return {
        "chat_id": chat_id,
        "chat_title": chat_title,
        "message_id": int(message.id or 0),
        "date": message.date.astimezone(timezone.utc).isoformat() if message.date else None,
        "sender_id": getattr(message, "sender_id", None),
        "text": message.raw_text or "",
    }


def _print_record(record: dict[str, Any]) -> None:
    print(
        f"[{record['date']}] sender={record['sender_id']} id={record['message_id']} "
        f"{record['text']}"
    )


async def _export_once(config: ExportConfig) -> int:
    if config.api_id <= 0 or not config.api_hash:
        raise RuntimeError("Set TELEGRAM_API_ID and TELEGRAM_API_HASH, or edit API_ID/API_HASH.")

    conn = _connect_progress_db(config.progress_db)
    exported = 0

    async with TelegramClient(config.session_name, config.api_id, config.api_hash) as client:
        await client.start(phone=config.phone_number)
        try:
            entity = await client.get_entity(config.chat_identifier)
        except (TypeError, ValueError, RPCError) as exc:
            raise RuntimeError(f"Invalid chat identifier: {config.chat_identifier!r}") from exc

        chat_id = int(getattr(entity, "id", 0) or 0)
        chat_title = str(
            getattr(entity, "title", None)
            or getattr(entity, "username", None)
            or getattr(entity, "phone", None)
            or chat_id
        )
        key = _progress_key(
            chat_id,
            config.start_date,
            config.end_date,
            end_date_is_open=config.end_date_is_open,
        )
        last_message_id = _load_last_message_id(conn, key)

        LOGGER.info(
            "Exporting chat=%s id=%s start=%s end=%s resume_after_id=%s",
            chat_title,
            chat_id,
            config.start_date.isoformat() if config.start_date else "oldest",
            config.end_date.isoformat(),
            last_message_id,
        )

        output_handle = None
        if config.save_jsonl:
            config.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
            output_handle = config.output_jsonl.open("a", encoding="utf-8")

        try:
            async with client.takeout() as takeout:
                async for message in takeout.iter_messages(
                    entity,
                    limit=None,
                    reverse=True,
                    min_id=max(0, last_message_id),
                    offset_date=config.start_date,
                    wait_time=max(0.0, float(config.wait_time)),
                ):
                    if not message or not getattr(message, "id", None):
                        continue
                    if int(message.id) <= last_message_id:
                        continue

                    message_date = message.date.astimezone(timezone.utc) if message.date else None
                    if message_date is None:
                        continue
                    if config.start_date and message_date < config.start_date:
                        continue
                    if message_date > config.end_date:
                        break

                    record = _message_to_record(message, chat_id=chat_id, chat_title=chat_title)
                    _print_record(record)
                    if output_handle is not None:
                        output_handle.write(json.dumps(record, ensure_ascii=False, default=_json_default) + "\n")
                        output_handle.flush()

                    last_message_id = int(message.id)
                    _save_last_message_id(
                        conn,
                        key=key,
                        chat_id=chat_id,
                        chat_title=chat_title,
                        start_date=config.start_date,
                        end_date=config.end_date,
                        message_id=last_message_id,
                    )
                    exported += 1
        finally:
            if output_handle is not None:
                output_handle.close()
            conn.close()

    return exported


async def export_messages(config: ExportConfig) -> int:
    for attempt in range(1, max(1, config.max_retries) + 1):
        try:
            return await _export_once(config)
        except TakeoutInitDelayError as exc:
            wait_seconds = int(getattr(exc, "seconds", 0) or 0)
            LOGGER.error("Telegram requires takeout delay: wait %ss, then rerun.", wait_seconds)
            raise
        except FloodWaitError as exc:
            wait_seconds = int(getattr(exc, "seconds", 0) or 0) + 1
            LOGGER.warning("FloodWait: sleeping %ss before retry %s/%s", wait_seconds, attempt, config.max_retries)
            await asyncio.sleep(wait_seconds)
        except (OSError, TimeoutError, asyncio.TimeoutError, RPCError) as exc:
            if attempt >= config.max_retries:
                LOGGER.exception("Export failed after %s attempts.", attempt)
                raise
            wait_seconds = min(60, 2 * attempt)
            LOGGER.warning("Transient error on attempt %s/%s: %s", attempt, config.max_retries, exc)
            await asyncio.sleep(wait_seconds)
    return 0


def _build_config_from_args() -> ExportConfig:
    parser = argparse.ArgumentParser(description="Export Telegram chat history with Takeout API.")
    parser.add_argument("--chat", default=None, help="Username, phone number, or integer chat ID.")
    parser.add_argument("--start", default=None, help="Start date: YYYY-MM-DD or ISO datetime.")
    parser.add_argument("--end", default=None, help="End date: YYYY-MM-DD or ISO datetime.")
    parser.add_argument("--session", default=SESSION_NAME, help="Telethon session name/path.")
    parser.add_argument("--jsonl", default=str(OUTPUT_JSONL), help="Output JSONL path.")
    parser.add_argument("--no-jsonl", action="store_true", help="Print only; do not save JSONL.")
    parser.add_argument("--progress-db", default=str(PROGRESS_DB), help="SQLite progress DB path.")
    parser.add_argument("--wait-time", type=float, default=WAIT_TIME_SECONDS, help="Seconds between API requests.")
    args = parser.parse_args()

    chat_identifier: str | int = args.chat if args.chat is not None else CHAT_IDENTIFIER
    if isinstance(chat_identifier, str) and chat_identifier.lstrip("-").isdigit():
        chat_identifier = int(chat_identifier)

    start = _parse_date(args.start if args.start is not None else START_DATE)
    raw_end = args.end if args.end is not None else END_DATE
    end_date_is_open = raw_end is None or not str(raw_end).strip()
    end = _parse_date(raw_end, end_of_day=True)
    if end is None:
        end = datetime.now(timezone.utc)
    if start is not None and end < start:
        raise ValueError("end_date must be greater than or equal to start_date.")

    return ExportConfig(
        api_id=API_ID,
        api_hash=API_HASH,
        chat_identifier=chat_identifier,
        start_date=start,
        end_date=end,
        end_date_is_open=end_date_is_open,
        session_name=str(args.session),
        wait_time=max(0.0, float(args.wait_time)),
        save_jsonl=not bool(args.no_jsonl) and SAVE_JSONL,
        output_jsonl=Path(args.jsonl),
        progress_db=Path(args.progress_db),
        max_retries=max(1, int(MAX_RETRIES)),
        phone_number=PHONE_NUMBER,
    )


def main() -> None:
    config = _build_config_from_args()
    exported = asyncio.run(export_messages(config))
    LOGGER.info("Export complete. Messages exported this run: %s", exported)


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        main()
