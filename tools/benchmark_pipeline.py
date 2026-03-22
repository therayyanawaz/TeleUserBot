from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import math
import statistics
import time
from pathlib import Path

import auth
import db
from ai_filter import (
    _fallback_filter_decision,
    decide_filter_action,
    get_filter_decision_cache_stats,
)
from utils import build_dupe_fingerprint, normalize_space


class _BenchmarkAuthManager:
    def __init__(self) -> None:
        self._manager = auth.AuthManager()

    async def get_auth_context(self):
        return await self._manager.get_auth_context()

    async def refresh_auth_context(self):
        return await self._manager.refresh_auth_context()


def _load_corpus(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            payload = json.loads(raw)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(math.ceil(len(ordered) * 0.95)) - 1))
    return ordered[index]


async def _run(args: argparse.Namespace) -> int:
    corpus_path = Path(args.corpus).resolve()
    if not corpus_path.exists():
        raise FileNotFoundError(f"Corpus file not found: {corpus_path}")

    db_path = Path(args.db or (corpus_path.parent / "benchmark.db")).resolve()
    auth.DB_PATH = str(db_path)
    db.DB_PATH = str(db_path)
    importlib.reload(db)
    db.DB_PATH = str(db_path)
    db.init_db()

    rows = _load_corpus(corpus_path)
    if not rows:
        raise RuntimeError("Benchmark corpus is empty.")

    duplicate_hits = 0
    seen_hashes: set[str] = set()
    decision_latencies_ms: list[float] = []
    auth_manager = _BenchmarkAuthManager() if args.use_ai else None

    for index, row in enumerate(rows, start=1):
        text = normalize_space(str(row.get("text") or ""))
        if not text:
            continue

        _normalized, text_hash = build_dupe_fingerprint(text)
        if text_hash in seen_hashes:
            duplicate_hits += 1
        else:
            seen_hashes.add(text_hash)

        channel_id = str(row.get("channel_id") or "benchmark")
        message_id = int(row.get("message_id") or index)
        payload_json = json.dumps(
            {
                "kind": "single",
                "channel_id": channel_id,
                "primary_message_id": message_id,
                "message_ids": [message_id],
                "candidate_text": text,
            },
            separators=(",", ":"),
        )
        db.enqueue_inbound_job(
            job_key=f"single:{channel_id}:{message_id}",
            stage="triage",
            channel_id=channel_id,
            message_id=message_id,
            payload_json=payload_json,
            priority=50,
            max_retries=4,
        )

        started = time.perf_counter()
        if auth_manager is not None:
            await decide_filter_action(text, auth_manager)
        else:
            _fallback_filter_decision(text)
        decision_latencies_ms.append((time.perf_counter() - started) * 1000.0)

    cache_stats = get_filter_decision_cache_stats()
    total_messages = max(1, len(rows))
    ai_calls = int(cache_stats.get("misses", 0.0)) if args.use_ai else 0

    report = {
        "messages_total": total_messages,
        "duplicates_detected": duplicate_hits,
        "duplicate_hit_rate": round(duplicate_hits / total_messages, 4),
        "ai_calls": ai_calls,
        "ai_calls_per_100_messages": round((ai_calls / total_messages) * 100.0, 2),
        "decision_avg_ms": round(statistics.mean(decision_latencies_ms), 2),
        "decision_p95_ms": round(_p95(decision_latencies_ms), 2),
        "cache_hits": cache_stats.get("hits", 0.0),
        "cache_misses": cache_stats.get("misses", 0.0),
        "cache_hit_rate": round(float(cache_stats.get("hit_rate", 0.0)), 4),
        "queued_jobs": db.count_inbound_jobs(),
    }
    print(json.dumps(report, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark duplicate-heavy pipeline behavior.")
    parser.add_argument("--corpus", required=True, help="Path to a JSONL corpus with a `text` field.")
    parser.add_argument("--db", help="Optional SQLite path for benchmark state.")
    parser.add_argument(
        "--use-ai",
        action="store_true",
        help="Use real AuthManager + decide_filter_action instead of heuristic fallback decisions.",
    )
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
