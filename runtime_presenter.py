"""Shared runtime activity presentation for console, files, and web views."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime
from html import escape
import json
import logging
import shutil
import textwrap
import traceback
from threading import Lock
from typing import Any, Callable, Deque, List, Mapping, Sequence, Tuple


SanitizeFn = Callable[[str], str]

_SURFACE_WIDTHS = {
    "console": 112,
    "file": 124,
    "web": 108,
}

_CATEGORY_LABELS = {
    "auth": "AUTH",
    "breaking": "BREAKING",
    "duplicates": "DUPES",
    "digest": "DIGEST",
    "error": "ERROR",
    "memory": "MEMORY",
    "pipeline": "PIPELINE",
    "query": "QUERY",
    "startup": "STARTUP",
    "story": "STORY",
    "warning": "WARN",
}

_CATEGORY_COLORS = {
    "auth": "\033[36m",
    "breaking": "\033[31m",
    "duplicates": "\033[33m",
    "digest": "\033[32m",
    "error": "\033[31m",
    "memory": "\033[35m",
    "pipeline": "\033[34m",
    "query": "\033[36m",
    "startup": "\033[37m",
    "story": "\033[33m",
    "warning": "\033[33m",
}

_RESET = "\033[0m"
_BOLD = "\033[1m"
_DIM = "\033[2m"

_EVENT_TITLE_OVERRIDES = {
    "album_media_duplicate_suppressed": "Album duplicate suppressed",
    "auth_startup_state": "Auth startup state",
    "breaking_album_sent_immediate": "Breaking album delivered",
    "breaking_duplicate_suppressed": "Breaking duplicate suppressed",
    "breaking_pipeline_sent": "Breaking alert delivered",
    "daily_digest_scheduler_start": "Daily digest scheduler online",
    "daily_digest_sent": "Daily digest sent",
    "digest_batch_claimed": "Digest batch claimed",
    "digest_failed_restored": "Digest restored after failure",
    "digest_pipeline_queued": "Queued for digest",
    "digest_queue_clear_scheduler_start": "Queue clear scheduler online",
    "digest_queue_cleared": "Digest queue cleared",
    "digest_scheduler_start": "Digest scheduler online",
    "digest_sent": "Digest sent",
    "dupe_detector_ready": "Duplicate backend ready",
    "inbound_job_enqueued": "Inbound job enqueued",
    "media_duplicate_suppressed": "Media duplicate suppressed",
    "memory_snapshot": "Memory snapshot",
    "query_answered": "Query answered",
    "query_failed": "Query failed",
    "severity_classified": "Severity classified",
    "startup_failure": "Startup failure",
    "startup_health": "Startup health",
    "story_cluster_created": "Story cluster created",
    "story_cluster_edited": "Story cluster updated",
    "story_cluster_suppressed": "Story cluster suppressed",
    "story_cluster_threaded": "Story cluster threaded",
    "web_status_server_started": "Status server online",
}

_EVENT_CATEGORY_PREFIXES = (
    ("auth", "auth"),
    ("breaking", "breaking"),
    ("daily_digest", "digest"),
    ("digest", "digest"),
    ("dupe", "duplicates"),
    ("memory", "memory"),
    ("query", "query"),
    ("severity", "pipeline"),
    ("startup", "startup"),
    ("story_cluster", "story"),
)

_DETAIL_PRIORITY = (
    "phase",
    "startup_phase",
    "source",
    "severity",
    "severity_score",
    "mode",
    "message_id",
    "first_message_id",
    "channel_id",
    "cluster_id",
    "root_message_id",
    "reply_to",
    "pending",
    "pending_queue",
    "inflight",
    "inflight_queue",
    "resolved_source_count",
    "batch_id",
    "rows",
    "error",
    "failure_reason",
)


@dataclass(frozen=True)
class RuntimeEventView:
    created_at: float
    timestamp_short: str
    timestamp_full: str
    level_name: str
    levelno: int
    logger_name: str
    category: str
    title: str
    summary: str
    badges: Tuple[str, ...]
    details: Tuple[Tuple[str, str], ...]
    traceback_text: str
    event_name: str = ""


def _prettify_token(value: str) -> str:
    text = str(value or "").replace("_", " ").replace("-", " ").strip()
    if not text:
        return "Activity"
    return " ".join(part.capitalize() for part in text.split())


def _load_payload(message: str) -> Mapping[str, Any] | None:
    raw = str(message or "").strip()
    if not raw.startswith("{") or not raw.endswith("}"):
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _summarize_mapping(mapping: Mapping[str, Any]) -> str:
    parts: List[str] = []
    for index, (key, value) in enumerate(mapping.items()):
        if index >= 3:
            break
        parts.append(f"{_prettify_token(key)} {_format_value(value, nested=True)}")
    if len(mapping) > 3:
        parts.append(f"+{len(mapping) - 3} more")
    return ", ".join(parts) if parts else f"{len(mapping)} fields"


def _summarize_sequence(values: Sequence[Any]) -> str:
    display = [str(_format_value(item, nested=True)) for item in values[:3]]
    if len(values) > 3:
        display.append(f"+{len(values) - 3} more")
    return ", ".join(display) if display else "0 items"


def _format_bytes(value: Any) -> str:
    try:
        amount = float(value)
    except Exception:
        return str(value)
    units = ("B", "KB", "MB", "GB", "TB")
    for unit in units:
        if abs(amount) < 1024.0 or unit == units[-1]:
            return f"{amount:.1f}{unit}" if unit != "B" else f"{int(amount)}B"
        amount /= 1024.0
    return f"{amount:.1f}TB"


def _format_value(value: Any, *, nested: bool = False) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        text = " ".join(value.split())
        if len(text) > 160 and nested:
            return text[:157].rstrip() + "..."
        return text
    if isinstance(value, Mapping):
        return _summarize_mapping(value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return _summarize_sequence(list(value))
    return str(value)


def _event_category(event_name: str, levelno: int) -> str:
    lowered = str(event_name or "").strip().lower()
    for prefix, category in _EVENT_CATEGORY_PREFIXES:
        if lowered.startswith(prefix):
            return category
    if "duplicate" in lowered:
        return "duplicates"
    if levelno >= logging.ERROR:
        return "error"
    if levelno >= logging.WARNING:
        return "warning"
    return "pipeline"


def _memory_summary(payload: Mapping[str, Any]) -> str:
    current = _format_bytes(payload.get("memory_current_bytes", 0))
    peak = _format_bytes(payload.get("memory_peak_bytes", 0))
    budget = _format_bytes(payload.get("memory_budget_bytes", 0))
    return f"Current {current} | Peak {peak} | Budget {budget}"


def _generic_summary(payload: Mapping[str, Any], logger_name: str) -> str:
    parts: List[str] = []
    source = payload.get("source")
    if source:
        parts.append(str(source))
    severity = payload.get("severity")
    if severity:
        parts.append(str(severity).upper())
    for key, label in (("message_id", "msg"), ("first_message_id", "msg"), ("channel_id", "chat")):
        if payload.get(key) is not None:
            parts.append(f"{label} {payload.get(key)}")
            break
    if payload.get("startup_phase"):
        parts.append(f"phase {payload.get('startup_phase')}")
    if payload.get("mode"):
        parts.append(f"mode {payload.get('mode')}")
    if payload.get("ready") is not None and not parts:
        parts.append("ready" if payload.get("ready") else "not ready")
    if not parts and logger_name != "tg_news_userbot":
        parts.append(logger_name)
    return " | ".join(parts[:4])


def _event_title(event_name: str, payload: Mapping[str, Any], fallback: str) -> str:
    lowered = str(event_name or "").strip().lower()
    if lowered == "memory_snapshot":
        memory_reason = str(payload.get("memory_reason") or "").strip()
        if memory_reason:
            return f"Memory snapshot: {_prettify_token(memory_reason)}"
    if lowered == "startup_phase_changed":
        phase_reason = str(payload.get("phase_reason") or payload.get("startup_phase") or "").strip()
        if phase_reason:
            return f"Startup phase: {_prettify_token(phase_reason)}"
    if lowered in _EVENT_TITLE_OVERRIDES:
        return _EVENT_TITLE_OVERRIDES[lowered]
    if lowered:
        return _prettify_token(lowered)
    return fallback


def _event_badges(payload: Mapping[str, Any], level_name: str, logger_name: str) -> Tuple[str, ...]:
    badges: List[str] = []
    severity = str(payload.get("severity") or "").strip()
    if severity:
        badges.append(severity.upper())
    if payload.get("mode"):
        badges.append(str(payload.get("mode")).upper())
    if payload.get("startup_phase"):
        badges.append(str(payload.get("startup_phase")).upper())
    if payload.get("status") and len(badges) < 3:
        badges.append(str(payload.get("status")).upper())
    if logger_name != "tg_news_userbot" and len(badges) < 3:
        badges.append(logger_name)
    if level_name in {"WARNING", "ERROR", "CRITICAL"} and level_name not in badges:
        badges.insert(0, level_name)
    deduped: List[str] = []
    for badge in badges:
        value = str(badge or "").strip()
        if value and value not in deduped:
            deduped.append(value)
    return tuple(deduped[:3])


def _detail_items(payload: Mapping[str, Any]) -> Tuple[Tuple[str, str], ...]:
    ignored = {"event", "ts"}
    ordered_keys: List[str] = [key for key in _DETAIL_PRIORITY if key in payload and key not in ignored]
    ordered_keys.extend(key for key in payload.keys() if key not in ignored and key not in ordered_keys)
    details: List[Tuple[str, str]] = []
    for key in ordered_keys:
        value = payload.get(key)
        if value is None or key == "summary_html":
            continue
        if key in {"memory_current_bytes", "memory_peak_bytes", "memory_budget_bytes", "memory_swap_bytes"}:
            value_text = _format_bytes(value)
        else:
            value_text = _format_value(value, nested=True)
        if not value_text or value_text == "n/a":
            continue
        details.append((_prettify_token(key), value_text))
        if len(details) >= 6:
            break
    return tuple(details)


def build_runtime_event_view(
    record: logging.LogRecord,
    *,
    sanitize: SanitizeFn | None = None,
) -> RuntimeEventView:
    message = record.getMessage()
    payload = _load_payload(message)
    created = float(getattr(record, "created", 0.0) or 0.0)
    timestamp = datetime.fromtimestamp(created) if created else datetime.utcnow()
    title = "Runtime activity"
    summary = ""
    details: Tuple[Tuple[str, str], ...] = ()
    event_name = ""

    if payload is not None:
        event_name = str(payload.get("event") or "").strip()
        title = _event_title(event_name, payload, "Structured activity")
        summary = _memory_summary(payload) if event_name == "memory_snapshot" else _generic_summary(payload, record.name)
        details = _detail_items(payload)
    else:
        title = message.splitlines()[0].strip() if message.strip() else "Runtime activity"
        if len(message.splitlines()) > 1:
            details = tuple(("Context", " ".join(line.strip() for line in message.splitlines()[1:] if line.strip())),)
        elif record.name != "tg_news_userbot":
            summary = record.name

    traceback_text = ""
    if record.exc_info:
        traceback_text = "".join(traceback.format_exception(*record.exc_info)).strip()
        if not summary:
            exc = record.exc_info[1]
            if exc is not None:
                summary = f"{exc.__class__.__name__}: {exc}"

    category = _event_category(event_name, record.levelno)
    badges = _event_badges(payload or {}, record.levelname, record.name)
    view = RuntimeEventView(
        created_at=created,
        timestamp_short=timestamp.strftime("%H:%M:%S"),
        timestamp_full=timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        level_name=record.levelname,
        levelno=record.levelno,
        logger_name=record.name,
        category=category,
        title=title.strip() or "Runtime activity",
        summary=summary.strip(),
        badges=badges,
        details=details,
        traceback_text=traceback_text,
        event_name=event_name,
    )
    if sanitize is None:
        return view
    return replace(
        view,
        title=sanitize(view.title),
        summary=sanitize(view.summary),
        details=tuple((sanitize(label), sanitize(value)) for label, value in view.details),
        traceback_text=sanitize(view.traceback_text),
    )


def _wrap_line(text: str, *, width: int, initial: str = "", subsequent: str = "") -> List[str]:
    if not text:
        return []
    return textwrap.wrap(
        text,
        width=max(24, width),
        initial_indent=initial,
        subsequent_indent=subsequent or initial,
        replace_whitespace=False,
        drop_whitespace=False,
    )


def render_runtime_event_text(
    view: RuntimeEventView,
    *,
    surface: str,
    color: bool = False,
    width: int | None = None,
) -> str:
    max_width = int(width or _SURFACE_WIDTHS.get(surface, 110))
    badge_text = " ".join(f"[{badge}]" for badge in view.badges)
    header = f"{view.timestamp_short if surface == 'console' else view.timestamp_full}  {_CATEGORY_LABELS.get(view.category, view.category.upper()):<10}  {view.title}"
    if badge_text:
        header = f"{header}  {badge_text}"
    header_lines = _wrap_line(header, width=max_width)

    lines: List[str] = []
    if color and header_lines:
        header_color = _CATEGORY_COLORS.get(view.category, "\033[37m")
        lines.extend(f"{_BOLD}{header_color}{line}{_RESET}" for line in header_lines)
    else:
        lines.extend(header_lines)

    if view.summary:
        summary_lines = _wrap_line(view.summary, width=max_width, initial="  ", subsequent="  ")
        if color:
            lines.extend(f"{_DIM}{line}{_RESET}" for line in summary_lines)
        else:
            lines.extend(summary_lines)

    for label, value in view.details:
        detail_lines = _wrap_line(
            f"{label}: {value}",
            width=max_width,
            initial="    ",
            subsequent="      ",
        )
        if color:
            lines.extend(f"{_DIM}{line}{_RESET}" for line in detail_lines)
        else:
            lines.extend(detail_lines)

    if view.traceback_text:
        lines.append("    Traceback:")
        for raw_line in view.traceback_text.splitlines():
            wrapped = _wrap_line(raw_line, width=max_width, initial="      ", subsequent="      ")
            if color:
                lines.extend(f"{_DIM}{line}{_RESET}" for line in wrapped)
            else:
                lines.extend(wrapped)

    return "\n".join(lines).rstrip()


def terminal_width(default: int = _SURFACE_WIDTHS["console"]) -> int:
    with_width = shutil.get_terminal_size((default, 40)).columns
    return max(72, min(with_width - 2, 160))


class RuntimeActivityFormatter(logging.Formatter):
    """Formatter that renders runtime events as readable activity blocks."""

    def __init__(
        self,
        *,
        surface: str,
        sanitize: SanitizeFn | None = None,
        color: bool = False,
    ) -> None:
        super().__init__()
        self._surface = surface
        self._sanitize = sanitize
        self._color = bool(color)

    def format(self, record: logging.LogRecord) -> str:
        view = build_runtime_event_view(record, sanitize=self._sanitize)
        width = terminal_width() if self._surface == "console" else _SURFACE_WIDTHS.get(self._surface, 110)
        return render_runtime_event_text(view, surface=self._surface, color=self._color, width=width)


class RuntimeActivityBufferHandler(logging.Handler):
    """In-memory recent activity feed for the web surface."""

    def __init__(
        self,
        *,
        capacity: int = 80,
        sanitize: SanitizeFn | None = None,
    ) -> None:
        super().__init__(level=logging.INFO)
        self._capacity = max(10, int(capacity))
        self._sanitize = sanitize
        self._events: Deque[RuntimeEventView] = deque(maxlen=self._capacity)
        self._lock = Lock()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            view = build_runtime_event_view(record, sanitize=self._sanitize)
        except Exception:
            return
        with self._lock:
            self._events.appendleft(view)

    def snapshot(self, *, limit: int = 24) -> List[RuntimeEventView]:
        with self._lock:
            return list(self._events)[: max(1, int(limit))]


def _html_badges(badges: Sequence[str]) -> str:
    return "".join(f'<span class="rt-badge">{escape(str(badge))}</span>' for badge in badges)


def render_runtime_event_html(view: RuntimeEventView) -> str:
    detail_html = "".join(
        f'<div class="rt-detail"><span class="rt-detail-key">{escape(label)}</span>'
        f'<span class="rt-detail-value">{escape(value)}</span></div>'
        for label, value in view.details
    )
    traceback_html = ""
    if view.traceback_text:
        traceback_html = (
            '<details class="rt-traceback"><summary>Traceback</summary>'
            f"<pre>{escape(view.traceback_text)}</pre></details>"
        )
    summary_html = f'<p class="rt-summary">{escape(view.summary)}</p>' if view.summary else ""
    return (
        f'<article class="rt-event rt-{escape(view.category)}">'
        f'<header class="rt-event-head"><span class="rt-time">{escape(view.timestamp_short)}</span>'
        f'<span class="rt-category">{escape(_CATEGORY_LABELS.get(view.category, view.category.upper()))}</span>'
        f'<h3>{escape(view.title)}</h3>'
        f'<div class="rt-badges">{_html_badges(view.badges)}</div></header>'
        f"{summary_html}"
        f'<div class="rt-details">{detail_html}</div>'
        f"{traceback_html}"
        "</article>"
    )


def _dashboard_card(label: str, value: str, tone: str = "") -> str:
    tone_class = f" rt-card-{tone}" if tone else ""
    return (
        f'<section class="rt-card{tone_class}">'
        f'<div class="rt-card-label">{escape(label)}</div>'
        f'<div class="rt-card-value">{escape(value)}</div>'
        "</section>"
    )


def render_dashboard_html(
    status: Mapping[str, Any],
    recent_events: Sequence[RuntimeEventView],
    *,
    health_only: bool = False,
) -> str:
    service = str(status.get("service") or "TeleUserBot")
    ready = bool(status.get("ready"))
    ok = bool(status.get("ok"))
    phase = str(status.get("phase") or "unknown")
    title = f"{service} Health" if health_only else f"{service} Operator Desk"
    auth = status.get("auth") if isinstance(status.get("auth"), Mapping) else {}
    auth_ready = bool(auth.get("ready")) if isinstance(auth, Mapping) else False
    auth_status = str(auth.get("status") or "unknown") if isinstance(auth, Mapping) else "unknown"
    queue_pending = str(status.get("pending_queue") or 0)
    queue_inflight = str(status.get("inflight_queue") or 0)
    mode = str(status.get("mode") or "unknown")
    sources = str(status.get("sources_monitored") or 0)
    started_as = str(status.get("started_as") or "unknown")
    timestamp = status.get("timestamp")
    if isinstance(timestamp, (int, float)) and timestamp > 0:
        refreshed = datetime.fromtimestamp(float(timestamp)).strftime("%Y-%m-%d %H:%M:%S")
    else:
        refreshed = "unknown"

    cards = [
        _dashboard_card("Phase", _prettify_token(phase), "alert" if not ready else "ok"),
        _dashboard_card("Mode", mode),
        _dashboard_card("Queue", f"{queue_pending} pending / {queue_inflight} inflight"),
        _dashboard_card("Sources", sources),
        _dashboard_card("Operator", started_as),
        _dashboard_card("Auth", auth_status, "alert" if not auth_ready else "ok"),
    ]
    if health_only:
        cards = cards[:4]

    events_html = "".join(render_runtime_event_html(event) for event in recent_events[:18])
    if not events_html:
        events_html = '<div class="rt-empty">No recent activity captured yet.</div>'

    hero_state = "Ready" if ready and ok else "Attention needed"
    hero_class = "rt-hero-ok" if ready and ok else "rt-hero-alert"
    feed_heading = "Recent activity" if not health_only else "Latest signals"
    status_label = "ready" if ready and ok else "degraded"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    :root {{
      color-scheme: dark light;
      --bg: oklch(0.16 0.02 255);
      --panel: oklch(0.22 0.02 255);
      --panel-soft: oklch(0.26 0.02 255);
      --text: oklch(0.92 0.01 95);
      --muted: oklch(0.72 0.02 95);
      --line: oklch(0.38 0.02 255);
      --accent: oklch(0.78 0.08 75);
      --accent-soft: oklch(0.44 0.05 75);
      --danger: oklch(0.68 0.16 22);
      --success: oklch(0.76 0.12 155);
      --shadow: 0 32px 90px rgba(0, 0, 0, 0.35);
    }}
    @media (prefers-color-scheme: light) {{
      :root {{
        --bg: oklch(0.95 0.01 95);
        --panel: oklch(0.99 0.01 95);
        --panel-soft: oklch(0.96 0.01 95);
        --text: oklch(0.24 0.02 255);
        --muted: oklch(0.45 0.02 255);
        --line: oklch(0.84 0.01 255);
        --accent: oklch(0.56 0.09 75);
        --accent-soft: oklch(0.88 0.03 75);
        --danger: oklch(0.56 0.14 22);
        --success: oklch(0.52 0.11 155);
        --shadow: 0 24px 70px rgba(24, 30, 40, 0.08);
      }}
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      background:
        radial-gradient(circle at top left, color-mix(in oklab, var(--accent) 16%, transparent), transparent 34%),
        radial-gradient(circle at top right, color-mix(in oklab, var(--danger) 12%, transparent), transparent 28%),
        linear-gradient(180deg, color-mix(in oklab, var(--bg) 92%, black), var(--bg));
      color: var(--text);
      font-family: "Avenir Next", "Segoe UI Variable Text", "Trebuchet MS", sans-serif;
      padding: clamp(18px, 3vw, 34px);
    }}
    .rt-shell {{
      max-width: 1320px;
      margin: 0 auto;
      background: color-mix(in oklab, var(--panel) 92%, transparent);
      border: 1px solid color-mix(in oklab, var(--line) 82%, transparent);
      border-radius: 28px;
      box-shadow: var(--shadow);
      overflow: hidden;
      backdrop-filter: blur(12px);
    }}
    .rt-topbar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 24px;
      border-bottom: 1px solid var(--line);
      background: color-mix(in oklab, var(--panel-soft) 86%, transparent);
    }}
    .rt-brand {{
      display: flex;
      flex-direction: column;
      gap: 4px;
    }}
    .rt-kicker {{
      color: var(--muted);
      letter-spacing: 0.18em;
      text-transform: uppercase;
      font-size: 0.72rem;
    }}
    .rt-title {{
      font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
      font-size: clamp(1.5rem, 2.5vw, 2.3rem);
      line-height: 1.05;
      margin: 0;
    }}
    .rt-meta {{
      color: var(--muted);
      font-size: 0.92rem;
    }}
    .rt-hero {{
      display: grid;
      grid-template-columns: 1.4fr 1fr;
      gap: 24px;
      padding: 24px;
      border-bottom: 1px solid var(--line);
      background:
        linear-gradient(135deg, color-mix(in oklab, var(--accent-soft) 26%, transparent), transparent 66%),
        color-mix(in oklab, var(--panel) 94%, transparent);
    }}
    .rt-hero-copy {{
      display: flex;
      flex-direction: column;
      gap: 14px;
    }}
    .rt-state {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      width: fit-content;
      padding: 8px 14px;
      border-radius: 999px;
      font-size: 0.78rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      background: color-mix(in oklab, var(--panel-soft) 90%, transparent);
      border: 1px solid var(--line);
    }}
    .rt-hero-ok .rt-state-dot {{ background: var(--success); }}
    .rt-hero-alert .rt-state-dot {{ background: var(--danger); }}
    .rt-state-dot {{
      width: 10px;
      height: 10px;
      border-radius: 50%;
      display: inline-block;
      box-shadow: 0 0 0 6px color-mix(in oklab, currentColor 10%, transparent);
    }}
    .rt-hero h2 {{
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Georgia, serif;
      font-size: clamp(1.8rem, 4vw, 3rem);
      line-height: 1;
    }}
    .rt-hero p {{
      margin: 0;
      max-width: 44rem;
      color: var(--muted);
      font-size: 1rem;
      line-height: 1.6;
    }}
    .rt-card-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 16px;
    }}
    .rt-card {{
      padding: 18px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: color-mix(in oklab, var(--panel-soft) 90%, transparent);
      min-height: 108px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }}
    .rt-card-ok {{
      border-color: color-mix(in oklab, var(--success) 44%, var(--line));
      box-shadow: inset 0 0 0 1px color-mix(in oklab, var(--success) 20%, transparent);
    }}
    .rt-card-alert {{
      border-color: color-mix(in oklab, var(--danger) 48%, var(--line));
      box-shadow: inset 0 0 0 1px color-mix(in oklab, var(--danger) 18%, transparent);
    }}
    .rt-card-label {{
      color: var(--muted);
      letter-spacing: 0.08em;
      text-transform: uppercase;
      font-size: 0.75rem;
    }}
    .rt-card-value {{
      font-size: clamp(1rem, 1.7vw, 1.3rem);
      line-height: 1.2;
      font-weight: 600;
    }}
    .rt-feed {{
      display: grid;
      grid-template-columns: 0.9fr 1.1fr;
      gap: 24px;
      padding: 24px;
    }}
    .rt-panel {{
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 20px;
      background: color-mix(in oklab, var(--panel-soft) 88%, transparent);
    }}
    .rt-panel h3 {{
      margin: 0 0 12px;
      font-size: 1rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }}
    .rt-list {{
      display: flex;
      flex-direction: column;
      gap: 14px;
    }}
    .rt-event {{
      padding: 16px 18px;
      border-radius: 18px;
      border: 1px solid color-mix(in oklab, var(--line) 78%, transparent);
      background: color-mix(in oklab, var(--panel) 94%, transparent);
    }}
    .rt-event-head {{
      display: grid;
      grid-template-columns: auto auto 1fr auto;
      gap: 12px;
      align-items: center;
    }}
    .rt-time, .rt-category {{
      color: var(--muted);
      font-size: 0.78rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}
    .rt-event h3 {{
      margin: 0;
      font-size: 1.02rem;
      line-height: 1.35;
    }}
    .rt-badges {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }}
    .rt-badge {{
      padding: 5px 10px;
      border-radius: 999px;
      background: color-mix(in oklab, var(--accent) 10%, var(--panel-soft));
      border: 1px solid color-mix(in oklab, var(--accent) 24%, var(--line));
      color: var(--text);
      font-size: 0.72rem;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }}
    .rt-summary {{
      margin: 12px 0 0;
      color: var(--text);
      line-height: 1.6;
    }}
    .rt-details {{
      display: grid;
      gap: 8px;
      margin-top: 14px;
    }}
    .rt-detail {{
      display: grid;
      grid-template-columns: minmax(130px, 170px) 1fr;
      gap: 10px;
      color: var(--muted);
      line-height: 1.55;
    }}
    .rt-detail-key {{
      color: color-mix(in oklab, var(--muted) 82%, var(--text));
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 0.74rem;
    }}
    .rt-detail-value {{
      color: var(--text);
      word-break: break-word;
    }}
    .rt-traceback {{
      margin-top: 14px;
      border-top: 1px solid var(--line);
      padding-top: 12px;
    }}
    .rt-traceback summary {{
      cursor: pointer;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 0.74rem;
    }}
    .rt-traceback pre {{
      margin: 12px 0 0;
      padding: 14px;
      border-radius: 14px;
      overflow: auto;
      background: color-mix(in oklab, var(--bg) 90%, transparent);
      color: var(--text);
      font-family: "Consolas", "SFMono-Regular", "Menlo", monospace;
      font-size: 0.83rem;
      line-height: 1.55;
    }}
    .rt-empty {{
      padding: 24px;
      border-radius: 18px;
      border: 1px dashed var(--line);
      color: var(--muted);
    }}
    @media (max-width: 980px) {{
      .rt-hero,
      .rt-feed {{
        grid-template-columns: 1fr;
      }}
      .rt-card-grid {{
        grid-template-columns: 1fr 1fr;
      }}
      .rt-event-head {{
        grid-template-columns: auto auto 1fr;
      }}
      .rt-badges {{
        grid-column: 1 / -1;
        justify-content: flex-start;
      }}
    }}
    @media (max-width: 640px) {{
      body {{ padding: 12px; }}
      .rt-topbar, .rt-hero, .rt-feed {{ padding: 16px; }}
      .rt-card-grid {{ grid-template-columns: 1fr; }}
      .rt-detail {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <main class="rt-shell">
    <header class="rt-topbar">
      <div class="rt-brand">
        <div class="rt-kicker">Newsroom OS</div>
        <h1 class="rt-title">{escape(title)}</h1>
        <div class="rt-meta">Refreshed {escape(refreshed)}</div>
      </div>
      <div class="{hero_class}">
        <div class="rt-state"><span class="rt-state-dot"></span>{escape(hero_state)}</div>
      </div>
    </header>
    <section class="rt-hero {hero_class}">
      <div class="rt-hero-copy">
        <div class="rt-state"><span class="rt-state-dot"></span>{escape(hero_state)}</div>
        <h2>{escape(_prettify_token(phase))}</h2>
        <p>{escape(service)} is {escape(status_label)} in {escape(mode)} mode for {escape(started_as)}. Auth is {escape(auth_status)}, and the desk is tracking {escape(sources)} monitored sources with {escape(queue_pending)} pending items.</p>
      </div>
      <div class="rt-card-grid">
        {''.join(cards)}
      </div>
    </section>
    <section class="rt-feed">
      <section class="rt-panel">
        <h3>Operational frame</h3>
        <div class="rt-list">
          <div class="rt-event">
            <header class="rt-event-head">
              <span class="rt-time">Now</span>
              <span class="rt-category">State</span>
              <h3>{escape(service)} is {escape(status_label)}</h3>
            </header>
            <p class="rt-summary">Phase: {escape(_prettify_token(phase))}. Auth: {escape(auth_status)}. Queue: {escape(queue_pending)} pending, {escape(queue_inflight)} inflight.</p>
            <div class="rt-details">
              <div class="rt-detail"><span class="rt-detail-key">Health</span><span class="rt-detail-value">{escape('ok' if ok else 'error')}</span></div>
              <div class="rt-detail"><span class="rt-detail-key">Operator</span><span class="rt-detail-value">{escape(started_as)}</span></div>
              <div class="rt-detail"><span class="rt-detail-key">Sources</span><span class="rt-detail-value">{escape(sources)}</span></div>
              <div class="rt-detail"><span class="rt-detail-key">Auth ready</span><span class="rt-detail-value">{escape('yes' if auth_ready else 'no')}</span></div>
            </div>
          </div>
        </div>
      </section>
      <section class="rt-panel">
        <h3>{escape(feed_heading)}</h3>
        <div class="rt-list">{events_html}</div>
      </section>
    </section>
  </main>
</body>
</html>"""
