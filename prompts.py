"""Centralized prompt templates for digest/query/classification with Telegram HTML output."""

from __future__ import annotations

from typing import Iterable


def quiet_period_message(interval_minutes: int) -> str:
    _ = interval_minutes
    return "<b>🟢 No major developments right now.</b>"


HTML_RULES = """
Output format is STRICT Telegram HTML only.
Allowed tags only: <b>, <i>, <u>, <s>, <tg-spoiler>, <code>, <pre>, <blockquote>, <a href="...">, <br>
Never output Markdown. Never output unsupported tags. Keep tags valid and closed.
""".strip()


DIGEST_PROMPT_CORE = """
You are an elite real-time news editor for Telegram digests.
Your job: compress noisy channel posts into ultra-short, high-signal updates.

Core rules:
1) Translate source text into OUTPUT_LANGUAGE when needed.
2) Remove promo/spam/noise/polls/meme chatter.
3) Merge duplicates and paraphrased echoes.
4) Keep only major, actionable developments.
5) Output between 3 and 12 lines total when meaningful updates exist.
6) Every line must be a short headline:
   <emoji> <headline> <i>[source]</i>
7) Severity emoji:
   🔥 = high impact / urgent escalation
   ⚠️ = medium impact / meaningful update
   ℹ️ = low-impact but useful context
8) Prioritize highest-severity updates first.
9) If no significant updates remain, output exactly:
   QUIET_PERIOD_SENTINEL
""".strip()


def build_digest_system_prompt(
    *,
    interval_minutes: int,
    json_mode: bool,
    importance_scoring: bool,
    include_links: bool,
    output_language: str,
    include_source_tags: bool,
) -> str:
    _ = json_mode  # kept for compatibility
    prompt = DIGEST_PROMPT_CORE.replace(
        "QUIET_PERIOD_SENTINEL",
        quiet_period_message(interval_minutes),
    )
    toggles = [
        f"Output language must be {output_language}.",
        f"Include source tags: {'yes' if include_source_tags else 'no'} (if no, omit source brackets).",
        f"Include links when reliable and available: {'yes' if include_links else 'no'}.",
        f"Importance scoring hint enabled: {'yes' if importance_scoring else 'no'}.",
        HTML_RULES,
    ]
    if include_links:
        toggles.append('When adding links, use <a href="https://...">Read more</a>.')
    return f"{prompt}\n\n" + "\n".join(toggles)


def build_digest_input_block(lines: Iterable[str]) -> str:
    joined = "\n".join(line for line in lines if line.strip())
    return f"Raw posts:\n{joined}" if joined else "Raw posts:\n"


QUERY_NO_MATCH_TEXT = "<b>🟢 No relevant information found.</b>"

QUERY_SYSTEM_PROMPT = """
You are a precise multilingual news analyst with access to provided Telegram context.
Answer only from evidence in context. No fabrication.

Answer rules:
1) Keep answer concise unless user asks for deep detail.
2) Use HTML only. No Markdown.
3) Suggested structure:
   <b>Short title</b><br>
   • <emoji> headline/fact<br>
   • <emoji> headline/fact<br>
4) Use <u> for important dates, locations, numbers.
5) Use <tg-spoiler> for uncertain or unverified details.
6) Cite sources as <i>[Source]</i> and include direct links when available:
   <a href="https://...">source</a>
7) If no relevant evidence exists, output exactly:
   NO_MATCH_SENTINEL
""".strip()


def build_query_system_prompt(*, output_language: str, detailed: bool) -> str:
    mode_line = (
        "Detail mode enabled: include brief sectioned analysis while staying concise."
        if detailed
        else "Detail mode disabled: keep response short and headline-first."
    )
    base = QUERY_SYSTEM_PROMPT.replace("NO_MATCH_SENTINEL", QUERY_NO_MATCH_TEXT)
    return (
        f"{base}\n\n"
        f"Output language must be {output_language}. Translate source text when needed.\n"
        f"{HTML_RULES}\n"
        f"{mode_line}"
    )

