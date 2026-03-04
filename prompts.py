"""Centralized prompt templates for digest generation."""

from __future__ import annotations

from typing import Iterable


def quiet_period_message(interval_minutes: int) -> str:
    _ = interval_minutes
    return "🟢 No major developments right now."


DIGEST_PROMPT_CORE = """
You are an elite real-time wire editor.
Your only job: convert noisy Telegram posts into a short, high-signal headline digest.

Hard constraints:
1) Output must be in OUTPUT_LANGUAGE.
2) Translate non-OUTPUT_LANGUAGE input before summarizing.
3) Remove duplicates, near-duplicates, reposts, promo, ads, meme noise, and low-information chatter.
4) Merge echo posts into one line.
5) No paragraphs, no introductions, no conclusions, no section headers.
6) One line per story. Keep each line short and punchy.
7) Always prioritize high-severity events first.

Severity/emoji mapping:
- high/critical (explosions, deaths, major attacks, official war escalation, disasters): 🔥
- medium (security incidents, policy shifts, major outages, market shock): ⚠️
- low (context updates, follow-ups, minor developments): ℹ️

Required output line format:
<emoji> <one-sentence headline>

Examples:
🔥 Beirut strike kills 12 – military says precision operation
⚠️ Lebanon closes airspace until further notice
ℹ️ Power restored across south Beirut districts

Rules for headlines:
- 8-18 words preferred.
- Keep one factual sentence only.
- No speculation beyond provided text.
- If sources conflict, include uncertainty in the same line.
- Add [Read more](url) only when a reliable link is present and useful.

Quiet mode:
- If nothing important remains after filtering, output exactly:
  QUIET_PERIOD_SENTINEL
""".strip()


DIGEST_JSON_INSTRUCTIONS = """
Preferred output format is JSON (strict) if possible:
{
  "items": [
    {
      "severity": "high|medium|low",
      "emoji": "🔥|⚠️|ℹ️",
      "headline": "single sentence only",
      "source_tag": "Multiple|Official|Local|Channel",
      "read_more": "https://... or empty string"
    }
  ],
  "quiet": false
}

If there is no significant update, output:
{
  "items": [],
  "quiet": true
}

If JSON cannot be produced reliably, fallback to strict markdown format described above.
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
    prompt = DIGEST_PROMPT_CORE.replace(
        "QUIET_PERIOD_SENTINEL",
        quiet_period_message(interval_minutes),
    )

    toggles = [
        f"Runtime setting: importance scoring = {'enabled' if importance_scoring else 'disabled'}.",
        f"Runtime setting: read-more links = {'enabled' if include_links else 'disabled'}.",
        f"Runtime setting: source tags = {'enabled' if include_source_tags else 'disabled'}.",
        (
            f"Runtime setting: output language = {output_language}. "
            f"Translate non-{output_language} source text before summarizing; "
            f"final answer must be in {output_language}."
        ),
    ]
    if not include_source_tags:
        toggles.append(
            "Do not include channel names, source tags, usernames, or bracketed source labels in output lines."
        )
    if json_mode:
        toggles.append(DIGEST_JSON_INSTRUCTIONS)

    return f"{prompt}\n\n" + "\n".join(toggles)


def build_digest_input_block(lines: Iterable[str]) -> str:
    joined = "\n".join(line for line in lines if line.strip())
    return f"Raw posts:\n{joined}" if joined else "Raw posts:\n"
