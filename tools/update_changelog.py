"""Generate CHANGELOG.md from conventional commit history."""

from __future__ import annotations

import re
import subprocess
from collections import OrderedDict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
CHANGELOG_PATH = REPO_ROOT / "CHANGELOG.md"

COMMIT_RE = re.compile(r"^(?P<type>[a-z]+)(?:\([^)]+\))?(?P<breaking>!)?: (?P<desc>.+)$")
SECTION_TITLES = {
    "feat": "Features",
    "fix": "Fixes",
    "refactor": "Refactors",
    "perf": "Performance",
    "docs": "Docs",
    "test": "Tests",
    "ci": "CI",
    "build": "Build",
    "chore": "Chores",
    "revert": "Reverts",
}


def _git_log() -> list[tuple[str, str, str]]:
    result = subprocess.run(
        [
            "git",
            "log",
            "--date=short",
            "--pretty=format:%h%x1f%ad%x1f%s",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    rows: list[tuple[str, str, str]] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        short_sha, commit_date, subject = line.split("\x1f", 2)
        rows.append((short_sha.strip(), commit_date.strip(), subject.strip()))
    return rows


def _section_for_subject(subject: str) -> str:
    match = COMMIT_RE.match(subject)
    if not match:
        return "Other"
    commit_type = match.group("type").lower()
    if match.group("breaking"):
        return "Breaking"
    return SECTION_TITLES.get(commit_type, "Other")


def render_changelog() -> str:
    grouped: "OrderedDict[str, OrderedDict[str, list[tuple[str, str]]]]" = OrderedDict()
    for short_sha, commit_date, subject in _git_log():
        grouped.setdefault(commit_date, OrderedDict())
        section = _section_for_subject(subject)
        grouped[commit_date].setdefault(section, [])
        grouped[commit_date][section].append((short_sha, subject))

    lines = [
        "# Changelog",
        "",
        "> Auto-generated from git history. Prefer conventional commits.",
        "",
    ]
    for commit_date, sections in grouped.items():
        lines.append(f"## {commit_date}")
        lines.append("")
        for section, items in sections.items():
            lines.append(f"### {section}")
            lines.append("")
            for short_sha, subject in items:
                lines.append(f"- `{short_sha}` {subject}")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    CHANGELOG_PATH.write_text(render_changelog(), encoding="utf-8")


if __name__ == "__main__":
    main()
