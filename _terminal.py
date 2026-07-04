"""Terminal UI components using rich."""

import os
import sys
import time
from contextlib import asynccontextmanager, contextmanager
from typing import Any

from rich.align import Align
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.text import Text
from rich import box

_console = Console(stderr=True, highlight=False, emoji=True)


def _supports_color() -> bool:
    forced = str(os.getenv("CLI_COLOR", "") or "").strip().lower()
    if forced in {"1", "true", "yes", "on"}:
        return True
    if forced in {"0", "false", "no", "off"}:
        return False
    if os.getenv("NO_COLOR"):
        return False
    return _console.is_terminal and _console.color_system is not None


_COLOR_ON = _supports_color()


def _is_interactive() -> bool:
    return sys.stdin.isatty() and _COLOR_ON


def print_banner() -> None:
    if not _is_interactive():
        return
    panel = Panel(
        Text("Telegram News Intelligence\nUserbot", style="bold cyan", justify="center"),
        subtitle=Text(
            "Single entrypoint: python main.py",
            style="dim cyan",
            justify="center",
        ),
        subtitle_align="center",
        box=box.HEAVY,
        border_style="cyan",
        padding=(1, 2),
        width=min(_console.width, 72),
    )
    _console.print()
    _console.print(Align.center(panel))
    _console.print()


_last_status_sig: tuple[str, str] | None = None
_last_status_at: float = 0.0


def print_status(symbol: str, text: str, *, level: str = "info") -> None:
    global _last_status_sig, _last_status_at
    if not _is_interactive():
        return
    if text == "OpenAI auth ready" and not getattr(
        sys.modules.get("config", None), "_auth_ready_placeholder", True
    ):
        return
    now = time.time()
    sig = (level, str(text).strip())
    if _last_status_sig == sig and (now - _last_status_at) < 0.5:
        return
    _last_status_sig = sig
    _last_status_at = now
    style = {"ok": "bold green", "warn": "bold yellow", "error": "bold red"}.get(
        level, "bold cyan"
    )
    _console.print(f"[{style}]{symbol}[/] {text}")


def print_config_summary(items: list[tuple[str, str]]) -> None:
    if not _is_interactive():
        return
    table = Table(box=box.SIMPLE, border_style="dim cyan", padding=(0, 1), show_header=False)
    table.add_column("Key", style="bold", no_wrap=True)
    table.add_column("Value", style="")
    for key, value in items:
        table.add_row(key, value)
    _console.print(Align.center(table))
    _console.print()


@asynccontextmanager
async def spinner(description: str = "Working..."):
    if not _is_interactive():
        yield
        return
    with _console.status(f"[bold cyan]{description}", spinner="dots"):
        yield


@contextmanager
def spinner_sync(description: str = "Working..."):
    if not _is_interactive():
        yield
        return
    with _console.status(f"[bold cyan]{description}", spinner="dots"):
        yield


def print_error(message: str) -> None:
    _console.print(f"[bold red]✗[/] {message}")


def print_warning(message: str) -> None:
    _console.print(f"[bold yellow]![/] {message}")


def print_success(message: str) -> None:
    _console.print(f"[bold green]✓[/] {message}")
