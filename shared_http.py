"""Shared HTTP clients for long-lived runtime workloads."""

from __future__ import annotations

import asyncio
from typing import Dict

import httpx


_CLIENT_LOCK = asyncio.Lock()
_CLIENTS: Dict[str, httpx.AsyncClient] = {}


async def _get_or_create(
    name: str,
    *,
    timeout: httpx.Timeout | float | int,
    follow_redirects: bool = False,
) -> httpx.AsyncClient:
    async with _CLIENT_LOCK:
        client = _CLIENTS.get(name)
        if client is not None:
            return client
        created = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=follow_redirects,
        )
        _CLIENTS[name] = created
        return created


async def get_codex_http_client() -> httpx.AsyncClient:
    return await _get_or_create(
        "codex",
        timeout=httpx.Timeout(90.0, connect=20.0),
    )


async def get_auth_http_client() -> httpx.AsyncClient:
    return await _get_or_create(
        "auth",
        timeout=httpx.Timeout(30.0),
    )


async def get_bot_http_client(timeout: httpx.Timeout | float | int) -> httpx.AsyncClient:
    # Reuse a single client for all bot API traffic. The first caller determines
    # the concrete timeout profile; later calls reuse the same pool.
    return await _get_or_create(
        "bot_api",
        timeout=timeout,
    )


async def get_web_http_client() -> httpx.AsyncClient:
    return await _get_or_create(
        "web_search",
        timeout=httpx.Timeout(20.0),
        follow_redirects=True,
    )


async def reset_shared_http_client(name: str) -> None:
    async with _CLIENT_LOCK:
        client = _CLIENTS.pop(name, None)
    if client is not None:
        await client.aclose()


async def close_shared_http_clients() -> None:
    async with _CLIENT_LOCK:
        clients = list(_CLIENTS.values())
        _CLIENTS.clear()
    for client in clients:
        await client.aclose()
