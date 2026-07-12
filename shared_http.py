"""Shared HTTP clients for long-lived runtime workloads."""

from __future__ import annotations

import asyncio
import os
from typing import Dict

import httpx
from curl_cffi import requests as curl_requests

_CLIENT_LOCK = asyncio.Lock()
_CLIENTS: Dict[str, httpx.AsyncClient | curl_requests.AsyncSession] = {}


def _default_limits() -> httpx.Limits:
    """Return sensible connection pool limits from env or defaults."""
    max_connections = int(os.getenv("HTTP_MAX_CONNECTIONS", "50"))
    max_keepalive = int(os.getenv("HTTP_MAX_KEEPALIVE", "10"))
    return httpx.Limits(
        max_connections=max_connections,
        max_keepalive_connections=max_keepalive,
    )


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
            limits=_default_limits(),
            trust_env=False,
        )
        _CLIENTS[name] = created
        return created


async def get_codex_http_client() -> curl_requests.AsyncSession:
    total_timeout = float(os.getenv("CODEX_HTTP_TIMEOUT", "90.0"))
    async with _CLIENT_LOCK:
        client = _CLIENTS.get("codex")
        if client is not None:
            return client
        created = curl_requests.AsyncSession(
            timeout=total_timeout,
            impersonate="chrome",
        )
        _CLIENTS["codex"] = created
        return created


async def get_auth_http_client() -> httpx.AsyncClient:
    timeout = float(os.getenv("AUTH_HTTP_TIMEOUT", "30.0"))
    return await _get_or_create(
        "auth",
        timeout=httpx.Timeout(timeout),
    )


async def get_bot_http_client(timeout: httpx.Timeout | float | int) -> httpx.AsyncClient:
    # Reuse a single client for all bot API traffic. The first caller determines
    # the concrete timeout profile; later calls reuse the same pool.
    return await _get_or_create(
        "bot_api",
        timeout=timeout,
    )


async def get_web_http_client() -> httpx.AsyncClient:
    timeout = float(os.getenv("WEB_HTTP_TIMEOUT", "20.0"))
    return await _get_or_create(
        "web_search",
        timeout=httpx.Timeout(timeout),
        follow_redirects=True,
    )


async def reset_shared_http_client(name: str) -> None:
    async with _CLIENT_LOCK:
        client = _CLIENTS.pop(name, None)
    if client is not None:
        if isinstance(client, httpx.AsyncClient):
            await client.aclose()
        else:
            await client.close()


async def close_shared_http_clients() -> None:
    async with _CLIENT_LOCK:
        clients = list(_CLIENTS.values())
        _CLIENTS.clear()
    for client in clients:
        if isinstance(client, httpx.AsyncClient):
            await client.aclose()
        else:
            await client.close()
