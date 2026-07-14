"""Shared HTTP clients for long-lived runtime workloads."""

from __future__ import annotations

import asyncio
import os
import threading
from typing import Dict

import httpx
from curl_cffi import requests as curl_requests

_LOCKS_BY_LOOP: Dict[int, asyncio.Lock] = {}
_LOCK_THREAD_GUARD = threading.Lock()
_CLIENTS: Dict[str, httpx.AsyncClient | curl_requests.AsyncSession] = {}


def _get_async_lock() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    loop_id = id(loop)
    with _LOCK_THREAD_GUARD:
        lock = _LOCKS_BY_LOOP.get(loop_id)
        if lock is None:
            lock = asyncio.Lock()
            _LOCKS_BY_LOOP[loop_id] = lock
        return lock


def _default_limits() -> httpx.Limits:
    """Return sensible connection pool limits from env or defaults."""
    try:
        max_connections = int(os.getenv("HTTP_MAX_CONNECTIONS", "50"))
    except Exception:
        max_connections = 50
    try:
        max_keepalive = int(os.getenv("HTTP_MAX_KEEPALIVE", "10"))
    except Exception:
        max_keepalive = 10
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
    lock = _get_async_lock()
    async with lock:
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
    try:
        total_timeout = float(os.getenv("CODEX_HTTP_TIMEOUT", "90.0"))
    except Exception:
        total_timeout = 90.0
    lock = _get_async_lock()
    async with lock:
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
    try:
        timeout = float(os.getenv("AUTH_HTTP_TIMEOUT", "30.0"))
    except Exception:
        timeout = 30.0
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
    try:
        timeout = float(os.getenv("WEB_HTTP_TIMEOUT", "20.0"))
    except Exception:
        timeout = 20.0
    return await _get_or_create(
        "web_search",
        timeout=httpx.Timeout(timeout),
        follow_redirects=True,
    )


async def reset_shared_http_client(name: str) -> None:
    lock = _get_async_lock()
    async with lock:
        client = _CLIENTS.pop(name, None)
    if client is not None:
        try:
            if isinstance(client, httpx.AsyncClient):
                await client.aclose()
            else:
                await client.close()
        except Exception:
            pass


async def close_shared_http_clients() -> None:
    lock = _get_async_lock()
    async with lock:
        clients = list(_CLIENTS.values())
        _CLIENTS.clear()
        with _LOCK_THREAD_GUARD:
            _LOCKS_BY_LOOP.clear()
    for client in clients:
        try:
            if isinstance(client, httpx.AsyncClient):
                await client.aclose()
            else:
                await client.close()
        except Exception:
            pass
