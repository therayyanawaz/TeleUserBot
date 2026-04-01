from __future__ import annotations

import http.client
import logging

from runtime_presenter import RuntimeEventView
from web_server import WebStatusServer


def _sample_event(title: str) -> RuntimeEventView:
    return RuntimeEventView(
        created_at=1775072206.0,
        timestamp_short="19:36:46",
        timestamp_full="2026-04-01 19:36:46",
        level_name="INFO",
        levelno=logging.INFO,
        logger_name="tg_news_userbot",
        category="digest",
        title=title,
        summary="Digest batches are moving again after the scheduler recovered.",
        badges=("DIGEST",),
        details=(("Queue", "12 pending / 1 inflight"),),
        traceback_text="",
        event_name="digest_scheduler_start",
    )


def _get_response(server: WebStatusServer, path: str) -> tuple[int, str, str]:
    connection = http.client.HTTPConnection("127.0.0.1", server.port, timeout=5)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        body = response.read().decode("utf-8")
        content_type = response.getheader("Content-Type", "")
        return response.status, content_type, body
    finally:
        connection.close()


def test_status_and_root_routes_render_dashboard_html():
    server = WebStatusServer(
        host="127.0.0.1",
        port=0,
        get_status=lambda: {
            "ok": True,
            "ready": True,
            "phase": "running",
            "service": "NetworkSlutter",
            "mode": "DIGEST",
            "started_as": "@desk",
            "timestamp": 1775072206,
            "pending_queue": 12,
            "inflight_queue": 1,
            "sources_monitored": 7,
            "auth": {"status": "ready", "ready": True},
        },
        get_recent_events=lambda limit: [_sample_event("Digest scheduler online")][:limit],
        logger=logging.getLogger("test.web.status"),
    )
    server.start()
    try:
        for path in ("/", "/status"):
            status, content_type, body = _get_response(server, path)
            assert status == 200
            assert content_type.startswith("text/html")
            assert "Operator Desk" in body
            assert "Digest scheduler online" in body
            assert "Recent activity" in body
    finally:
        server.stop()


def test_health_route_returns_html_and_preserves_status_codes():
    healthy = WebStatusServer(
        host="127.0.0.1",
        port=0,
        get_status=lambda: {
            "ok": True,
            "ready": True,
            "phase": "running",
            "service": "NetworkSlutter",
            "mode": "DIGEST",
            "started_as": "@desk",
            "timestamp": 1775072206,
            "pending_queue": 0,
            "inflight_queue": 0,
            "sources_monitored": 7,
            "auth": {"status": "ready", "ready": True},
        },
        get_recent_events=lambda limit: [_sample_event("Memory snapshot")][:limit],
        logger=logging.getLogger("test.web.health.ok"),
    )
    healthy.start()
    try:
        status, content_type, body = _get_response(healthy, "/health")
        assert status == 200
        assert content_type.startswith("text/html")
        assert "Health" in body
        assert "Latest signals" in body
    finally:
        healthy.stop()

    unhealthy = WebStatusServer(
        host="127.0.0.1",
        port=0,
        get_status=lambda: {
            "ok": False,
            "ready": False,
            "phase": "error",
            "service": "NetworkSlutter",
            "mode": "DIGEST",
            "started_as": "@desk",
            "timestamp": 1775072206,
            "pending_queue": 40,
            "inflight_queue": 3,
            "sources_monitored": 7,
            "auth": {"status": "degraded", "ready": False},
        },
        get_recent_events=lambda limit: [_sample_event("Startup failure")][:limit],
        logger=logging.getLogger("test.web.health.bad"),
    )
    unhealthy.start()
    try:
        status, content_type, body = _get_response(unhealthy, "/health")
        assert status == 503
        assert content_type.startswith("text/html")
        assert "Attention needed" in body
        assert "Startup failure" in body
    finally:
        unhealthy.stop()
