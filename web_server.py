"""Operator-facing HTTP status server."""

from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Sequence

from runtime_presenter import RuntimeEventView, render_dashboard_html


class WebStatusServer:
    """Background HTTP server exposing human-readable operator dashboards."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        get_status: Callable[[], Dict[str, Any]],
        get_recent_events: Callable[[int], Sequence[RuntimeEventView]],
        logger: logging.Logger,
    ) -> None:
        self._host = host
        self._port = int(port)
        self._get_status = get_status
        self._get_recent_events = get_recent_events
        self._logger = logger
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._server is not None:
            return

        get_status = self._get_status
        get_recent_events = self._get_recent_events

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: Any) -> None:
                return

            def _send_html(self, status_code: int, payload: str) -> None:
                raw = str(payload or "").encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self) -> None:  # noqa: N802
                path = self.path.split("?", 1)[0]
                try:
                    status = get_status()
                except Exception:
                    # Never fail health endpoints due to upstream startup races.
                    status = {
                        "ok": False,
                        "service": "NetworkSlutter",
                        "error": "status_unavailable",
                    }
                    try:
                        # BaseHTTPRequestHandler has no shared logger; use outer logger.
                        logger = getattr(self.server, "_logger", None)
                        if logger is not None:
                            logger.exception("Web status payload generation failed.")
                    except Exception:
                        pass
                status_dict = status if isinstance(status, dict) else {}
                try:
                    recent_events = list(get_recent_events(24))
                except Exception:
                    recent_events = []

                if path == "/health":
                    self._send_html(
                        200 if bool(status_dict.get("ok", True)) else 503,
                        render_dashboard_html(
                            status_dict or {"ok": False},
                            recent_events,
                            health_only=True,
                        ),
                    )
                    return
                if path in {"/", "/status"}:
                    self._send_html(
                        200,
                        render_dashboard_html(
                            status_dict or {"ok": False},
                            recent_events,
                            health_only=False,
                        ),
                    )
                    return
                self._send_html(
                    404,
                    render_dashboard_html(
                        {
                            "ok": False,
                            "ready": False,
                            "phase": "not_found",
                            "service": "NetworkSlutter",
                            "auth": {"status": "unavailable", "ready": False},
                        },
                        recent_events,
                        health_only=True,
                    ),
                )

        self._server = ThreadingHTTPServer((self._host, self._port), Handler)
        setattr(self._server, "_logger", self._logger)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.5},
            daemon=True,
            name="web-status-server",
        )
        self._thread.start()
        self._logger.info("Web status server listening on http://%s:%s", self._host, self.port)

    @property
    def port(self) -> int:
        if self._server is not None:
            return int(self._server.server_address[1])
        return self._port

    def stop(self) -> None:
        if self._server is None:
            return
        try:
            self._server.shutdown()
            self._server.server_close()
        finally:
            self._server = None
            if self._thread is not None:
                self._thread.join(timeout=2.0)
                self._thread = None
