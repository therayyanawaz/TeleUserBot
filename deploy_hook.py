"""Minimal deploy webhook server. Run as a background service."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


WEBHOOK_SECRET = os.environ["DEPLOY_WEBHOOK_SECRET"]
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
RESTART_CMD = os.getenv("DEPLOY_RESTART_CMD", "systemctl restart teleuserbot").split()
PORT = int(os.getenv("DEPLOY_WEBHOOK_PORT", "9876"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
LOGGER = logging.getLogger("deploy_hook")


def _run_deploy(sha: str) -> None:
    LOGGER.info("Deploy started. SHA=%s", sha)
    steps = [
        ["git", "pull", "origin", "main"],
        [sys.executable, "-m", "pip", "install", "-r", "requirements.txt", "-q"],
        RESTART_CMD,
    ]
    for command in steps:
        LOGGER.info("Running: %s", " ".join(command))
        subprocess.run(
            command,
            cwd=PROJECT_DIR,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    LOGGER.info("Deploy finished. SHA=%s", sha)


class DeployHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/deploy":
            self.send_response(404)
            self.end_headers()
            return

        auth = self.headers.get("Authorization", "")
        token = auth.removeprefix("Bearer ").strip()
        if token != WEBHOOK_SECRET:
            LOGGER.warning("Unauthorized deploy attempt from %s", self.client_address)
            self.send_response(401)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        sha = "unknown"
        try:
            payload = json.loads(body)
            sha = str(payload.get("sha") or "unknown")
        except Exception:
            pass

        LOGGER.info("Deploy trigger accepted. SHA=%s", sha)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

        worker = threading.Thread(target=_run_deploy, args=(sha,), daemon=True)
        worker.start()

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


if __name__ == "__main__":
    LOGGER.info("Deploy webhook listening on port %d", PORT)
    HTTPServer(("0.0.0.0", PORT), DeployHandler).serve_forever()
