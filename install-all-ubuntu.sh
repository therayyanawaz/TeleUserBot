#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '\n==> %s\n' "$1"
}

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_root"

if [[ "${EUID}" -eq 0 ]]; then
  SUDO=""
else
  SUDO="sudo"
fi

log "Updating apt metadata"
$SUDO apt-get update

python_bin=""
if command -v python3.11 >/dev/null 2>&1; then
  python_bin="python3.11"
elif apt-cache show python3.11 >/dev/null 2>&1 && apt-cache show python3.11-venv >/dev/null 2>&1; then
  log "Installing Python 3.11"
  $SUDO apt-get install -y python3.11 python3.11-venv python3-pip
  python_bin="python3.11"
else
  log "Installing system Python packages"
  $SUDO apt-get install -y python3 python3-venv python3-pip
  python_bin="python3"
fi

log "Installing FFmpeg and Tesseract"
$SUDO apt-get install -y \
  ffmpeg \
  tesseract-ocr \
  tesseract-ocr-ara \
  tesseract-ocr-fas \
  tesseract-ocr-rus \
  tesseract-ocr-urd

log "Checking Python version"
$python_bin - <<'PY'
import sys
if sys.version_info < (3, 11):
    raise SystemExit(
        f"Python 3.11+ is required, but found {sys.version.split()[0]}. "
        "Install Python 3.11+ and rerun this script."
    )
PY

log "Creating virtual environment"
if [[ ! -x ".venv/bin/python" ]]; then
  "$python_bin" -m venv .venv
fi

venv_python="$repo_root/.venv/bin/python"

log "Upgrading pip tooling"
"$venv_python" -m pip install --upgrade pip setuptools wheel

log "Installing Python dependencies"
"$venv_python" -m pip install -r requirements.txt -r requirements.optional.txt

log "Warming sentence-transformers model cache"
PYTHONIOENCODING="utf-8" "$venv_python" -c \
  "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2'); print('model-ready')"

log "Install completed"
printf '%s\n' "Run the bot with:"
printf '  %s\n' "PYTHONIOENCODING=utf-8 ./.venv/bin/python main.py"
printf '\n%s\n' "The bot still needs runtime config in .env, including TELEGRAM_API_ID, TELEGRAM_API_HASH, and DESTINATION."
