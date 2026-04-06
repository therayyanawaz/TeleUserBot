#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '\n==> %s\n' "$1"
}

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$repo_root"

minimum_python_version="3.11"

if [[ "${EUID}" -eq 0 ]]; then
  SUDO=""
else
  SUDO="sudo"
fi

log "Updating apt metadata"
$SUDO apt-get update

version_ge() {
  [[ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | tail -n 1)" == "$1" ]]
}

python_version_of() {
  local bin="$1"
  "$bin" - <<'PY' 2>/dev/null
import sys
print(f"{sys.version_info[0]}.{sys.version_info[1]}.{sys.version_info[2]}")
PY
}

pick_best_installed_python() {
  local best_bin=""
  local best_version=""
  local candidate version
  local -a candidates=()
  declare -A seen=()

  if command -v python3 >/dev/null 2>&1; then
    candidates+=("python3")
  fi

  while IFS= read -r candidate; do
    [[ -n "$candidate" ]] && candidates+=("$candidate")
  done < <(compgen -c | grep -E '^python3\.[0-9]+$' | sort -u)

  for candidate in "${candidates[@]}"; do
    [[ -n "${seen[$candidate]:-}" ]] && continue
    seen[$candidate]=1

    if ! command -v "$candidate" >/dev/null 2>&1; then
      continue
    fi

    version="$(python_version_of "$candidate")"
    [[ -z "$version" ]] && continue
    version_ge "$version" "$minimum_python_version" || continue

    if [[ -z "$best_version" ]] || version_ge "$version" "$best_version"; then
      best_bin="$candidate"
      best_version="$version"
    fi
  done

  if [[ -n "$best_bin" ]]; then
    printf '%s\n%s\n' "$best_bin" "$best_version"
  fi
}

find_latest_apt_python() {
  local best_minor=""
  local candidate minor

  while IFS= read -r candidate; do
    minor="${candidate#python3.}"
    [[ "$minor" =~ ^[0-9]+$ ]] || continue
    (( minor >= 11 )) || continue

    if ! apt-cache show "$candidate" >/dev/null 2>&1; then
      continue
    fi
    if ! apt-cache show "${candidate}-venv" >/dev/null 2>&1; then
      continue
    fi

    if [[ -z "$best_minor" || "$minor" -gt "$best_minor" ]]; then
      best_minor="$minor"
    fi
  done < <(apt-cache search '^python3\.[0-9]+$' 2>/dev/null | sed -nE 's/^(python3\.[0-9]+)\s.*/\1/p')

  if [[ -n "$best_minor" ]]; then
    printf 'python3.%s\n' "$best_minor"
  fi
}

python_bin=""
python_version=""
mapfile -t installed_python_info < <(pick_best_installed_python)
if [[ "${#installed_python_info[@]}" -ge 2 ]]; then
  python_bin="${installed_python_info[0]}"
  python_version="${installed_python_info[1]}"
fi

if [[ -z "$python_bin" ]]; then
  latest_python_pkg="$(find_latest_apt_python)"
  if [[ -n "$latest_python_pkg" ]]; then
    log "Installing ${latest_python_pkg}"
    $SUDO apt-get install -y "$latest_python_pkg" "${latest_python_pkg}-venv" python3-pip
    python_bin="$latest_python_pkg"
    python_version="$(python_version_of "$python_bin")"
  fi
fi

if [[ -z "$python_bin" ]]; then
  log "Installing system Python packages"
  $SUDO apt-get install -y python3 python3-venv python3-pip
  python_bin="python3"
  python_version="$(python_version_of "$python_bin")"
fi

log "Checking Python version"
$python_bin - <<'PY'
import sys
if sys.version_info < (3, 11):
    raise SystemExit(
        f"Python 3.11+ is required, but found {sys.version.split()[0]}. "
        "Install Python 3.11+ and rerun this script."
    )
PY

log "Using Python ${python_version}"

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
