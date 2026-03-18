#!/usr/bin/env bash
set -e

# ── colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[setup]${NC} $*"; }
warn()    { echo -e "${YELLOW}[warn]${NC}  $*"; }
error()   { echo -e "${RED}[error]${NC} $*"; exit 1; }

echo ""
echo "  Google Drive Deduplicator — Setup"
echo "  ─────────────────────────────────"
echo ""

# ── 1. Python version check ───────────────────────────────────────────────────
PYTHON=$(command -v python3.12 || command -v python3.11 || command -v python3.10 || command -v python3 || true)
[[ -z "$PYTHON" ]] && error "python3 not found. Install Python 3.10+ and re-run."

PY_VERSION=$("$PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)

if [[ "$PY_MAJOR" -lt 3 ]] || { [[ "$PY_MAJOR" -eq 3 ]] && [[ "$PY_MINOR" -lt 10 ]]; }; then
  error "Python 3.10+ required (found $PY_VERSION)."
fi
info "Python $PY_VERSION — OK"

# ── 2. Virtual environment ────────────────────────────────────────────────────
VENV_DIR="$(dirname "$0")/.venv"

if [[ ! -d "$VENV_DIR" ]]; then
  info "Creating virtual environment at .venv ..."
  "$PYTHON" -m venv "$VENV_DIR"
else
  info "Virtual environment already exists — skipping creation."
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
info "Virtual environment activated."

# ── 3. Install dependencies ───────────────────────────────────────────────────
info "Installing dependencies from requirements.txt ..."
pip install --quiet --upgrade pip
pip install --quiet -r "$(dirname "$0")/requirements.txt"
info "Dependencies installed."

# ── 4. credentials.json check ────────────────────────────────────────────────
CREDS="$(dirname "$0")/credentials.json"

if [[ ! -f "$CREDS" ]]; then
  warn "credentials.json not found."
  echo ""
  echo "  You need to create Google Drive API credentials:"
  echo "  1. Go to https://console.cloud.google.com/"
  echo "  2. Create a project (or select an existing one)"
  echo "  3. Enable the Google Drive API:"
  echo "       APIs & Services → Enable APIs → search 'Google Drive API' → Enable"
  echo "  4. Create OAuth credentials:"
  echo "       APIs & Services → Credentials → Create Credentials → OAuth 2.0 Client ID"
  echo "       Application type: Desktop app"
  echo "  5. Download the JSON file and save it as:"
  echo "       $(realpath "$(dirname "$0")")/credentials.json"
  echo ""
else
  info "credentials.json found — OK"
fi

# ── 5. Done ───────────────────────────────────────────────────────────────────
echo ""
echo "  Setup complete. To run the deduplicator:"
echo ""
echo "    source .venv/bin/activate"
echo "    python3 dedupe_gdrive.py <GOOGLE_DRIVE_FOLDER_ID> [dest_folder_name]"
echo ""
echo "  Your folder ID is the last part of the Drive URL:"
echo "    https://drive.google.com/drive/folders/<FOLDER_ID>"
echo ""
