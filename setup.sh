#!/usr/bin/env bash
#
# setup.sh - Bootstrap a fresh CLIProxyAPI install from the committed personal config.
#
# Copies the non-secret personal configuration template (config.personal.yaml) into
# config.yaml, creates the runtime directories the server expects, and reminds you
# which secrets must be set manually or transferred from your primary device.
#
# Idempotent: existing config.yaml and .env are never overwritten unless --force is used.

set -euo pipefail

FORCE=0
if [[ "${1:-}" == "--force" ]]; then
  FORCE=1
elif [[ "${1:-}" != "" ]]; then
  echo "Error: unknown option '${1}'."
  echo "Usage: ./setup.sh [--force]"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TEMPLATE="config.personal.yaml"
CONFIG="config.yaml"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "Error: $TEMPLATE not found. Run this script from the repository root."
  exit 1
fi

# --- Step 1: config.yaml ---
if [[ -f "$CONFIG" && $FORCE -eq 0 ]]; then
  echo "config.yaml already exists - keeping it (use --force to re-copy from the template)."
elif [[ -f "$CONFIG" ]]; then
  cp "$TEMPLATE" "$CONFIG"
  echo "config.yaml re-created from $TEMPLATE."
else
  cp "$TEMPLATE" "$CONFIG"
  echo "Created config.yaml from $TEMPLATE."
fi

# --- Step 2: runtime directories ---
mkdir -p auths logs
echo "Ensured runtime directories (auths/, logs/)."

# --- Step 3: .env ---
if [[ ! -f ".env" ]]; then
  cp .env.example .env
  echo "Created .env from .env.example."
else
  echo ".env already exists - keeping it."
fi

# --- Step 4: secrets checklist ---
echo
echo "Next steps:"
echo "  1. Fill in your secrets in config.yaml (and .env):"
echo "     - api-keys: your device API key (e.g. the key your CLI clients use)"
echo "     - remote-management.secret-key: your management key"
echo "     - openai-compatibility[].api-key-entries[].api-key: your OpenRouter key"
echo "     - .env: MANAGEMENT_PASSWORD (if you use the management web UI)"
echo "  2. If your primary device stores OAuth/login files under auths/, copy them here,"
echo "     e.g.: scp -r user@primary:/path/to/cliproxy/auths/* auths/"
echo "  3. Start the server:"
echo "     go run ./cmd/server          # dev, or:"
echo "     go build -o cli-proxy-api ./cmd/server && ./cli-proxy-api"
echo

REMAINING="$(grep -c 'SET_ME' "$CONFIG" 2>/dev/null || true)"
if [[ "$REMAINING" -gt 0 ]]; then
  echo "$REMAINING SET_ME placeholder(s) still need a real value in config.yaml."
else
  echo "No SET_ME placeholders left in config.yaml."
fi
