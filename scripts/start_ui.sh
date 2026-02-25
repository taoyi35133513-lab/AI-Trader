#!/bin/bash

# Start AI-Trader Web UI
set -euo pipefail

# Get the project root directory (parent of scripts/)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
UI_PORT="${UI_PORT:-8890}"
UI_HOST="${UI_HOST:-0.0.0.0}"
SYNC_SCRIPT="$PROJECT_ROOT/scripts/sync_frontend_config.py"

cd "$PROJECT_ROOT"

# Check and activate virtual environment
if [ -d ".venv" ]; then
    echo "🔌 Activating virtual environment..."
    source .venv/bin/activate
else
    echo "❌ Error: Virtual environment .venv not found in project root."
    exit 1
fi

if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$UI_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "❌ Error: Port $UI_PORT is already in use."
    echo "   Change port via UI_PORT, e.g.: UI_PORT=8899 bash scripts/start_ui.sh"
    exit 1
fi

echo "🌐 Starting Web UI server..."
echo "UI URL: http://localhost:${UI_PORT}"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

if [ -f "$SYNC_SCRIPT" ]; then
    echo "📝 Syncing frontend configuration..."
    python3 "$SYNC_SCRIPT"
else
    echo "⚠️  sync_frontend_config.py not found, skipping config sync."
fi

cd docs
python3 -m http.server "$UI_PORT" --bind "$UI_HOST"
