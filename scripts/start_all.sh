#!/bin/bash

# Start both API and UI servers for AI-Trader
set -euo pipefail

# Get the project root directory (parent of scripts/)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
API_PORT="${API_PORT:-8888}"
UI_PORT="${UI_PORT:-8890}"
API_HOST="${API_HOST:-0.0.0.0}"
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

# Sync frontend config
if [ -f "$SYNC_SCRIPT" ]; then
    echo "📝 Syncing frontend configuration..."
    python3 "$SYNC_SCRIPT"
else
    echo "⚠️  sync_frontend_config.py not found, skipping config sync."
fi

if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$API_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "❌ Error: API port $API_PORT is already in use."
    exit 1
fi

if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$UI_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "❌ Error: UI port $UI_PORT is already in use."
    exit 1
fi

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Shutting down servers..."
    kill $API_PID 2>/dev/null
    kill $UI_PID 2>/dev/null
    exit 0
}

# Trap SIGINT (Ctrl+C) and SIGTERM
trap cleanup SIGINT SIGTERM

echo ""
echo "🚀 Starting FastAPI server on port ${API_PORT}..."
uvicorn api.main:app --host "$API_HOST" --port "$API_PORT" &
API_PID=$!

# Wait for API to be ready
sleep 2

echo "🌐 Starting Web UI server on port ${UI_PORT}..."
cd docs && python3 -m http.server "$UI_PORT" --bind "$UI_HOST" &
UI_PID=$!

cd "$PROJECT_ROOT"

echo ""
echo "✅ Both servers are running:"
echo "   - API:  http://localhost:${API_PORT} (docs: http://localhost:${API_PORT}/docs)"
echo "   - UI:   http://localhost:${UI_PORT}"
echo ""
echo "Press Ctrl+C to stop both servers"
echo ""

# Wait for both processes
wait $API_PID $UI_PID
