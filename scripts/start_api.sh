#!/bin/bash

# Start AI-Trader FastAPI Backend
set -euo pipefail

# Get the project root directory (parent of scripts/)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
API_PORT="${API_PORT:-8888}"
API_HOST="${API_HOST:-0.0.0.0}"

cd "$PROJECT_ROOT"

# Check and activate virtual environment
if [ -d ".venv" ]; then
    echo "🔌 Activating virtual environment..."
    source .venv/bin/activate
else
    echo "❌ Error: Virtual environment .venv not found in project root."
    exit 1
fi

if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$API_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "❌ Error: Port $API_PORT is already in use."
    echo "   Change port via API_PORT, e.g.: API_PORT=8999 bash scripts/start_api.sh"
    exit 1
fi

echo "🚀 Starting FastAPI server..."
echo ""
echo "API Documentation: http://localhost:${API_PORT}/docs"
echo "Health Check: http://localhost:${API_PORT}/health"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Run the FastAPI server
uvicorn api.main:app --host "$API_HOST" --port "$API_PORT" --reload
