#!/bin/bash

# Start both API and UI servers for AI-Trader

# Get the project root directory (parent of scripts/)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

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
echo "📝 Syncing frontend configuration..."
python3 scripts/sync_frontend_config.py

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
echo "🚀 Starting FastAPI server on port 8000..."
uvicorn api.main:app --host 0.0.0.0 --port 8000 &
API_PID=$!

# Wait for API to be ready
sleep 2

echo "🌐 Starting Web UI server on port 8888..."
cd docs && python3 -m http.server 8888 &
UI_PID=$!

cd "$PROJECT_ROOT"

echo ""
echo "✅ Both servers are running:"
echo "   - API:  http://localhost:8000 (docs: http://localhost:8000/docs)"
echo "   - UI:   http://localhost:8888"
echo ""
echo "Press Ctrl+C to stop both servers"
echo ""

# Wait for both processes
wait $API_PID $UI_PID
