#!/bin/bash

# Start AI-Trader Web UI

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

echo "🌐 Starting Web UI server..."
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

python3 scripts/sync_frontend_config.py

cd docs
python3 -m http.server 8888
