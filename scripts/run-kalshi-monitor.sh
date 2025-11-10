#!/bin/bash
# Kalshi Monitor - Copy Trading Bot

cd "$(dirname "$0")/.."

# Check virtual environment
if [ ! -d "venv" ]; then
    echo "Error: venv not found. Run: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Check .env
if [ ! -f .env ]; then
    echo "Error: .env not found. Run: cp .env.example .env"
    exit 1
fi

# Check trader list config
if [ ! -f config/kalshi_trader_list.json ]; then
    echo "Error: config/kalshi_trader_list.json not found"
    echo "Create it with: cp config/kalshi_trader_list.json.example config/kalshi_trader_list.json"
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Run monitor
echo "Starting Kalshi Monitor..."
echo "Press Ctrl+C to stop"
echo ""

python -m src.kalshi.bots.kalshi_monitor \
    --config config/kalshi_trader_list.json \
    --verbose \
    "$@"
