#!/bin/bash
# Синк заказов и продаж WB → SQLite → Supabase
# Крон: 0 3 * * *

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

WB_API_TOKEN=$(grep 'api_token' "$PROJECT_DIR/.streamlit/secrets.toml" | sed 's/.*= *"//' | sed 's/".*//')

LOG_FILE="$PROJECT_DIR/logs/orders_sync.log"
mkdir -p "$PROJECT_DIR/logs"

YEAR=$(date '+%Y')
MONTH=$(date '+%-m')

echo "$(date '+%Y-%m-%d %H:%M:%S') — Запуск синка заказов и продаж за $YEAR-$MONTH" >> "$LOG_FILE"

cd "$PROJECT_DIR"
WB_API_TOKEN="$WB_API_TOKEN" \
DATABASE_URL="sqlite:////$PROJECT_DIR/wb_local.db" \
SYNC_YEAR="$YEAR" \
SYNC_MONTH="$MONTH" \
    python3 -u scripts/run_sync.py >> "$LOG_FILE" 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') — Синк завершён, мигрирую в Supabase..." >> "$LOG_FILE"

python3 -u scripts/migrate_to_pg.py >> "$LOG_FILE" 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') — Готово" >> "$LOG_FILE"
