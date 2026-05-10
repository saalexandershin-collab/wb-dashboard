#!/bin/bash
# Автоматический синк остатков WB → Supabase
# Запускается cron-ом каждые 3 часа: 0, 3, 6, 9, 12, 15, 18, 21

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Читаем токен из secrets.toml
WB_API_TOKEN=$(grep 'api_token' "$PROJECT_DIR/.streamlit/secrets.toml" | sed 's/.*= *"//' | sed 's/".*//')

# Supabase URL (прямая запись без промежуточного SQLite)
DATABASE_URL="postgresql://postgres.uuhgslolrfytzjrmxwte:umQAcAnEXxUgk5XF@aws-0-eu-west-1.pooler.supabase.com:5432/postgres"

LOG_FILE="$PROJECT_DIR/logs/stocks_sync.log"
mkdir -p "$PROJECT_DIR/logs"

echo "$(date '+%Y-%m-%d %H:%M:%S') — Запуск синка остатков" >> "$LOG_FILE"

cd "$PROJECT_DIR"
WB_API_TOKEN="$WB_API_TOKEN" DATABASE_URL="$DATABASE_URL" \
    python3 -u scripts/sync_stocks.py >> "$LOG_FILE" 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') — Готово" >> "$LOG_FILE"
