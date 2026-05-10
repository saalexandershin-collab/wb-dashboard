#!/bin/bash
# Автоматический синк заказов и продаж WB → Supabase
# Запускается cron-ом каждый день в 3:00

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Читаем токен из secrets.toml
WB_API_TOKEN=$(grep 'api_token' "$PROJECT_DIR/.streamlit/secrets.toml" | sed 's/.*= *"//' | sed 's/".*//')

# Supabase URL
DATABASE_URL="postgresql://postgres.uuhgslolrfytzjrmxwte:umQAcAnEXxUgk5XF@aws-0-eu-west-1.pooler.supabase.com:5432/postgres"

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

echo "$(date '+%Y-%m-%d %H:%M:%S') — Синк завершён, запускаю миграцию в Supabase..." >> "$LOG_FILE"

python3 -u scripts/migrate_to_pg.py >> "$LOG_FILE" 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') — Готово" >> "$LOG_FILE"
