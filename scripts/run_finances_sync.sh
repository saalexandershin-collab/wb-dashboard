#!/bin/bash
# Синк финансовых отчётов WB → SQLite → Supabase
# Крон: 30 4 * * *  (через 1.5ч после заказов, через 1ч после складов)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

WB_API_TOKEN=$(grep 'api_token' "$PROJECT_DIR/.streamlit/secrets.toml" | sed 's/.*= *"//' | sed 's/".*//')

LOG_FILE="$PROJECT_DIR/logs/finances_sync.log"
mkdir -p "$PROJECT_DIR/logs"

echo "$(date '+%Y-%m-%d %H:%M:%S') — Запуск синка финансовых отчётов" >> "$LOG_FILE"

cd "$PROJECT_DIR"

# Синкаем предыдущий месяц + текущий.
# Python считает корректные год/месяц, чтобы не было ошибки на январе.
PROJECT_DIR="$PROJECT_DIR" WB_API_TOKEN="$WB_API_TOKEN" python3 -u - >> "$LOG_FILE" 2>&1 << 'PYEOF'
import subprocess, sys, os
from datetime import date
from dateutil.relativedelta import relativedelta

today = date.today()
prev  = today - relativedelta(months=1)

project_dir = os.environ["PROJECT_DIR"]
token       = os.environ["WB_API_TOKEN"]

for d in [prev, today]:
    print(f"  → Финансы {d.year}-{d.month:02d}...")
    env = {
        **os.environ,
        "DATABASE_URL": f"sqlite:////{project_dir}/wb_local.db",
        "SYNC_YEAR":   str(d.year),
        "SYNC_MONTHS": str(d.month),
    }
    subprocess.run([sys.executable, "-u", f"{project_dir}/scripts/sync_finances.py"], env=env)
PYEOF

echo "$(date '+%Y-%m-%d %H:%M:%S') — Синк финансов завершён, мигрирую в Supabase..." >> "$LOG_FILE"

python3 -u scripts/migrate_to_pg.py >> "$LOG_FILE" 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') — Готово" >> "$LOG_FILE"
