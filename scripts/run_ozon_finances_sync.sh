#!/bin/bash
# Синк финансовых транзакций Ozon → Supabase
# Крон: 0 6 * * *

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SECRETS="$PROJECT_DIR/.streamlit/secrets.toml"

OZON_CLIENT_ID=$(grep 'client_id' "$SECRETS" | tail -1 | sed 's/.*= *"//' | sed 's/".*//')
OZON_API_KEY=$(grep 'api_key'    "$SECRETS" | tail -1 | sed 's/.*= *"//' | sed 's/".*//')
DATABASE_URL=$(grep 'url'        "$SECRETS" | head -1 | sed 's/.*= *"//' | sed 's/".*//')

LOG_FILE="$PROJECT_DIR/logs/ozon_finances_sync.log"
mkdir -p "$PROJECT_DIR/logs"

cd "$PROJECT_DIR"

export OZON_CLIENT_ID DATABASE_URL OZON_API_KEY PROJECT_DIR

# Синкаем предыдущий месяц + текущий
python3 -u - >> "$LOG_FILE" 2>&1 << 'PYEOF'
import subprocess, sys, os
from datetime import date
from dateutil.relativedelta import relativedelta

today = date.today()
prev  = today - relativedelta(months=1)

env_base = {
    **os.environ,
    "OZON_CLIENT_ID": os.environ["OZON_CLIENT_ID"],
    "OZON_API_KEY":   os.environ["OZON_API_KEY"],
    "DATABASE_URL":   os.environ["DATABASE_URL"],
}

project_dir = os.environ["PROJECT_DIR"]

for d in [prev, today]:
    print(f"  → Финансы Ozon {d.year}-{d.month:02d}...")
    env = {**env_base, "SYNC_YEAR": str(d.year), "SYNC_MONTH": str(d.month)}
    subprocess.run([sys.executable, "-u", f"{project_dir}/scripts/sync_ozon_finances.py"], env=env)
PYEOF

echo "$(date '+%Y-%m-%d %H:%M:%S') — Ozon финансы готово" >> "$LOG_FILE"
