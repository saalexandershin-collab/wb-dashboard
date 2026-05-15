"""
Загрузка финансовых отчётов WB за указанный период.

Использование:
  WB_API_TOKEN='...' DATABASE_URL='sqlite:///wb_local.db' \
  SYNC_YEAR=2026 SYNC_MONTH=4 python3 scripts/sync_finances.py

  # Несколько месяцев через запятую:
  SYNC_MONTHS=4,5 python3 scripts/sync_finances.py
"""

import os
import sys
import time
import calendar
from datetime import datetime, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.wb_client import WBClient, WBApiError, parse_financial_report
from src.db.models import init_db, get_session_factory
from src.db.repository import FinancialReportRepository

TOKEN    = os.environ.get("WB_API_TOKEN", "")
DB_URL   = os.environ.get("DATABASE_URL", "sqlite:///wb_local.db")
YEAR     = int(os.environ.get("SYNC_YEAR", datetime.now().year))
MONTHS_RAW = os.environ.get("SYNC_MONTHS", os.environ.get("SYNC_MONTH", str(datetime.now().month)))
MONTHS = [int(m.strip()) for m in MONTHS_RAW.split(",")]

INTER_MONTH_DELAY = int(os.environ.get("INTER_MONTH_DELAY", "0"))  # секунд между месяцами

if not TOKEN:
    print("❌ Нужен WB_API_TOKEN")
    sys.exit(1)

engine  = init_db(DB_URL)
Session = get_session_factory(engine)
client  = WBClient(TOKEN)
repo    = FinancialReportRepository()

for idx, month in enumerate(MONTHS):
    # Пауза между месяцами — защита от блокировки WB API
    if idx > 0 and INTER_MONTH_DELAY > 0:
        mins = INTER_MONTH_DELAY // 60
        print(f"\n⏳ Пауза {mins} мин. перед следующим месяцем (защита от блокировки WB)...")
        time.sleep(INTER_MONTH_DELAY)

    last_day = calendar.monthrange(YEAR, month)[1]
    date_from = date(YEAR, month, 1)
    date_to   = date(YEAR, month, last_day)
    print(f"\n📅 Загружаю финансовый отчёт за {date_from} — {date_to}...")

    try:
        raw = client.get_financial_report(
            datetime(YEAR, month, 1),
            datetime(YEAR, month, last_day),
            on_progress=print,
        )
    except WBApiError as e:
        print(f"❌ Ошибка API: {e}")
        continue

    if not raw:
        print("⚠️  Нет данных за этот период")
        continue

    records = parse_financial_report(raw)
    print(f"   Получено строк: {len(records)}")

    with Session() as session:
        saved = repo.upsert_many(session, records)
    print(f"✅ Сохранено: {saved} строк")

print("\nГотово.")
