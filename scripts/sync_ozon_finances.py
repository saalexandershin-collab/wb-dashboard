"""
Синхронизация финансовых транзакций Ozon за указанный месяц.

Использование:
    OZON_CLIENT_ID='...' OZON_API_KEY='...' DATABASE_URL='...' \
    SYNC_YEAR=2026 SYNC_MONTH=5 \
    python3 scripts/sync_ozon_finances.py
"""

import os
import sys
import calendar
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.api.ozon_client import OzonClient, OzonApiError, parse_transactions
from src.db.models import init_db, get_session_factory
from src.db.repository import OzonTransactionRepository, SyncLogRepository

CLIENT_ID = os.environ["OZON_CLIENT_ID"]
API_KEY   = os.environ["OZON_API_KEY"]
DB_URL    = os.environ["DATABASE_URL"]

today = datetime.today()
YEAR  = int(os.environ.get("SYNC_YEAR",  today.year))
MONTH = int(os.environ.get("SYNC_MONTH", today.month))

date_from = datetime(YEAR, MONTH, 1)
date_to   = datetime(YEAR, MONTH, calendar.monthrange(YEAR, MONTH)[1])

print(f"[Ozon Finances] Синхронизация {YEAR}-{MONTH:02d}: {date_from.date()} → {date_to.date()}")

engine  = init_db(DB_URL)
Session = get_session_factory(engine)
client  = OzonClient(CLIENT_ID, API_KEY)
repo    = OzonTransactionRepository()
log_repo = SyncLogRepository()

with Session() as session:
    log = log_repo.create(session, platform="ozon", sync_type="finances",
                          date_from=date_from.date(), date_to=date_to.date(),
                          status="running")
    try:
        def progress(msg):
            print(" ", msg)

        raw = client.get_transactions(date_from, date_to, on_progress=progress)
        rows = parse_transactions(raw)
        print(f"  Получено {len(rows)} транзакций")

        saved = repo.upsert_many(session, rows)
        print(f"  Сохранено {saved} строк в БД")

        log_repo.finish(session, log.id, "success", sales_loaded=saved)
        print("[Ozon Finances] Готово ✓")

    except OzonApiError as e:
        log_repo.finish(session, log.id, "error", error_message=str(e))
        print(f"[Ozon Finances] Ошибка API: {e}")
        sys.exit(1)
    except Exception as e:
        log_repo.finish(session, log.id, "error", error_message=str(e))
        print(f"[Ozon Finances] Неожиданная ошибка: {e}")
        raise
