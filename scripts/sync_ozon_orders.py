"""
Синхронизация заказов/постингов Ozon (FBO + FBS) за указанный месяц.

Использование:
    OZON_CLIENT_ID='...' OZON_API_KEY='...' DATABASE_URL='...' \
    SYNC_YEAR=2026 SYNC_MONTH=5 \
    python3 scripts/sync_ozon_orders.py
"""

import os
import sys
import calendar
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.api.ozon_client import OzonClient, OzonApiError, parse_postings
from src.db.models import init_db, get_session_factory
from src.db.repository import OzonPostingRepository, SyncLogRepository

CLIENT_ID  = os.environ["OZON_CLIENT_ID"]
API_KEY    = os.environ["OZON_API_KEY"]
DB_URL     = os.environ["DATABASE_URL"]

today = datetime.today()
YEAR  = int(os.environ.get("SYNC_YEAR",  today.year))
MONTH = int(os.environ.get("SYNC_MONTH", today.month))

date_from = datetime(YEAR, MONTH, 1)
date_to   = datetime(YEAR, MONTH, calendar.monthrange(YEAR, MONTH)[1])

print(f"[Ozon Orders] Синхронизация {YEAR}-{MONTH:02d}: {date_from.date()} → {date_to.date()}")

engine  = init_db(DB_URL)
Session = get_session_factory(engine)
client  = OzonClient(CLIENT_ID, API_KEY)
repo    = OzonPostingRepository()
log_repo = SyncLogRepository()

with Session() as session:
    log = log_repo.create(session, platform="ozon", sync_type="orders",
                          date_from=date_from.date(), date_to=date_to.date(),
                          status="running")
    try:
        def progress(msg):
            print(" ", msg)

        print("  Загружаю FBO постинги...")
        raw_fbo = client.get_postings_fbo(date_from, date_to, on_progress=progress)
        fbo_rows = parse_postings(raw_fbo, order_type="FBO")
        print(f"  FBO: {len(fbo_rows)} строк")

        print("  Загружаю FBS постинги...")
        raw_fbs = client.get_postings_fbs(date_from, date_to, on_progress=progress)
        fbs_rows = parse_postings(raw_fbs, order_type="FBS")
        print(f"  FBS: {len(fbs_rows)} строк")

        all_rows = fbo_rows + fbs_rows
        saved = repo.upsert_many(session, all_rows)
        print(f"  Сохранено {saved} строк в БД")

        log_repo.finish(session, log.id, "success",
                        orders_loaded=saved, sales_loaded=0)
        print("[Ozon Orders] Готово ✓")

    except OzonApiError as e:
        log_repo.finish(session, log.id, "error", error_message=str(e))
        print(f"[Ozon Orders] Ошибка API: {e}")
        sys.exit(1)
    except Exception as e:
        log_repo.finish(session, log.id, "error", error_message=str(e))
        print(f"[Ozon Orders] Неожиданная ошибка: {e}")
        raise
