"""
Синхронизация остатков Ozon.

Использование:
    OZON_CLIENT_ID='...' OZON_API_KEY='...' DATABASE_URL='...' \
    python3 scripts/sync_ozon_stocks.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.api.ozon_client import OzonClient, OzonApiError, parse_stocks
from src.db.models import init_db, get_session_factory
from src.db.repository import OzonStockRepository

CLIENT_ID = os.environ["OZON_CLIENT_ID"]
API_KEY   = os.environ["OZON_API_KEY"]
DB_URL    = os.environ["DATABASE_URL"]

print("[Ozon Stocks] Загружаю остатки...")

engine  = init_db(DB_URL)
Session = get_session_factory(engine)
client  = OzonClient(CLIENT_ID, API_KEY)
repo    = OzonStockRepository()

with Session() as session:
    try:
        def progress(msg):
            print(" ", msg)

        raw = client.get_stocks(on_progress=progress)
        rows = parse_stocks(raw)
        print(f"  Получено {len(rows)} позиций")

        saved = repo.replace_all(session, rows)
        print(f"  Сохранено {saved} строк в БД")
        print("[Ozon Stocks] Готово ✓")

    except OzonApiError as e:
        print(f"[Ozon Stocks] Ошибка API: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[Ozon Stocks] Неожиданная ошибка: {e}")
        raise
