"""
Загрузка остатков FBO с WB и сохранение в базу.

Использование:
  WB_API_TOKEN='...' DATABASE_URL='sqlite:///wb_local.db' python3 scripts/sync_stocks.py
"""

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.wb_client import WBClient, WBApiError, parse_stocks
from src.db.models import init_db, get_session_factory
from src.db.repository import StockRepository

TOKEN  = os.environ.get("WB_API_TOKEN", "")
DB_URL = os.environ.get("DATABASE_URL", "sqlite:///wb_local.db")

if not TOKEN:
    print("❌ Нужен WB_API_TOKEN")
    sys.exit(1)

engine  = init_db(DB_URL)
Session = get_session_factory(engine)
client  = WBClient(TOKEN)
repo    = StockRepository()

date_from = datetime.now() - timedelta(days=30)
print(f"📦 Загружаю остатки с WB (dateFrom={date_from.strftime('%Y-%m-%d')})...")

try:
    raw = client.get_stocks(date_from, on_progress=print)
except WBApiError as e:
    print(f"❌ Ошибка API: {e}")
    sys.exit(1)

if not raw:
    print("⚠️  Нет данных об остатках")
    sys.exit(0)

records = parse_stocks(raw)
# Добавляем platform и synced_at
now = datetime.utcnow()
for r in records:
    r["platform"] = "wb"
    r["synced_at"] = now

print(f"   Получено позиций: {len(records)}")

with Session() as session:
    saved = repo.replace_all(session, records)

print(f"✅ Сохранено: {saved} позиций")
print("Готово.")
