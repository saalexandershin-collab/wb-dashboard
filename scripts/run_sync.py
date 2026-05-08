"""
Запускается GitHub Actions ежедневно.
Переменные окружения: WB_API_TOKEN, DATABASE_URL, SYNC_YEAR, SYNC_MONTH.
"""
import os
import sys
from datetime import date

# Добавляем корень проекта в путь
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.sync.sync_manager import SyncManager
from src.api.wb_client import WBApiError

WB_TOKEN = os.environ.get("WB_API_TOKEN", "")
DB_URL = os.environ.get("DATABASE_URL", "")

if not WB_TOKEN:
    print("❌ WB_API_TOKEN не задан")
    sys.exit(1)
if not DB_URL:
    print("❌ DATABASE_URL не задан")
    sys.exit(1)

today = date.today()
year_env = os.environ.get("SYNC_YEAR", "").strip()
month_env = os.environ.get("SYNC_MONTH", "").strip()

year = int(year_env) if year_env else today.year
month = int(month_env) if month_env else today.month

print(f"Синхронизация: {month:02d}.{year}")

manager = SyncManager(DB_URL, WB_TOKEN)

try:
    result = manager.sync_month(
        year, month,
        on_progress=lambda msg: print(f"  {msg}"),
    )
    print(f"✅ Заказов: {result['orders']}, продаж/возвратов: {result['sales']}")
except WBApiError as e:
    print(f"❌ Ошибка WB API: {e}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Ошибка: {e}")
    sys.exit(1)
