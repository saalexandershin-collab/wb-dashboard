"""
Синхронизация ТОЛЬКО заказов WB.
Переменные окружения: WB_API_TOKEN, DATABASE_URL, SYNC_YEAR, SYNC_MONTH
"""
import os, sys, calendar
from datetime import datetime, timedelta, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.wb_client import WBClient, WBApiError, parse_orders
from src.db.models import init_db, get_session_factory
from src.db.repository import OrderRepository, SyncLogRepository

WB_TOKEN = os.environ.get("WB_API_TOKEN", "").strip()
DB_URL    = os.environ.get("DATABASE_URL", "").strip()

if not WB_TOKEN:
    print("❌ WB_API_TOKEN не задан"); sys.exit(1)
if not DB_URL:
    print("❌ DATABASE_URL не задан"); sys.exit(1)

today = date.today()
year  = int(os.environ.get("SYNC_YEAR",  "").strip() or today.year)
month = int(os.environ.get("SYNC_MONTH", "").strip() or today.month)

date_from    = datetime(year, month, 1) - timedelta(seconds=1)
last_day_num = calendar.monthrange(year, month)[1]
first_day    = date(year, month, 1)
last_day     = date(year, month, last_day_num)

print(f"📅 Период: {first_day} — {last_day}")
print(f"🔍 dateFrom: {date_from.strftime('%Y-%m-%dT%H:%M:%S')}")
print(f"⏳ Синхронизация: только заказы")

client    = WBClient(WB_TOKEN)
progress  = lambda msg: print(f"  {msg}")

print("📥 Запрашиваю заказы у WB API...")
try:
    raw_orders = client.get_orders(date_from, flag=0, on_progress=progress)
except WBApiError as e:
    print(f"❌ Ошибка WB API: {e}"); sys.exit(1)

print(f"   Получено от API: {len(raw_orders)}")

def filter_by_month(records, y, m):
    result = []
    for r in records:
        raw = r.get("date", "")
        if not raw: continue
        try:
            dt = datetime.strptime(str(raw)[:10], "%Y-%m-%d")
            if dt.year == y and dt.month == m:
                result.append(r)
        except ValueError:
            continue
    return result

raw_filtered = filter_by_month(raw_orders, year, month)
print(f"   После фильтра по {month:02d}.{year}: {len(raw_filtered)} записей")
orders = parse_orders(raw_filtered)

engine    = init_db(DB_URL)
Session   = get_session_factory(engine)
order_repo = OrderRepository()
log_repo   = SyncLogRepository()

with Session() as session:
    log = log_repo.create(session, platform="wb", sync_type="orders_only",
                          date_from=first_day, date_to=last_day, status="running")

n_orders  = 0
error_msg = None
try:
    with Session() as session:
        n_orders = order_repo.upsert_many(session, orders)
    print(f"✅ Сохранено заказов: {n_orders}")
except Exception as e:
    error_msg = str(e)
    print(f"❌ Ошибка БД: {e}")
finally:
    status = "success" if not error_msg else "error"
    with Session() as session:
        log_repo.finish(session, log.id, status, orders_loaded=n_orders,
                        sales_loaded=0, error_message=error_msg)

if error_msg:
    sys.exit(1)
print(f"🎉 Готово! Заказов за {month:02d}.{year}: {n_orders}")
