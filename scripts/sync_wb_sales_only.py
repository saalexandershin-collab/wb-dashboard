"""
Синхронизация ТОЛЬКО продаж WB — без заказов.
Один запрос к API, корректные задержки, без блокировок.

Переменные окружения:
  WB_API_TOKEN  — токен WB Statistics API
  DATABASE_URL  — строка подключения к БД
  SYNC_YEAR     — год (необязательно, по умолчанию текущий)
  SYNC_MONTH    — месяц 1-12 (необязательно, по умолчанию текущий)
  DATE_FROM     — если задан, переопределяет расчёт начала периода (YYYY-MM-DD)
"""
import os
import sys
import time
import calendar
from datetime import datetime, timedelta, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.wb_client import WBClient, WBApiError, parse_sales
from src.db.models import init_db, get_session_factory
from src.db.repository import SaleRepository, SyncLogRepository

WB_TOKEN = os.environ.get("WB_API_TOKEN", "").strip()
DB_URL    = os.environ.get("DATABASE_URL", "").strip()

if not WB_TOKEN:
    print("❌ WB_API_TOKEN не задан")
    sys.exit(1)
if not DB_URL:
    print("❌ DATABASE_URL не задан")
    sys.exit(1)

# ── Период синхронизации ────────────────────────────────────────────────────
today = date.today()
year  = int(os.environ.get("SYNC_YEAR",  "").strip() or today.year)
month = int(os.environ.get("SYNC_MONTH", "").strip() or today.month)

# DATE_FROM переопределяет расчётное начало периода
date_from_env = os.environ.get("DATE_FROM", "").strip()
if date_from_env:
    date_from = datetime.strptime(date_from_env, "%Y-%m-%d") - timedelta(seconds=1)
else:
    # flag=0: WB возвращает записи с lastChangeDate > dateFrom
    # Берём секунду до начала месяца, чтобы захватить все обновления за месяц
    date_from = datetime(year, month, 1) - timedelta(seconds=1)

last_day_num = calendar.monthrange(year, month)[1]
first_day    = date(year, month, 1)
last_day     = date(year, month, last_day_num)

print(f"📅 Период: {first_day} — {last_day}")
print(f"🔍 dateFrom для API: {date_from.strftime('%Y-%m-%dT%H:%M:%S')}")
print(f"⏳ Выполняется: только продажи (заказы пропускаются)")

# ── Проверяем таймер глобального лимита WB ────────────────────────────────
_GLOBAL_LOCK = "/tmp/wb_last_any_request.txt"
MIN_INTERVAL = 90  # секунд — официальный лимит WB: 1 req/min, берём с запасом

try:
    with open(_GLOBAL_LOCK) as f:
        last_ts = float(f.read().strip())
    elapsed = time.time() - last_ts
    if elapsed < MIN_INTERVAL:
        wait = int(MIN_INTERVAL - elapsed + 3)
        print(f"⏱  Предварительная пауза {wait} сек (глобальный лимит WB API)...")
        time.sleep(wait)
except Exception:
    pass  # файл не существует — запросов ещё не было

# ── Запрос к API ──────────────────────────────────────────────────────────
client = WBClient(WB_TOKEN)

def progress(msg: str):
    print(f"  {msg}")

print("📥 Запрашиваю продажи у WB API...")
try:
    raw_sales = client.get_sales(date_from, flag=0, on_progress=progress)
except WBApiError as e:
    print(f"❌ Ошибка WB API: {e}")
    sys.exit(1)

print(f"   Получено записей от API: {len(raw_sales)}")

# ── Фильтрация по месяцу ──────────────────────────────────────────────────
def filter_by_month(records: list, y: int, m: int) -> list:
    result = []
    for r in records:
        raw = r.get("date", "")
        if not raw:
            continue
        try:
            dt = datetime.strptime(str(raw)[:10], "%Y-%m-%d")
            if dt.year == y and dt.month == m:
                result.append(r)
        except ValueError:
            continue
    return result

raw_filtered = filter_by_month(raw_sales, year, month)
print(f"   После фильтра по {month:02d}.{year}: {len(raw_filtered)} записей")

sales = parse_sales(raw_filtered)

# ── Сохранение в БД ───────────────────────────────────────────────────────
engine  = init_db(DB_URL)
Session = get_session_factory(engine)
sale_repo = SaleRepository()
log_repo  = SyncLogRepository()

with Session() as session:
    log = log_repo.create(
        session, platform="wb", sync_type="sales_only",
        date_from=first_day, date_to=last_day, status="running",
    )

n_sales = 0
error_msg = None
try:
    with Session() as session:
        n_sales = sale_repo.upsert_many(session, sales)
    print(f"✅ Сохранено продаж/возвратов: {n_sales}")
except Exception as e:
    error_msg = str(e)
    print(f"❌ Ошибка сохранения в БД: {e}")
finally:
    status = "success" if error_msg is None else "error"
    with Session() as session:
        log_repo.finish(
            session, log.id, status,
            orders_loaded=0,
            sales_loaded=n_sales,
            error_message=error_msg,
        )

if error_msg:
    sys.exit(1)

print(f"🎉 Готово! Продаж за {month:02d}.{year}: {n_sales}")
