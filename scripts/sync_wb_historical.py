"""
Исторический импорт заказов WB — помесячно с соблюдением rate limit.
Используется для загрузки данных за прошлые периоды.

Env:
  WB_API_TOKEN, DATABASE_URL
  YEAR_FROM, MONTH_FROM  — начало диапазона
  YEAR_TO,   MONTH_TO    — конец диапазона (включительно)
"""
import os, sys, calendar, time
from datetime import datetime, timedelta, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.models import init_db, get_session_factory
from src.api.wb_client import WBClient, WBApiError, parse_orders
from src.api.db_rate_limiter import DBRateLimiter
from src.db.repository import OrderRepository, SyncLogRepository

WB_TOKEN = os.environ.get("WB_API_TOKEN", "").strip()
DB_URL   = os.environ.get("DATABASE_URL", "").strip()
if not WB_TOKEN: print("❌ WB_API_TOKEN не задан"); sys.exit(1)
if not DB_URL:   print("❌ DATABASE_URL не задан"); sys.exit(1)

year_from  = int(os.environ.get("YEAR_FROM",  "2025"))
month_from = int(os.environ.get("MONTH_FROM", "7"))
year_to    = int(os.environ.get("YEAR_TO",    "2026"))
month_to   = int(os.environ.get("MONTH_TO",   "3"))

engine  = init_db(DB_URL)
Session = get_session_factory(engine)
rl      = DBRateLimiter(engine)

# Проверяем блокировку перед стартом
blocked = rl.check_blocked()
if blocked is not None:
    sys.exit(0)

client     = WBClient(WB_TOKEN, rate_limiter=rl)
order_repo = OrderRepository()
log_repo   = SyncLogRepository()

# Строим список месяцев
months = []
y, m = year_from, month_from
while (y, m) <= (year_to, month_to):
    months.append((y, m))
    m += 1
    if m > 12: m = 1; y += 1

print(f"📅 Загрузка {len(months)} месяцев: {months[0]} → {months[-1]}")
total_saved = 0
errors = []

for idx, (year, month) in enumerate(months):
    last_day  = calendar.monthrange(year, month)[1]
    first_day = date(year, month, 1)
    last_day_d = date(year, month, last_day)
    date_from = datetime(year, month, 1) - timedelta(seconds=1)

    print(f"\n[{idx+1}/{len(months)}] {month:02d}.{year} ────────────────")

    # Ждём rate limit перед каждым запросом (кроме первого — rl сам подождёт)
    if idx > 0:
        rl.wait_if_needed(key="default", on_progress=lambda m: print(f"  ⏱ {m}"))

    # Ещё раз проверяем блокировку
    if rl.check_blocked():
        print(f"  🚫 Токен заблокирован, останавливаемся")
        break

    with Session() as session:
        log = log_repo.create(session, platform="wb", sync_type="orders_historical",
                              date_from=first_day, date_to=last_day_d, status="running")

    n = 0; err = None
    try:
        raw = client.get_orders(date_from, flag=0, on_progress=lambda m: print(f"  {m}"))
        print(f"  Получено от API: {len(raw)}")

        # Фильтр по месяцу
        filtered = []
        for r in raw:
            raw_d = r.get("date", "")
            if not raw_d: continue
            try:
                dt = datetime.strptime(str(raw_d)[:10], "%Y-%m-%d")
                if dt.year == year and dt.month == month:
                    filtered.append(r)
            except ValueError:
                continue
        print(f"  После фильтра {month:02d}.{year}: {len(filtered)}")

        orders = parse_orders(filtered)
        with Session() as session:
            n = order_repo.upsert_many(session, orders)
        print(f"  ✅ Сохранено: {n}")
        total_saved += n

    except WBApiError as e:
        err = str(e); print(f"  ❌ WB API: {e}")
        errors.append(f"{month:02d}.{year}: {e}")
        if "429" in str(e) or "заблокирован" in str(e).lower():
            print("  🛑 Блокировка — останавливаемся")
            with Session() as session:
                log_repo.finish(session, log.id, "error", orders_loaded=0, sales_loaded=0, error_message=err)
            break
    except Exception as e:
        err = str(e); print(f"  ❌ Ошибка: {e}")
        errors.append(f"{month:02d}.{year}: {e}")
    finally:
        status = "success" if not err else "error"
        with Session() as session:
            log_repo.finish(session, log.id, status, orders_loaded=n, sales_loaded=0, error_message=err)

print(f"\n{'='*50}")
print(f"✅ Итого сохранено заказов: {total_saved}")
if errors:
    print(f"⚠️  Ошибки ({len(errors)}): {', '.join(errors)}")
