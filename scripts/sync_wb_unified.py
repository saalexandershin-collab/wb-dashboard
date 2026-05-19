"""
Единый скрипт синхронизации WB — ровно ОДИН запрос к API.

Режимы (переменная SYNC_MODE):
  orders   — заказы и отмены        (ежедневно 03:00 МСК)
  sales    — выкупы (продажи)       (ежедневно 04:00 МСК)
  finances — финансовый отчёт       (ежедневно 06:00 МСК)

Переменные окружения:
  WB_API_TOKEN  — токен WB Statistics API
  DATABASE_URL  — строка подключения к БД
  SYNC_YEAR     — год (по умолчанию текущий)
  SYNC_MONTH    — месяц 1-12 (по умолчанию текущий)
  SYNC_MODE     — 'orders' | 'sales' | 'finances'

Защита от параллельных запусков:
  Перед запросом проверяет wb_blocked_until в БД.
  Если токен заблокирован — завершается с кодом 0 (не ошибка, просто ждём).
  GitHub Actions concurrency group предотвращает одновременный запуск двух job'ов.
"""
import os
import sys
import calendar
from datetime import datetime, timedelta, date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.models import init_db, get_session_factory
from src.api.wb_client import WBClient, WBApiError, parse_orders, parse_sales, parse_financial_report
from src.api.db_rate_limiter import DBRateLimiter
from src.db.repository import OrderRepository, SaleRepository, SyncLogRepository

# ── Конфигурация ──────────────────────────────────────────────────────────────
WB_TOKEN  = os.environ.get("WB_API_TOKEN", "").strip()
DB_URL    = os.environ.get("DATABASE_URL", "").strip()
SYNC_MODE = os.environ.get("SYNC_MODE", "orders").strip().lower()

if not WB_TOKEN:
    print("❌ WB_API_TOKEN не задан"); sys.exit(1)
if not DB_URL:
    print("❌ DATABASE_URL не задан"); sys.exit(1)
if SYNC_MODE not in ("orders", "sales", "finances"):
    print(f"❌ SYNC_MODE должен быть 'orders', 'sales' или 'finances', получено: {SYNC_MODE!r}")
    sys.exit(1)

today = date.today()
year  = int(os.environ.get("SYNC_YEAR",  "").strip() or today.year)
month = int(os.environ.get("SYNC_MONTH", "").strip() or today.month)

last_day_num = calendar.monthrange(year, month)[1]
first_day    = date(year, month, 1)
last_day     = date(year, month, last_day_num)
date_from    = datetime(year, month, 1) - timedelta(seconds=1)

print(f"📅 Период: {first_day} — {last_day}  |  Режим: {SYNC_MODE}")

# ── Инициализация БД и rate limiter ───────────────────────────────────────────
engine  = init_db(DB_URL)
Session = get_session_factory(engine)
rl      = DBRateLimiter(engine)

# ── Проверяем, не заблокирован ли токен ───────────────────────────────────────
blocked_for = rl.check_blocked()
if blocked_for is not None:
    # Не ошибка — просто ждём разблокировки, выходим чисто
    sys.exit(0)

# ── Один запрос к WB API ──────────────────────────────────────────────────────
client   = WBClient(WB_TOKEN, rate_limiter=rl)
progress = lambda msg: print(f"  {msg}")

log_repo  = SyncLogRepository()

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


# ════════════════════════════════════════════════════════════════════════════════
if SYNC_MODE == "orders":
    # ── Заказы ────────────────────────────────────────────────────────────────
    print("📥 Запрашиваю заказы у WB API...")
    with Session() as session:
        log = log_repo.create(session, platform="wb", sync_type="orders_unified",
                              date_from=first_day, date_to=last_day, status="running")

    n_orders  = 0
    error_msg = None
    try:
        raw_orders = client.get_orders(date_from, flag=0, on_progress=progress)
        print(f"   Получено от API: {len(raw_orders)}")

        raw_filtered = filter_by_month(raw_orders, year, month)
        print(f"   После фильтра по {month:02d}.{year}: {len(raw_filtered)}")

        orders = parse_orders(raw_filtered)
        order_repo = OrderRepository()
        with Session() as session:
            n_orders = order_repo.upsert_many(session, orders)
        print(f"✅ Сохранено заказов: {n_orders}")

    except WBApiError as e:
        error_msg = str(e)
        print(f"❌ Ошибка WB API: {e}")
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Ошибка: {e}")
    finally:
        status = "success" if not error_msg else "error"
        with Session() as session:
            log_repo.finish(session, log.id, status,
                            orders_loaded=n_orders, sales_loaded=0,
                            error_message=error_msg)

    if error_msg:
        sys.exit(1)
    print(f"🎉 Заказов за {month:02d}.{year}: {n_orders}")


# ════════════════════════════════════════════════════════════════════════════════
elif SYNC_MODE == "sales":
    # ── Выкупы (продажи) ──────────────────────────────────────────────────────
    print("📥 Запрашиваю выкупы у WB API...")
    with Session() as session:
        log = log_repo.create(session, platform="wb", sync_type="sales_unified",
                              date_from=first_day, date_to=last_day, status="running")

    n_sales   = 0
    error_msg = None
    try:
        raw_sales = client.get_sales(date_from, flag=0, on_progress=progress)
        print(f"   Получено от API: {len(raw_sales)}")

        raw_filtered = filter_by_month(raw_sales, year, month)
        print(f"   После фильтра по {month:02d}.{year}: {len(raw_filtered)}")

        sales = parse_sales(raw_filtered)
        sale_repo = SaleRepository()
        with Session() as session:
            n_sales = sale_repo.upsert_many(session, sales)
        print(f"✅ Сохранено выкупов: {n_sales}")

    except WBApiError as e:
        error_msg = str(e)
        print(f"❌ Ошибка WB API: {e}")
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Ошибка: {e}")
    finally:
        status = "success" if not error_msg else "error"
        with Session() as session:
            log_repo.finish(session, log.id, status,
                            orders_loaded=0, sales_loaded=n_sales,
                            error_message=error_msg)

    if error_msg:
        sys.exit(1)
    print(f"🎉 Выкупов за {month:02d}.{year}: {n_sales}")


# ════════════════════════════════════════════════════════════════════════════════
elif SYNC_MODE == "finances":
    # ── Финансовый отчёт ──────────────────────────────────────────────────────
    from src.db.repository import FinancialReportRepository
    print("📥 Запрашиваю финансовый отчёт у WB API...")
    with Session() as session:
        log = log_repo.create(session, platform="wb", sync_type="finances_unified",
                              date_from=first_day, date_to=last_day, status="running")

    n_rows    = 0
    error_msg = None
    try:
        raw_rows = client.get_financial_report(
            datetime(year, month, 1),
            datetime(year, month, last_day_num, 23, 59, 59),
            on_progress=progress,
        )
        print(f"   Получено строк: {len(raw_rows)}")

        rows = parse_financial_report(raw_rows)
        fin_repo = FinancialReportRepository()
        with Session() as session:
            n_rows = fin_repo.upsert_many(session, rows)
        print(f"✅ Сохранено строк финотчёта: {n_rows}")

    except WBApiError as e:
        error_msg = str(e)
        print(f"❌ Ошибка WB API: {e}")
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Ошибка: {e}")
    finally:
        status = "success" if not error_msg else "error"
        with Session() as session:
            log_repo.finish(session, log.id, status,
                            orders_loaded=0, sales_loaded=n_rows,
                            error_message=error_msg)

    if error_msg:
        sys.exit(1)
    print(f"🎉 Финотчёт за {month:02d}.{year}: {n_rows} строк")
