import time
from datetime import datetime, date
from typing import Optional, Callable
import calendar

from src.api.wb_client import WBClient, WBApiError, parse_orders, parse_sales
from src.db.models import init_db, get_session_factory
from src.db.repository import OrderRepository, SaleRepository, SyncLogRepository

order_repo = OrderRepository()
sale_repo = SaleRepository()
log_repo = SyncLogRepository()

REQUEST_DELAY = 70  # секунд между запросами (лимит WB: 1 req/мин, берём с запасом)


class SyncManager:

    def __init__(self, db_url: str, wb_token: str):
        self.engine = init_db(db_url)
        self.Session = get_session_factory(self.engine)
        self.wb_client = WBClient(wb_token)

    def sync_month(self, year: int, month: int, on_progress: Optional[Callable] = None) -> dict:
        first_day = datetime(year, month, 1)
        last_day_num = calendar.monthrange(year, month)[1]
        last_day = datetime(year, month, last_day_num, 23, 59, 59)

        with self.Session() as session:
            log = log_repo.create(
                session, platform="wb", sync_type="month",
                date_from=first_day.date(), date_to=last_day.date(), status="running",
            )

        n_orders = 0
        n_sales = 0
        error_msg = None

        try:
            # ── Шаг 1: загрузка заказов ─────────────────────────────────────
            if on_progress:
                on_progress("Загружаю заказы из WB API...")
            raw_orders = self.wb_client.get_orders(first_day, on_progress=on_progress)
            raw_orders = _filter_by_month(raw_orders, year, month, "date")
            orders = parse_orders(raw_orders)

            with self.Session() as session:
                n_orders = order_repo.upsert_many(session, orders)
            if on_progress:
                on_progress(f"Сохранено заказов: {n_orders}.")

            # ── Шаг 2: загрузка продаж (клиент сам выждет лимит) ────────────
            if on_progress:
                on_progress("Загружаю продажи из WB API...")
            raw_sales = self.wb_client.get_sales(first_day, on_progress=on_progress)
            raw_sales = _filter_by_month(raw_sales, year, month, "date")
            sales = parse_sales(raw_sales)

            with self.Session() as session:
                n_sales = sale_repo.upsert_many(session, sales)
            if on_progress:
                on_progress(f"Сохранено продаж/возвратов: {n_sales}.")

        except WBApiError as e:
            error_msg = str(e)
            raise
        except Exception as e:
            error_msg = str(e)
            raise
        finally:
            status = "success" if error_msg is None else "error"
            with self.Session() as session:
                log_repo.finish(
                    session, log.id, status,
                    orders_loaded=n_orders,
                    sales_loaded=n_sales,
                    error_message=error_msg,
                )

        if on_progress:
            on_progress(f"Готово! Заказов: {n_orders}, продаж/возвратов: {n_sales}.")
        return {"status": "success", "orders": n_orders, "sales": n_sales}

    def get_last_sync_info(self) -> Optional[dict]:
        with self.Session() as session:
            log = log_repo.get_last(session)
            if not log:
                return None
            return {
                "finished_at": log.finished_at,
                "orders_loaded": log.orders_loaded,
                "sales_loaded": log.sales_loaded,
            }


def _filter_by_month(records: list[dict], year: int, month: int, date_field: str) -> list[dict]:
    result = []
    for r in records:
        raw = r.get(date_field, "")
        if not raw:
            continue
        try:
            dt = datetime.strptime(str(raw)[:10], "%Y-%m-%d")
            if dt.year == year and dt.month == month:
                result.append(r)
        except ValueError:
            continue
    return result
