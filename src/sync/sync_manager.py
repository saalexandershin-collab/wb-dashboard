from datetime import datetime, date, timedelta
from typing import Optional, Callable
import calendar

from sqlalchemy.orm import Session

from src.api.wb_client import WBClient, WBApiError, parse_orders, parse_sales
from src.db.models import init_db, get_session_factory
from src.db.repository import OrderRepository, SaleRepository, SyncLogRepository


order_repo = OrderRepository()
sale_repo = SaleRepository()
log_repo = SyncLogRepository()


class SyncManager:

    def __init__(self, db_url: str, wb_token: str):
        self.engine = init_db(db_url)
        self.Session = get_session_factory(self.engine)
        self.wb_client = WBClient(wb_token)

    def sync_month(
        self,
        year: int,
        month: int,
        on_progress: Optional[Callable[[str], None]] = None,
    ) -> dict:
        first_day = datetime(year, month, 1)
        last_day_num = calendar.monthrange(year, month)[1]
        last_day = datetime(year, month, last_day_num, 23, 59, 59)

        with self.Session() as session:
            log = log_repo.create(
                session,
                platform="wb",
                sync_type="month",
                date_from=first_day.date(),
                date_to=last_day.date(),
                status="running",
            )
            try:
                raw_orders, raw_sales = self.wb_client.get_orders_and_sales(
                    date_from=first_day,
                    on_progress=on_progress,
                )

                # Фильтруем только нужный месяц (API может вернуть больше)
                raw_orders = _filter_by_month(raw_orders, year, month, "date")
                raw_sales = _filter_by_month(raw_sales, year, month, "date")

                orders = parse_orders(raw_orders)
                sales = parse_sales(raw_sales)

                if on_progress:
                    on_progress(f"Сохраняю {len(orders)} заказов в БД...")
                n_orders = order_repo.upsert_many(session, orders)

                if on_progress:
                    on_progress(f"Сохраняю {len(sales)} продаж/возвратов в БД...")
                n_sales = sale_repo.upsert_many(session, sales)

                log_repo.finish(
                    session, log.id, "success",
                    orders_loaded=n_orders,
                    sales_loaded=n_sales,
                )
                if on_progress:
                    on_progress(f"Готово! Загружено: заказов {n_orders}, продаж/возвратов {n_sales}.")
                return {"status": "success", "orders": n_orders, "sales": n_sales}

            except WBApiError as e:
                log_repo.finish(session, log.id, "error", error_message=str(e))
                raise
            except Exception as e:
                log_repo.finish(session, log.id, "error", error_message=str(e))
                raise

    def get_last_sync_info(self) -> Optional[dict]:
        with self.Session() as session:
            log = log_repo.get_last(session)
            if not log:
                return None
            return {
                "finished_at": log.finished_at,
                "orders_loaded": log.orders_loaded,
                "sales_loaded": log.sales_loaded,
                "date_from": log.date_from,
                "date_to": log.date_to,
            }

    def auto_sync_if_needed(self, year: int, month: int, max_age_hours: int = 12):
        info = self.get_last_sync_info()
        if info is None:
            return self.sync_month(year, month)
        age = datetime.utcnow() - info["finished_at"]
        if age.total_seconds() > max_age_hours * 3600:
            return self.sync_month(year, month)
        return None


def _filter_by_month(records: list[dict], year: int, month: int, date_field: str) -> list[dict]:
    result = []
    for r in records:
        raw_date = r.get(date_field, "")
        if not raw_date:
            continue
        try:
            dt = datetime.strptime(str(raw_date)[:10], "%Y-%m-%d")
            if dt.year == year and dt.month == month:
                result.append(r)
        except ValueError:
            continue
    return result
