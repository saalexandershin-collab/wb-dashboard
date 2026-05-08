from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import select, and_, func, extract
from datetime import date, datetime
from typing import Optional
import pandas as pd

from src.db.models import Order, Sale, SyncLog


class OrderRepository:

    def upsert_many(self, session: Session, records: list[dict]):
        if not records:
            return 0
        stmt = pg_insert(Order).values(records)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_orders_platform_srid",
            set_={
                "is_cancel": stmt.excluded.is_cancel,
                "cancel_dt": stmt.excluded.cancel_dt,
                "last_change_date": stmt.excluded.last_change_date,
                "total_price": stmt.excluded.total_price,
                "finished_price": stmt.excluded.finished_price,
                "price_with_disc": stmt.excluded.price_with_disc,
            }
        )
        session.execute(stmt)
        session.commit()
        return len(records)

    def get_by_month(
        self,
        session: Session,
        year: int,
        month: int,
        platform: str = "wb",
    ) -> pd.DataFrame:
        stmt = select(Order).where(
            and_(
                Order.platform == platform,
                extract("year", Order.order_date) == year,
                extract("month", Order.order_date) == month,
            )
        )
        rows = session.execute(stmt).scalars().all()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([_order_to_dict(r) for r in rows])


class SaleRepository:

    def upsert_many(self, session: Session, records: list[dict]):
        if not records:
            return 0
        stmt = pg_insert(Sale).values(records)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_sales_platform_sale_id",
            set_={
                "price_with_disc": stmt.excluded.price_with_disc,
                "finished_price": stmt.excluded.finished_price,
                "for_pay": stmt.excluded.for_pay,
                "last_change_date": stmt.excluded.last_change_date,
            }
        )
        session.execute(stmt)
        session.commit()
        return len(records)

    def get_by_month(
        self,
        session: Session,
        year: int,
        month: int,
        platform: str = "wb",
    ) -> pd.DataFrame:
        stmt = select(Sale).where(
            and_(
                Sale.platform == platform,
                extract("year", Sale.sale_date) == year,
                extract("month", Sale.sale_date) == month,
            )
        )
        rows = session.execute(stmt).scalars().all()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([_sale_to_dict(r) for r in rows])


class SyncLogRepository:

    def create(self, session: Session, **kwargs) -> SyncLog:
        log = SyncLog(**kwargs)
        session.add(log)
        session.commit()
        session.refresh(log)
        return log

    def finish(self, session: Session, log_id: int, status: str, **kwargs):
        log = session.get(SyncLog, log_id)
        if log:
            log.status = status
            log.finished_at = datetime.utcnow()
            for k, v in kwargs.items():
                setattr(log, k, v)
            session.commit()

    def get_last(self, session: Session, platform: str = "wb") -> Optional[SyncLog]:
        stmt = (
            select(SyncLog)
            .where(SyncLog.platform == platform, SyncLog.status == "success")
            .order_by(SyncLog.finished_at.desc())
            .limit(1)
        )
        return session.execute(stmt).scalar_one_or_none()


def _order_to_dict(o: Order) -> dict:
    return {
        "id": o.id,
        "srid": o.srid,
        "nm_id": o.nm_id,
        "supplier_article": o.supplier_article,
        "barcode": o.barcode,
        "brand": o.brand,
        "subject": o.subject,
        "category": o.category,
        "warehouse_name": o.warehouse_name,
        "region_name": o.region_name,
        "total_price": o.total_price,
        "discount_percent": o.discount_percent,
        "finished_price": o.finished_price,
        "price_with_disc": o.price_with_disc,
        "order_date": o.order_date,
        "is_cancel": o.is_cancel,
    }


def _sale_to_dict(s: Sale) -> dict:
    return {
        "id": s.id,
        "sale_id": s.sale_id,
        "nm_id": s.nm_id,
        "supplier_article": s.supplier_article,
        "barcode": s.barcode,
        "brand": s.brand,
        "subject": s.subject,
        "category": s.category,
        "warehouse_name": s.warehouse_name,
        "region_name": s.region_name,
        "finished_price": s.finished_price,
        "for_pay": s.for_pay,
        "price_with_disc": s.price_with_disc,
        "sale_date": s.sale_date,
        "is_return": s.is_return,
    }
