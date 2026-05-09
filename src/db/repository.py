from sqlalchemy.orm import Session
from sqlalchemy import select, and_, extract, delete
from datetime import datetime
from typing import Optional
import pandas as pd

from src.db.models import Order, Sale, SyncLog


class OrderRepository:

    def upsert_many(self, session: Session, records: list[dict]):
        if not records:
            return 0
        srids = [r["srid"] for r in records]
        platform = records[0]["platform"]
        # Удаляем существующие, потом вставляем заново — работает и в SQLite, и в PostgreSQL
        session.execute(
            delete(Order).where(Order.platform == platform, Order.srid.in_(srids))
        )
        session.bulk_insert_mappings(Order, records)
        session.commit()
        return len(records)

    def get_by_month(self, session: Session, year: int, month: int, platform: str = "wb") -> pd.DataFrame:
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
        sale_ids = [r["sale_id"] for r in records]
        platform = records[0]["platform"]
        session.execute(
            delete(Sale).where(Sale.platform == platform, Sale.sale_id.in_(sale_ids))
        )
        session.bulk_insert_mappings(Sale, records)
        session.commit()
        return len(records)

    def get_by_month(self, session: Session, year: int, month: int, platform: str = "wb") -> pd.DataFrame:
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

    def delete_by_id(self, session: Session, log_id: int):
        session.execute(delete(SyncLog).where(SyncLog.id == log_id))
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
        "id": o.id, "srid": o.srid, "nm_id": o.nm_id,
        "supplier_article": o.supplier_article, "barcode": o.barcode,
        "brand": o.brand, "subject": o.subject, "category": o.category,
        "warehouse_name": o.warehouse_name, "region_name": o.region_name,
        "total_price": o.total_price, "discount_percent": o.discount_percent,
        "finished_price": o.finished_price, "price_with_disc": o.price_with_disc,
        "order_date": o.order_date, "is_cancel": o.is_cancel,
    }


def _sale_to_dict(s: Sale) -> dict:
    return {
        "id": s.id, "sale_id": s.sale_id, "nm_id": s.nm_id,
        "supplier_article": s.supplier_article, "barcode": s.barcode,
        "brand": s.brand, "subject": s.subject, "category": s.category,
        "warehouse_name": s.warehouse_name, "region_name": s.region_name,
        "finished_price": s.finished_price, "for_pay": s.for_pay,
        "price_with_disc": s.price_with_disc, "sale_date": s.sale_date,
        "is_return": s.is_return,
    }
