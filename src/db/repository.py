from sqlalchemy.orm import Session
from sqlalchemy import select, and_, delete
from datetime import datetime
import calendar
from typing import Optional
import pandas as pd

from src.db.models import Order, Sale, SyncLog, Stock, FinancialReport


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
        date_from = datetime(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        date_to = datetime(year, month, last_day, 23, 59, 59)
        stmt = select(Order).where(
            and_(
                Order.platform == platform,
                Order.order_date >= date_from,
                Order.order_date <= date_to,
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
        date_from = datetime(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        date_to = datetime(year, month, last_day, 23, 59, 59)
        stmt = select(Sale).where(
            and_(
                Sale.platform == platform,
                Sale.sale_date >= date_from,
                Sale.sale_date <= date_to,
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


class StockRepository:

    def replace_all(self, session: Session, records: list[dict], platform: str = "wb"):
        """Полностью заменяет остатки: удаляет старые, вставляет новые."""
        if not records:
            return 0
        session.execute(delete(Stock).where(Stock.platform == platform))
        session.bulk_insert_mappings(Stock, records)
        session.commit()
        return len(records)

    def get_all(self, session: Session, platform: str = "wb") -> pd.DataFrame:
        stmt = select(Stock).where(Stock.platform == platform)
        rows = session.execute(stmt).scalars().all()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([_stock_to_dict(r) for r in rows])

    def get_synced_at(self, session: Session, platform: str = "wb"):
        stmt = select(Stock.synced_at).where(Stock.platform == platform).limit(1)
        result = session.execute(stmt).scalar_one_or_none()
        return result


class FinancialReportRepository:

    def upsert_many(self, session: Session, records: list[dict]):
        if not records:
            return 0
        rrd_ids = [r["rrd_id"] for r in records if r.get("rrd_id")]
        platform = records[0]["platform"]
        session.execute(
            delete(FinancialReport).where(
                FinancialReport.platform == platform,
                FinancialReport.rrd_id.in_(rrd_ids),
            )
        )
        session.bulk_insert_mappings(FinancialReport, records)
        session.commit()
        return len(records)

    def get_by_month(self, session: Session, year: int, month: int, platform: str = "wb") -> pd.DataFrame:
        date_from = datetime(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        date_to = datetime(year, month, last_day, 23, 59, 59)
        stmt = select(FinancialReport).where(
            and_(
                FinancialReport.platform == platform,
                FinancialReport.create_dt >= date_from,
                FinancialReport.create_dt <= date_to,
            )
        )
        rows = session.execute(stmt).scalars().all()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([_fin_to_dict(r) for r in rows])


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


def _stock_to_dict(s: Stock) -> dict:
    return {
        "nm_id": s.nm_id, "supplier_article": s.supplier_article,
        "barcode": s.barcode, "brand": s.brand,
        "subject": s.subject, "category": s.category,
        "warehouse_name": s.warehouse_name,
        "quantity": s.quantity,
        "in_way_to_client": s.in_way_to_client,
        "in_way_from_client": s.in_way_from_client,
        "quantity_full": s.quantity_full,
        "synced_at": s.synced_at,
    }


def _fin_to_dict(f: FinancialReport) -> dict:
    return {
        "id": f.id, "rrd_id": f.rrd_id,
        "realizationreport_id": f.realizationreport_id,
        "date_from": f.date_from, "date_to": f.date_to,
        "create_dt": f.create_dt,
        "nm_id": f.nm_id, "supplier_article": f.supplier_article,
        "brand_name": f.brand_name, "subject_name": f.subject_name,
        "doc_type_name": f.doc_type_name,
        "supplier_oper_name": f.supplier_oper_name,
        "quantity": f.quantity,
        "retail_price": f.retail_price,
        "retail_price_withdisc_rub": f.retail_price_withdisc_rub,
        "ppvz_for_pay": f.ppvz_for_pay,
        "ppvz_sales_commission": f.ppvz_sales_commission,
        "delivery_rub": f.delivery_rub,
        "penalty": f.penalty,
        "additional_payment": f.additional_payment,
        "storage_fee": f.storage_fee,
        "acquiring_fee": f.acquiring_fee,
    }
