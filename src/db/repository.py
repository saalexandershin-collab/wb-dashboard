from sqlalchemy.orm import Session
from sqlalchemy import select, and_, delete
from datetime import datetime
import calendar
from typing import Optional
import pandas as pd

from src.db.models import Order, Sale, SyncLog, Stock, FinancialReport, OzonPosting, OzonStock, OzonTransaction


def _get_dialect(session: Session) -> str:
    try:
        return session.get_bind().dialect.name
    except Exception:
        return "sqlite"


class OrderRepository:

    def upsert_many(self, session: Session, records: list[dict]):
        if not records:
            return 0
        platform = records[0]["platform"]
        dialect = _get_dialect(session)
        if dialect == "postgresql":
            # Атомарный UPSERT через INSERT ... ON CONFLICT — без duplicate key при конкуренции
            from sqlalchemy.dialects.postgresql import insert as pg_insert
            stmt = pg_insert(Order).values(records)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_orders_platform_srid",
                set_={
                    "nm_id": stmt.excluded.nm_id,
                    "supplier_article": stmt.excluded.supplier_article,
                    "brand": stmt.excluded.brand,
                    "subject": stmt.excluded.subject,
                    "category": stmt.excluded.category,
                    "warehouse_name": stmt.excluded.warehouse_name,
                    "region_name": stmt.excluded.region_name,
                    "total_price": stmt.excluded.total_price,
                    "discount_percent": stmt.excluded.discount_percent,
                    "finished_price": stmt.excluded.finished_price,
                    "price_with_disc": stmt.excluded.price_with_disc,
                    "order_date": stmt.excluded.order_date,
                    "is_cancel": stmt.excluded.is_cancel,
                    "last_change_date": stmt.excluded.last_change_date,
                },
            )
            session.execute(stmt)
        else:
            # SQLite: delete + insert
            srids = [r["srid"] for r in records]
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
        platform = records[0]["platform"]
        dialect = _get_dialect(session)
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert
            stmt = pg_insert(Sale).values(records)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_sales_platform_sale_id",
                set_={
                    "nm_id": stmt.excluded.nm_id,
                    "supplier_article": stmt.excluded.supplier_article,
                    "brand": stmt.excluded.brand,
                    "subject": stmt.excluded.subject,
                    "category": stmt.excluded.category,
                    "warehouse_name": stmt.excluded.warehouse_name,
                    "region_name": stmt.excluded.region_name,
                    "price_with_disc": stmt.excluded.price_with_disc,
                    "finished_price": stmt.excluded.finished_price,
                    "for_pay": stmt.excluded.for_pay,
                    "total_price": stmt.excluded.total_price,
                    "discount_percent": stmt.excluded.discount_percent,
                    "sale_date": stmt.excluded.sale_date,
                    "is_return": stmt.excluded.is_return,
                    "last_change_date": stmt.excluded.last_change_date,
                },
            )
            session.execute(stmt)
        else:
            sale_ids = [r["sale_id"] for r in records]
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
        from datetime import date as date_cls, timedelta
        first_day = date_cls(year, month, 1)
        last_day_num = calendar.monthrange(year, month)[1]
        last_day = date_cls(year, month, last_day_num)
        # Неделя (пн–вс) принадлежит месяцу, в котором её середина (чт = date_from + 3 дня).
        # Значит берём отчёты с date_from в диапазоне [first_day - 3, last_day - 3].
        df_min = first_day - timedelta(days=3)
        df_max = last_day - timedelta(days=3)
        stmt = select(FinancialReport).where(
            and_(
                FinancialReport.platform == platform,
                FinancialReport.date_from >= df_min,
                FinancialReport.date_from <= df_max,
            )
        )
        rows = session.execute(stmt).scalars().all()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([_fin_to_dict(r) for r in rows])


class OzonPostingRepository:

    def upsert_many(self, session: Session, records: list[dict]):
        if not records:
            return 0
        keys = [(r["posting_number"], r["sku"]) for r in records]
        posting_numbers = list({r["posting_number"] for r in records})
        skus = list({r["sku"] for r in records if r.get("sku")})
        session.execute(
            delete(OzonPosting).where(
                OzonPosting.posting_number.in_(posting_numbers),
                OzonPosting.sku.in_(skus),
            )
        )
        session.bulk_insert_mappings(OzonPosting, records)
        session.commit()
        return len(records)

    def get_by_month(self, session: Session, year: int, month: int) -> pd.DataFrame:
        date_from = datetime(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        date_to = datetime(year, month, last_day, 23, 59, 59)
        stmt = select(OzonPosting).where(
            OzonPosting.created_at >= date_from,
            OzonPosting.created_at <= date_to,
        )
        rows = session.execute(stmt).scalars().all()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([_ozon_posting_to_dict(r) for r in rows])


class OzonStockRepository:

    def replace_all(self, session: Session, records: list[dict]):
        if not records:
            return 0
        session.execute(delete(OzonStock))
        session.bulk_insert_mappings(OzonStock, records)
        session.commit()
        return len(records)

    def get_all(self, session: Session) -> pd.DataFrame:
        rows = session.execute(select(OzonStock)).scalars().all()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([_ozon_stock_to_dict(r) for r in rows])

    def get_synced_at(self, session: Session):
        return session.execute(select(OzonStock.synced_at).limit(1)).scalar_one_or_none()


class OzonTransactionRepository:

    def upsert_many(self, session: Session, records: list[dict]):
        if not records:
            return 0
        # Deduplicate by (operation_id, sku) — API may return duplicates
        seen = {}
        for r in records:
            key = (r["operation_id"], r.get("sku") or "")
            seen[key] = r
        records = list(seen.values())
        op_ids = list({r["operation_id"] for r in records})
        session.execute(
            delete(OzonTransaction).where(
                OzonTransaction.operation_id.in_(op_ids),
            )
        )
        session.bulk_insert_mappings(OzonTransaction, records)
        session.commit()
        return len(records)

    def get_by_month(self, session: Session, year: int, month: int) -> pd.DataFrame:
        date_from = datetime(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        date_to = datetime(year, month, last_day, 23, 59, 59)
        stmt = select(OzonTransaction).where(
            OzonTransaction.operation_date >= date_from,
            OzonTransaction.operation_date <= date_to,
        )
        rows = session.execute(stmt).scalars().all()
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame([_ozon_tx_to_dict(r) for r in rows])

        # Finance API often omits offer_id — fill from postings via posting_number
        missing_mask = df["offer_id"].fillna("") == ""
        pn_missing = df.loc[missing_mask, "posting_number"].dropna().unique().tolist()
        if pn_missing:
            p_stmt = select(OzonPosting).where(OzonPosting.posting_number.in_(pn_missing))
            postings = session.execute(p_stmt).scalars().all()
            if postings:
                pmap_offer = {p.posting_number: p.offer_id or "" for p in postings}
                pmap_name  = {p.posting_number: p.product_name or "" for p in postings}
                df.loc[missing_mask, "offer_id"] = (
                    df.loc[missing_mask, "posting_number"].map(pmap_offer).fillna("")
                )
                name_missing = missing_mask & (df["product_name"].fillna("") == "")
                df.loc[name_missing, "product_name"] = (
                    df.loc[name_missing, "posting_number"].map(pmap_name).fillna("")
                )

        # Fallback: if offer_id still missing but product_name is known,
        # look up offer_id from any posting with the same product_name
        still_missing = df["offer_id"].fillna("") == ""
        if still_missing.any():
            known_names = df.loc[still_missing, "product_name"].dropna().unique().tolist()
            known_names = [n for n in known_names if n]
            if known_names:
                name_stmt = select(OzonPosting.product_name, OzonPosting.offer_id).where(
                    OzonPosting.product_name.in_(known_names),
                    OzonPosting.offer_id.isnot(None),
                    OzonPosting.offer_id != "",
                ).distinct()
                name_rows = session.execute(name_stmt).all()
                if name_rows:
                    nmap = {r.product_name: r.offer_id for r in name_rows}
                    df.loc[still_missing, "offer_id"] = (
                        df.loc[still_missing, "product_name"].map(nmap).fillna("")
                    )
        return df


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


def _ozon_posting_to_dict(p: OzonPosting) -> dict:
    return {
        "id": p.id, "posting_number": p.posting_number,
        "order_id": p.order_id, "order_number": p.order_number,
        "order_type": p.order_type, "sku": p.sku, "offer_id": p.offer_id,
        "product_name": p.product_name, "quantity": p.quantity,
        "price": p.price, "total_discount_value": p.total_discount_value,
        "commission_amount": p.commission_amount, "payout": p.payout,
        "old_price": p.old_price, "warehouse_name": p.warehouse_name,
        "region": p.region, "status": p.status, "is_cancelled": p.is_cancelled,
        "created_at": p.created_at, "in_process_at": p.in_process_at,
        "shipment_date": p.shipment_date,
    }


def _ozon_stock_to_dict(s: OzonStock) -> dict:
    return {
        "id": s.id, "sku": s.sku, "offer_id": s.offer_id,
        "product_name": s.product_name, "warehouse_name": s.warehouse_name,
        "free_to_sell_amount": s.free_to_sell_amount,
        "promised_amount": s.promised_amount, "reserved_amount": s.reserved_amount,
        "synced_at": s.synced_at,
    }


def _ozon_tx_to_dict(t: OzonTransaction) -> dict:
    return {
        "id": t.id, "operation_id": t.operation_id,
        "operation_date": t.operation_date, "operation_type": t.operation_type,
        "operation_type_name": t.operation_type_name,
        "posting_number": t.posting_number, "order_id": t.order_id,
        "sku": t.sku, "offer_id": t.offer_id, "product_name": t.product_name,
        "quantity": t.quantity, "amount": t.amount,
        "accruals_for_sale": t.accruals_for_sale, "sale_commission": t.sale_commission,
        "delivery_charge": t.delivery_charge,
        "return_delivery_charge": t.return_delivery_charge,
        "period_from": t.period_from, "period_to": t.period_to,
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
