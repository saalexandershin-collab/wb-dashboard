from sqlalchemy import (
    Column, String, Integer, BigInteger, Float, Boolean,
    DateTime, Date, Text, UniqueConstraint, Index, create_engine
)
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from datetime import datetime


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    platform = Column(String(20), nullable=False, default="wb")

    # Уникальный ключ WB
    srid = Column(String(100), nullable=False)

    # Товар
    nm_id = Column(Integer)
    supplier_article = Column(String(200))
    barcode = Column(String(100))
    brand = Column(String(200))
    subject = Column(String(200))
    category = Column(String(200))

    # Логистика
    warehouse_name = Column(String(200))
    region_name = Column(String(200))
    country_name = Column(String(200))

    # Суммы
    total_price = Column(Float)
    discount_percent = Column(Integer)
    spp = Column(Float)
    finished_price = Column(Float)
    price_with_disc = Column(Float)

    # Даты
    order_date = Column(DateTime)
    last_change_date = Column(DateTime)

    # Статус
    is_cancel = Column(Boolean, default=False)
    cancel_dt = Column(DateTime)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("platform", "srid", name="uq_orders_platform_srid"),
        Index("ix_orders_platform_order_date", "platform", "order_date"),
    )


class Sale(Base):
    __tablename__ = "sales"

    id = Column(Integer, primary_key=True, autoincrement=True)
    platform = Column(String(20), nullable=False, default="wb")

    # Уникальный ключ WB
    sale_id = Column(String(100), nullable=False)

    # Товар
    nm_id = Column(Integer)
    supplier_article = Column(String(200))
    barcode = Column(String(100))
    brand = Column(String(200))
    subject = Column(String(200))
    category = Column(String(200))

    # Логистика
    warehouse_name = Column(String(200))
    region_name = Column(String(200))
    country_name = Column(String(200))

    # Суммы
    price_with_disc = Column(Float)
    finished_price = Column(Float)
    for_pay = Column(Float)
    total_price = Column(Float)
    discount_percent = Column(Integer)
    spp = Column(Float)

    # Дата продажи / возврата
    sale_date = Column(DateTime)
    last_change_date = Column(DateTime)

    # Тип: True = возврат (saleId начинается с R), False = продажа
    is_return = Column(Boolean, default=False)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("platform", "sale_id", name="uq_sales_platform_sale_id"),
        Index("ix_sales_platform_sale_date", "platform", "sale_date"),
    )


class SyncLog(Base):
    __tablename__ = "sync_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    platform = Column(String(20), nullable=False)
    sync_type = Column(String(50))
    date_from = Column(Date)
    date_to = Column(Date)
    orders_loaded = Column(Integer, default=0)
    sales_loaded = Column(Integer, default=0)
    status = Column(String(20))
    error_message = Column(Text)
    started_at = Column(DateTime, default=datetime.utcnow)
    finished_at = Column(DateTime)


class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    platform = Column(String(20), nullable=False, default="wb")

    nm_id = Column(Integer)
    supplier_article = Column(String(200))
    barcode = Column(String(100))
    brand = Column(String(200))
    subject = Column(String(200))
    category = Column(String(200))
    warehouse_name = Column(String(200))
    quantity = Column(Integer, default=0)
    in_way_to_client = Column(Integer, default=0)
    in_way_from_client = Column(Integer, default=0)
    quantity_full = Column(Integer, default=0)

    synced_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("platform", "nm_id", "barcode", "warehouse_name", name="uq_stocks_key"),
        Index("ix_stocks_platform_nm_id", "platform", "nm_id"),
    )


class FinancialReport(Base):
    __tablename__ = "financial_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    platform = Column(String(20), nullable=False, default="wb")

    # Идентификаторы
    rrd_id = Column(BigInteger)                 # уникальный ID строки отчёта (>2B, нужен BIGINT)
    realizationreport_id = Column(BigInteger)   # ID недельного отчёта

    # Период отчёта
    date_from = Column(Date)
    date_to = Column(Date)
    create_dt = Column(DateTime)                # дата транзакции (для группировки по месяцу)

    # Товар
    nm_id = Column(Integer)
    supplier_article = Column(String(200))
    brand_name = Column(String(200))
    subject_name = Column(String(200))

    # Тип операции
    doc_type_name = Column(String(100))         # Продажа / Возврат / Корректировка
    supplier_oper_name = Column(String(200))    # детальное название операции
    quantity = Column(Integer, default=0)

    # Финансы
    retail_price = Column(Float)                # полная цена товара
    retail_price_withdisc_rub = Column(Float)   # цена продажи (с учётом скидки)
    ppvz_for_pay = Column(Float)                # начислено продавцу по этой строке
    ppvz_sales_commission = Column(Float)       # комиссия WB
    delivery_rub = Column(Float)                # логистика
    penalty = Column(Float)                     # штраф
    additional_payment = Column(Float)          # прочие удержания / доплаты
    storage_fee = Column(Float)                 # хранение
    acquiring_fee = Column(Float)               # эквайринг

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("platform", "rrd_id", name="uq_fin_platform_rrd_id"),
        Index("ix_fin_platform_create_dt", "platform", "create_dt"),
    )


def get_engine(db_url: str):
    if db_url.startswith("postgresql"):
        return create_engine(
            db_url,
            pool_pre_ping=True,
            connect_args={"options": "-c statement_timeout=0"},
        )
    return create_engine(db_url, pool_pre_ping=True)


def get_session_factory(engine):
    return sessionmaker(bind=engine, expire_on_commit=False)


def init_db(db_url: str):
    engine = get_engine(db_url)
    try:
        Base.metadata.create_all(engine)
    except Exception:
        # Supabase race condition: два воркера одновременно пытаются CREATE TABLE
        # Повторный вызов безопасен — IF NOT EXISTS пропустит уже созданные таблицы
        try:
            Base.metadata.create_all(engine)
        except Exception:
            pass
    return engine
