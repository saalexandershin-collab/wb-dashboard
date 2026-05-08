from sqlalchemy import (
    Column, String, Integer, Float, Boolean,
    DateTime, Date, Text, UniqueConstraint, create_engine
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


def get_engine(db_url: str):
    return create_engine(db_url, pool_pre_ping=True)


def get_session_factory(engine):
    return sessionmaker(bind=engine, expire_on_commit=False)


def init_db(db_url: str):
    engine = get_engine(db_url)
    Base.metadata.create_all(engine)
    return engine
