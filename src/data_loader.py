"""
Централизованный загрузчик данных для всего дашборда.

Все функции декорированы @st.cache_data — кэш разделяется между всеми страницами.
Это значит: первая открытая вкладка делает запрос в БД, все остальные
используют уже готовый кэш без дополнительных запросов.

TTL = 600 секунд (10 минут). Сбросить кэш вручную: st.cache_data.clear()
"""

import streamlit as st
import pandas as pd

from src.db.models import init_db, get_session_factory
from src.db.repository import (
    OrderRepository,
    SaleRepository,
    StockRepository,
    FinancialReportRepository,
    OzonPostingRepository,
    OzonStockRepository,
    OzonTransactionRepository,
)

_TTL = 600  # 10 минут


# ── WB: финансовые отчёты (FinancialReport) ──────────────────────────────────

@st.cache_data(ttl=_TTL, show_spinner="Загружаю финансовые данные WB…")
def load_wb_financial(db_url: str, year: int, month: int) -> pd.DataFrame:
    """WB financial_reports за один месяц (retail, ppvz, комиссии…)."""
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        return FinancialReportRepository().get_by_month(session, year, month)


@st.cache_data(ttl=_TTL, show_spinner="Загружаю данные WB…")
def load_wb_financial_range(
    db_url: str, year_from: int, month_from: int, year_to: int, month_to: int
) -> pd.DataFrame:
    """WB financial_reports за диапазон месяцев (для страницы Сверка)."""
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    repo = FinancialReportRepository()
    frames: list[pd.DataFrame] = []
    y, m = year_from, month_from
    while (y, m) <= (year_to, month_to):
        with Session() as session:
            df = repo.get_by_month(session, y, m, platform="wb")
        if not df.empty:
            df["_year"] = y
            df["_month"] = m
            frames.append(df)
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── WB: заказы и продажи ─────────────────────────────────────────────────────

@st.cache_data(ttl=_TTL, show_spinner="Загружаю заказы WB…")
def load_wb_orders(db_url: str, year: int, month: int) -> pd.DataFrame:
    """Заказы WB за один месяц."""
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        return OrderRepository().get_by_month(session, year, month, platform="wb")


@st.cache_data(ttl=_TTL, show_spinner="Загружаю продажи WB…")
def load_wb_sales(db_url: str, year: int, month: int) -> pd.DataFrame:
    """Продажи WB за один месяц."""
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        return SaleRepository().get_by_month(session, year, month, platform="wb")


# ── WB: склады ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=_TTL, show_spinner="Загружаю остатки WB…")
def load_wb_stocks(db_url: str):
    """Текущие остатки WB + дата последней синхронизации."""
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    repo = StockRepository()
    with Session() as session:
        stocks_df = repo.get_all(session)
        synced_at = repo.get_synced_at(session)
    return stocks_df, synced_at


# ── Ozon: постинги ───────────────────────────────────────────────────────────

@st.cache_data(ttl=_TTL, show_spinner="Загружаю постинги Ozon…")
def load_ozon_postings(db_url: str, year: int, month: int) -> pd.DataFrame:
    """Ozon постинги за один месяц."""
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        return OzonPostingRepository().get_by_month(session, year, month)


@st.cache_data(ttl=_TTL, show_spinner="Загружаю постинги Ozon…")
def load_ozon_postings_range(
    db_url: str, year_from: int, month_from: int, year_to: int, month_to: int
) -> pd.DataFrame:
    """Ozon постинги за диапазон месяцев (для страницы Сверка)."""
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    repo = OzonPostingRepository()
    frames: list[pd.DataFrame] = []
    y, m = year_from, month_from
    while (y, m) <= (year_to, month_to):
        with Session() as session:
            df = repo.get_by_month(session, y, m)
        if not df.empty:
            df["_year"] = y
            df["_month"] = m
            frames.append(df)
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── Ozon: транзакции ─────────────────────────────────────────────────────────

@st.cache_data(ttl=_TTL, show_spinner="Загружаю транзакции Ozon…")
def load_ozon_transactions(db_url: str, year: int, month: int) -> pd.DataFrame:
    """Ozon транзакции за один месяц."""
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        return OzonTransactionRepository().get_by_month(session, year, month)


@st.cache_data(ttl=_TTL, show_spinner="Загружаю транзакции Ozon…")
def load_ozon_transactions_range(
    db_url: str, year_from: int, month_from: int, year_to: int, month_to: int
) -> pd.DataFrame:
    """Ozon транзакции за диапазон месяцев (для страницы Сверка)."""
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    repo = OzonTransactionRepository()
    frames: list[pd.DataFrame] = []
    y, m = year_from, month_from
    while (y, m) <= (year_to, month_to):
        try:
            with Session() as session:
                df = repo.get_by_month(session, y, m)
            if not df.empty:
                df["_year"] = y
                df["_month"] = m
                frames.append(df)
        except Exception:
            pass
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── Ozon: склады ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=_TTL, show_spinner="Загружаю остатки Ozon…")
def load_ozon_stocks(db_url: str):
    """Текущие остатки Ozon + дата последней синхронизации."""
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    repo = OzonStockRepository()
    with Session() as session:
        stocks_df = repo.get_all(session)
        synced_at = repo.get_synced_at(session)
    return stocks_df, synced_at
