import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
import calendar

from src.db.models import init_db, get_session_factory
from src.db.repository import OrderRepository, SaleRepository
from src.auth import require_login

st.set_page_config(page_title="Дашборд", page_icon="📊", layout="wide")
require_login()
st.title("📊 Дашборд продаж")

# ── Проверка конфигурации ────────────────────────────────────────────────────
if "database" not in st.secrets or "wildberries" not in st.secrets:
    st.error("Сначала настройте токен и базу данных в разделе ⚙️ Настройки.")
    st.stop()

DB_URL = st.secrets["database"]["url"]

# ── Выбор периода ────────────────────────────────────────────────────────────
today = date.today()
col_y, col_m = st.sidebar.columns(2)
year = col_y.selectbox("Год", list(range(today.year, today.year - 3, -1)), index=0)
month = col_m.selectbox(
    "Месяц",
    list(range(1, 13)),
    index=today.month - 1,
    format_func=lambda m: calendar.month_name[m],
)
st.sidebar.markdown("---")
st.sidebar.caption(f"Период: {calendar.month_name[month]} {year}")
st.sidebar.markdown("---")
st.sidebar.markdown(f"👤 {st.session_state.get('username', '')}")
if st.sidebar.button("Выйти"):
    st.session_state.clear()
    st.rerun()

# ── Загрузка данных ──────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Загружаю данные...")
def load_data(db_url: str, year: int, month: int):
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        df_orders = OrderRepository().get_by_month(session, year, month)
        df_sales = SaleRepository().get_by_month(session, year, month)
    return df_orders, df_sales

df_orders, df_sales = load_data(DB_URL, year, month)

if df_orders.empty and df_sales.empty:
    st.warning("Данных за выбранный период нет. Загрузите данные в разделе ⚙️ Настройки.")
    st.stop()

# ── Подготовка данных ────────────────────────────────────────────────────────
def prep_orders(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["day"] = df["order_date"].dt.date
    df["is_actual"] = ~df["is_cancel"].fillna(False)
    return df

def prep_sales(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df["day"] = df["sale_date"].dt.date
    return df

ord_df = prep_orders(df_orders)
sal_df = prep_sales(df_sales)

# ── Фильтры в сайдбаре ───────────────────────────────────────────────────────
st.sidebar.markdown("### Фильтры")

# Бренд
all_brands = sorted(ord_df["brand"].dropna().unique().tolist()) if not ord_df.empty else []
sel_brands = st.sidebar.multiselect("Бренд", all_brands)

# Артикул — только актуальные (последнее название по nm_id)
if not ord_df.empty:
    latest_articles = (
        ord_df.groupby("nm_id")["supplier_article"]
        .last()
        .reset_index()
        .sort_values("supplier_article")
    )
    all_articles = latest_articles["supplier_article"].dropna().tolist()
else:
    latest_articles = pd.DataFrame(columns=["nm_id", "supplier_article"])
    all_articles = []

sel_articles = st.sidebar.multiselect("Артикул продавца", all_articles)

# Применяем фильтры
if sel_brands:
    ord_df = ord_df[ord_df["brand"].isin(sel_brands)]
    sal_df = sal_df[sal_df["brand"].isin(sel_brands)] if not sal_df.empty else sal_df

if sel_articles:
    nm_ids = latest_articles[latest_articles["supplier_article"].isin(sel_articles)]["nm_id"]
    ord_df = ord_df[ord_df["nm_id"].isin(nm_ids)]
    sal_df = sal_df[sal_df["nm_id"].isin(nm_ids)] if not sal_df.empty else sal_df

ret_df = sal_df[sal_df["is_return"] == True] if not sal_df.empty else pd.DataFrame()
buy_df = sal_df[sal_df["is_return"] == False] if not sal_df.empty else pd.DataFrame()

# ── KPI карточки ─────────────────────────────────────────────────────────────
total_orders = len(ord_df[ord_df["is_actual"]]) if not ord_df.empty else 0
total_buyouts = len(buy_df)
total_returns = len(ret_df)
buyout_rate = (total_buyouts / total_orders * 100) if total_orders > 0 else 0

st.markdown("### Итого за месяц")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Заказов", f"{total_orders:,}".replace(",", " "))
c2.metric("Выкупов", f"{total_buyouts:,}".replace(",", " "))
c3.metric("Возвратов", f"{total_returns:,}".replace(",", " "))
c4.metric("% выкупа", f"{buyout_rate:.1f}%")

st.markdown("---")

# ── Динамика по дням ─────────────────────────────────────────────────────────
def daily_orders(df):
    if df.empty:
        return pd.DataFrame(columns=["day", "count"])
    actual = df[df["is_actual"]]
    g = actual.groupby("day").agg(count=("srid", "count")).reset_index()
    return g

def daily_sales(df, is_return: bool):
    if df.empty:
        return pd.DataFrame(columns=["day", "count"])
    sub = df[df["is_return"] == is_return]
    if sub.empty:
        return pd.DataFrame(columns=["day", "count"])
    g = sub.groupby("day").agg(count=("sale_id", "count")).reset_index()
    return g

d_orders = daily_orders(ord_df)
d_buyouts = daily_sales(sal_df, False)
d_returns = daily_sales(sal_df, True)

def daily_cancelled(df):
    if df.empty:
        return pd.DataFrame(columns=["day", "count"])
    cancelled = df[~df["is_actual"]]
    if cancelled.empty:
        return pd.DataFrame(columns=["day", "count"])
    return cancelled.groupby("day").agg(count=("srid", "count")).reset_index()

d_cancelled = daily_cancelled(ord_df)

st.markdown("#### Заказы, выкупы, отмены и возвраты по дням (шт.)")
if not d_orders.empty or not d_buyouts.empty:
    fig = go.Figure()
    if not d_orders.empty:
        fig.add_trace(go.Bar(x=d_orders["day"], y=d_orders["count"],
                             name="Заказы", marker_color="#7C3AED",
                             text=d_orders["count"], textposition="outside"))
    if not d_buyouts.empty:
        fig.add_trace(go.Bar(x=d_buyouts["day"], y=d_buyouts["count"],
                             name="Выкупы", marker_color="#10B981",
                             text=d_buyouts["count"], textposition="outside"))
    if not d_cancelled.empty:
        fig.add_trace(go.Bar(x=d_cancelled["day"], y=d_cancelled["count"],
                             name="Отмены", marker_color="#F59E0B",
                             text=d_cancelled["count"], textposition="outside"))
    if not d_returns.empty:
        fig.add_trace(go.Bar(x=d_returns["day"], y=d_returns["count"],
                             name="Возвраты", marker_color="#EF4444",
                             text=d_returns["count"], textposition="outside"))
    fig.update_layout(barmode="group", legend=dict(orientation="h"),
                      margin=dict(t=30, b=10), height=360,
                      uniformtext=dict(mode="hide", minsize=8))
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Нет данных")

st.markdown("#### Топ-10 товаров по заказам")
if not ord_df.empty:
    actual = ord_df[ord_df["is_actual"]]
    names = actual.groupby("nm_id").agg(
        supplier_article=("supplier_article", "last"),
        brand=("brand", "last"),
    ).reset_index()
    top = (
        actual.groupby("nm_id")
        .agg(count=("srid", "count"))
        .reset_index()
        .merge(names, on="nm_id", how="left")
        .sort_values("count", ascending=False)
        .head(10)
    )
    top["label"] = top["supplier_article"].fillna("") + " / " + top["brand"].fillna("")
    fig4 = px.bar(top, x="count", y="label", orientation="h",
                  color_discrete_sequence=["#7C3AED"])
    fig4.update_layout(yaxis=dict(autorange="reversed"),
                       margin=dict(t=10, b=10), height=320)
    st.plotly_chart(fig4, use_container_width=True)
else:
    st.info("Нет данных")
