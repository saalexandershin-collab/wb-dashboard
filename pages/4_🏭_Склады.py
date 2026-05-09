import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from src.api.wb_client import WBClient, WBApiError, parse_stocks
from src.auth import require_login

st.set_page_config(page_title="Склады", page_icon="🏭", layout="wide")
require_login()
st.title("🏭 Остатки на складах FBO")

if "wildberries" not in st.secrets:
    st.error("Не настроен токен WB. Проверьте раздел ⚙️ Настройки.")
    st.stop()

st.sidebar.markdown(f"👤 {st.session_state.get('username', '')}")
if st.sidebar.button("Выйти"):
    st.session_state.clear()
    st.rerun()

# ── Загрузка остатков ─────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner="Загружаю остатки с WB...")
def load_stocks(token: str) -> pd.DataFrame:
    client = WBClient(token)
    date_from = datetime.now() - timedelta(days=30)
    raw = client.get_stocks(date_from)
    rows = parse_stocks(raw)
    return pd.DataFrame(rows) if rows else pd.DataFrame()

token = st.secrets["wildberries"]["api_token"]

try:
    df = load_stocks(token)
except WBApiError as e:
    st.error(str(e))
    st.stop()

if df.empty:
    st.warning("Нет данных об остатках.")
    st.stop()

# ── Актуальные артикулы по nm_id ──────────────────────────────────────────────
latest = (
    df.groupby("nm_id")
    .agg(supplier_article=("supplier_article", "last"), brand=("brand", "last"))
    .reset_index()
)
df = df.drop(columns=["supplier_article", "brand"]).merge(latest, on="nm_id", how="left")

# ── Фильтры ───────────────────────────────────────────────────────────────────
st.sidebar.markdown("### Фильтры")
all_brands = sorted(df["brand"].dropna().unique())
sel_brands = st.sidebar.multiselect("Бренд", all_brands)

all_articles = sorted(latest["supplier_article"].dropna().unique())
sel_articles = st.sidebar.multiselect("Артикул продавца", all_articles)

if sel_brands:
    df = df[df["brand"].isin(sel_brands)]
if sel_articles:
    nm_ids = latest[latest["supplier_article"].isin(sel_articles)]["nm_id"]
    df = df[df["nm_id"].isin(nm_ids)]

# ── KPI ───────────────────────────────────────────────────────────────────────
total_stock = df["quantity"].sum()
total_transit_to = df["in_way_to_client"].sum()
total_transit_from = df["in_way_from_client"].sum()
total_full = df["quantity_full"].sum()

c1, c2, c3, c4 = st.columns(4)
c1.metric("На складах", f"{total_stock:,}".replace(",", " "))
c2.metric("В пути к клиенту", f"{total_transit_to:,}".replace(",", " "))
c3.metric("В пути от клиента", f"{total_transit_from:,}".replace(",", " "))
c4.metric("Всего (с учётом пути)", f"{total_full:,}".replace(",", " "))

st.markdown("---")

# ── Переключатель показателя ──────────────────────────────────────────────────
metric_choice = st.radio(
    "Показатель:",
    ["На складе", "В пути к клиенту", "В пути от клиента"],
    horizontal=True,
)
metric_col = {"На складе": "quantity", "В пути к клиенту": "in_way_to_client",
              "В пути от клиента": "in_way_from_client"}[metric_choice]

# ── Сводная таблица: артикулы × склады ───────────────────────────────────────
pivot = (
    df.groupby(["nm_id", "warehouse_name"])[metric_col]
    .sum()
    .reset_index()
    .pivot(index="nm_id", columns="warehouse_name", values=metric_col)
    .fillna(0)
    .astype(int)
)

# Добавляем инфо о товаре и итоги
pivot = pivot.reset_index().merge(latest[["nm_id", "brand", "supplier_article"]], on="nm_id", how="left")
warehouse_cols = [c for c in pivot.columns if c not in ("nm_id", "brand", "supplier_article")]
pivot["Итого"] = pivot[warehouse_cols].sum(axis=1)
pivot = pivot.sort_values("Итого", ascending=False)

display_cols = ["brand", "supplier_article", "nm_id"] + warehouse_cols + ["Итого"]
pivot = pivot[display_cols].rename(columns={
    "brand": "Бренд",
    "supplier_article": "Артикул",
    "nm_id": "nmId WB",
})

st.markdown(f"### {metric_choice} — по артикулам и складам")
st.dataframe(pivot, use_container_width=True, hide_index=True, height=500)

# ── Итого по складам ──────────────────────────────────────────────────────────
st.markdown("### Итого по складам")
by_warehouse = (
    df.groupby("warehouse_name")[metric_col]
    .sum()
    .reset_index()
    .rename(columns={"warehouse_name": "Склад", metric_col: "Количество"})
    .sort_values("Количество", ascending=False)
)
st.dataframe(by_warehouse, use_container_width=True, hide_index=True)
