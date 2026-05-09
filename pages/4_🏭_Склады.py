import math
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text

from src.api.wb_client import WBClient, WBApiError, parse_stocks
from src.db.models import init_db, get_session_factory
from src.auth import require_login

st.set_page_config(page_title="Склады", page_icon="🏭", layout="wide")
require_login()
st.title("🏭 Остатки и продажи по складам FBO")

if "wildberries" not in st.secrets or "database" not in st.secrets:
    st.error("Не настроены токен WB или база данных. Проверьте раздел ⚙️ Настройки.")
    st.stop()

st.sidebar.markdown(f"👤 {st.session_state.get('username', '')}")
if st.sidebar.button("Выйти"):
    st.session_state.clear()
    st.rerun()

DAYS_ANALYSIS = 14   # период для расчёта среднедневных продаж
GREEN_DAYS    = 30   # порог «всё хорошо» (дней запаса)
YELLOW_DAYS   = 14   # порог «предупреждение»
RED_DAYS      = 7    # порог «критично»

# ── Загрузка остатков ─────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner="Загружаю остатки с WB...")
def load_stocks(token: str) -> pd.DataFrame:
    client = WBClient(token)
    raw = client.get_stocks(datetime.now() - timedelta(days=30))
    rows = parse_stocks(raw)
    return pd.DataFrame(rows) if rows else pd.DataFrame()

# ── Загрузка продаж за последние N дней ──────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner="Загружаю продажи...")
def load_sales(db_url: str, days: int) -> pd.DataFrame:
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    date_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    with Session() as session:
        rows = session.execute(text("""
            SELECT nm_id, warehouse_name, COUNT(*) as sales_count
            FROM sales
            WHERE sale_date >= :date_from
              AND sale_id NOT LIKE 'R%'
            GROUP BY nm_id, warehouse_name
        """), {"date_from": date_from}).fetchall()
    if not rows:
        return pd.DataFrame(columns=["nm_id", "warehouse_name", "sales_count"])
    return pd.DataFrame(rows, columns=["nm_id", "warehouse_name", "sales_count"])

token  = st.secrets["wildberries"]["api_token"]
db_url = st.secrets["database"]["url"]

try:
    stocks_df = load_stocks(token)
except WBApiError as e:
    st.error(str(e))
    st.stop()

sales_df = load_sales(db_url, DAYS_ANALYSIS)

if stocks_df.empty:
    st.warning("Нет данных об остатках.")
    st.stop()

# ── Актуальные артикулы по nm_id ──────────────────────────────────────────────
latest = (
    stocks_df.groupby("nm_id")
    .agg(supplier_article=("supplier_article", "last"), brand=("brand", "last"))
    .reset_index()
)
stocks_df = stocks_df.drop(columns=["supplier_article", "brand"]).merge(latest, on="nm_id", how="left")

# ── Фильтры ───────────────────────────────────────────────────────────────────
st.sidebar.markdown("### Фильтры")
sel_brands   = st.sidebar.multiselect("Бренд", sorted(stocks_df["brand"].dropna().unique()))
sel_articles = st.sidebar.multiselect("Артикул продавца", sorted(latest["supplier_article"].dropna().unique()))

if sel_brands:
    nm_ids_b = latest[latest["brand"].isin(sel_brands)]["nm_id"]
    stocks_df = stocks_df[stocks_df["nm_id"].isin(nm_ids_b)]
    sales_df  = sales_df[sales_df["nm_id"].isin(nm_ids_b)]
if sel_articles:
    nm_ids_a = latest[latest["supplier_article"].isin(sel_articles)]["nm_id"]
    stocks_df = stocks_df[stocks_df["nm_id"].isin(nm_ids_a)]
    sales_df  = sales_df[sales_df["nm_id"].isin(nm_ids_a)]

# ── KPI ───────────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("На складах", f"{stocks_df['quantity'].sum():,}".replace(",", " "))
c2.metric("В пути к клиенту", f"{stocks_df['in_way_to_client'].sum():,}".replace(",", " "))
c3.metric("В пути от клиента", f"{stocks_df['in_way_from_client'].sum():,}".replace(",", " "))
c4.metric(f"Продаж за {DAYS_ANALYSIS} дней", f"{int(sales_df['sales_count'].sum()):,}".replace(",", " "))

st.markdown("---")

# ── Сводная таблица остатков по складам ──────────────────────────────────────
st.markdown("### Остатки по артикулам и складам")
pivot = (
    stocks_df.groupby(["nm_id", "warehouse_name"])["quantity"]
    .sum().reset_index()
    .pivot(index="nm_id", columns="warehouse_name", values="quantity")
    .fillna(0).astype(int).reset_index()
)
warehouse_cols = [c for c in pivot.columns if c != "nm_id"]
pivot = pivot.merge(latest[["nm_id", "brand", "supplier_article"]], on="nm_id", how="left")
pivot["Итого"] = pivot[warehouse_cols].sum(axis=1)
pivot = pivot.sort_values("Итого", ascending=False)
display_cols = ["brand", "supplier_article", "nm_id"] + warehouse_cols + ["Итого"]
pivot = pivot[display_cols].rename(columns={"brand": "Бренд", "supplier_article": "Артикул", "nm_id": "nmId WB"})
st.dataframe(pivot, use_container_width=True, hide_index=True, height=400)

st.markdown("---")

# ── Анализ запасов: склад + артикул ──────────────────────────────────────────
st.markdown(f"### Анализ запасов (продажи за {DAYS_ANALYSIS} дн. · порог поставки: {GREEN_DAYS} дн.)")

stock_by = stocks_df.groupby(["nm_id", "warehouse_name"])["quantity"].sum().reset_index()
stock_by.columns = ["nm_id", "warehouse_name", "stock"]

sales_by = sales_df.rename(columns={"sales_count": "sales_total"})
sales_by["daily_avg"] = (sales_by["sales_total"] / DAYS_ANALYSIS).round(2)

merged = stock_by.merge(sales_by[["nm_id", "warehouse_name", "sales_total", "daily_avg"]],
                        on=["nm_id", "warehouse_name"], how="left").fillna(0)
merged = merged.merge(latest[["nm_id", "brand", "supplier_article"]], on="nm_id", how="left")

# Дней запаса и сколько нужно поставить
def days_of_supply(row):
    if row["daily_avg"] <= 0:
        return 999
    return round(row["stock"] / row["daily_avg"], 1)

def needed_qty(row):
    if row["daily_avg"] <= 0:
        return 0
    target = math.ceil(GREEN_DAYS * row["daily_avg"])
    need = target - int(row["stock"])
    return max(0, need)

def status(days):
    if days >= GREEN_DAYS:
        return "🟢 OK"
    if days >= YELLOW_DAYS:
        return "🟡 Предупреждение"
    if days >= RED_DAYS:
        return "🟠 Скоро закончится"
    return "🔴 Критично"

merged["days_supply"] = merged.apply(days_of_supply, axis=1)
merged["needed"]      = merged.apply(needed_qty, axis=1)
merged["status"]      = merged["days_supply"].apply(status)

# Убираем строки где нет продаж и нет остатка
merged = merged[(merged["stock"] > 0) | (merged["sales_total"] > 0)]
merged = merged.sort_values(["days_supply"], ascending=True)

result = merged[[
    "brand", "supplier_article", "nm_id", "warehouse_name",
    "stock", "daily_avg", "days_supply", "needed", "status"
]].rename(columns={
    "brand": "Бренд",
    "supplier_article": "Артикул",
    "nm_id": "nmId WB",
    "warehouse_name": "Склад",
    "stock": "Остаток",
    "daily_avg": "Продаж/день",
    "days_supply": "Запас (дней)",
    "needed": "Нужно поставить",
    "status": "Статус",
})

# Сначала показываем критичные
st.dataframe(
    result,
    use_container_width=True,
    hide_index=True,
    height=500,
    column_config={
        "Запас (дней)": st.column_config.NumberColumn(format="%.1f"),
        "Продаж/день": st.column_config.NumberColumn(format="%.1f"),
        "Нужно поставить": st.column_config.NumberColumn(
            help=f"Количество для достижения {GREEN_DAYS} дней запаса"
        ),
    },
)

# ── Сводка по критичным позициям ──────────────────────────────────────────────
critical = result[result["Статус"].str.startswith("🔴")]
warning  = result[(result["Статус"].str.startswith("🟠")) | (result["Статус"].str.startswith("🟡"))]

if not critical.empty:
    st.error(f"🔴 Критично (менее {RED_DAYS} дней): {len(critical)} позиций")
    st.dataframe(critical[["Артикул", "Склад", "Остаток", "Продаж/день", "Запас (дней)", "Нужно поставить"]],
                 use_container_width=True, hide_index=True)

if not warning.empty:
    st.warning(f"🟡 Предупреждение (менее {GREEN_DAYS} дней): {len(warning)} позиций")
    st.dataframe(warning[["Артикул", "Склад", "Остаток", "Продаж/день", "Запас (дней)", "Нужно поставить"]],
                 use_container_width=True, hide_index=True)
