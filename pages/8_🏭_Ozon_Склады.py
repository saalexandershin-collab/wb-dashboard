import math
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text

from src.db.models import init_db, get_session_factory
from src.data_loader import load_ozon_stocks

st.title("🏭 Остатки и продажи Ozon FBO")

if "database" not in st.secrets:
    st.error("Не настроена база данных.")
    st.stop()

DB_URL = st.secrets["database"]["url"]

DAYS_ANALYSIS = 14
GREEN_DAYS    = 30
YELLOW_DAYS   = 14
RED_DAYS      = 7

@st.cache_data(ttl=300, show_spinner="Загружаю продажи Ozon...")
def load_ozon_recent_sales(db_url: str, days: int) -> pd.DataFrame:
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        date_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = session.execute(text("""
            SELECT sku, warehouse_name, SUM(quantity) as sales_count
            FROM ozon_postings
            WHERE created_at >= :date_from
              AND status = 'delivered'
              AND is_cancelled = false
            GROUP BY sku, warehouse_name
        """), {"date_from": date_from}).fetchall()
    return (
        pd.DataFrame(rows, columns=["sku", "warehouse_name", "sales_count"])
        if rows else
        pd.DataFrame(columns=["sku", "warehouse_name", "sales_count"])
    )

stocks_df, synced_at = load_ozon_stocks(DB_URL)
sales_df = load_ozon_recent_sales(DB_URL, DAYS_ANALYSIS)

if stocks_df.empty:
    st.warning(
        "Нет данных об остатках. Запустите синхронизацию:\n\n"
        "```\nOZON_CLIENT_ID='...' OZON_API_KEY='...' DATABASE_URL='...' "
        "python3 scripts/sync_ozon_stocks.py\n```"
    )
    st.stop()

if synced_at:
    age = datetime.utcnow() - synced_at
    age_str = (f"{int(age.total_seconds() // 3600)} ч назад"
               if age.total_seconds() > 3600
               else f"{int(age.total_seconds() // 60)} мин назад")
    st.caption(f"📦 Остатки обновлены: {age_str} · Для обновления запустите `sync_ozon_stocks.py`")

# ── Фильтры ───────────────────────────────────────────────────────────────────
st.sidebar.markdown("### Фильтры")
sel_offers = st.sidebar.multiselect("Артикул (offer_id)",
                                    sorted(stocks_df["offer_id"].dropna().unique()))
if sel_offers:
    skus = stocks_df[stocks_df["offer_id"].isin(sel_offers)]["sku"].unique()
    stocks_df = stocks_df[stocks_df["sku"].isin(skus)]
    sales_df  = sales_df[sales_df["sku"].isin(skus)]

# ── KPI ───────────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("На складах FBO",      f"{stocks_df['free_to_sell_amount'].sum():,}".replace(",", " "))
c2.metric("Зарезервировано",     f"{stocks_df['reserved_amount'].sum():,}".replace(",", " "))
c3.metric("Обещано (в пути)",    f"{stocks_df['promised_amount'].sum():,}".replace(",", " "))
c4.metric(f"Продаж за {DAYS_ANALYSIS} дней", f"{int(sales_df['sales_count'].sum()):,}".replace(",", " "))

st.markdown("---")

# ── Сводная таблица ───────────────────────────────────────────────────────────
st.markdown("### Остатки по артикулам и складам")
pivot = (
    stocks_df.groupby(["sku", "warehouse_name"])["free_to_sell_amount"]
    .sum().reset_index()
    .pivot(index="sku", columns="warehouse_name", values="free_to_sell_amount")
    .fillna(0).astype(int).reset_index()
)
wh_cols = [c for c in pivot.columns if c != "sku"]
offer_map = stocks_df.drop_duplicates("sku").set_index("sku")[["offer_id", "product_name"]]
pivot = pivot.join(offer_map, on="sku")
pivot["Итого"] = pivot[wh_cols].sum(axis=1)
pivot = pivot.sort_values("Итого", ascending=False)
display = ["offer_id", "product_name", "sku"] + wh_cols + ["Итого"]
pivot = pivot[display].rename(columns={
    "offer_id": "Артикул", "product_name": "Название", "sku": "SKU Ozon"
})
st.dataframe(pivot, use_container_width=True, hide_index=True, height=400)

st.markdown("---")

# ── Анализ запасов ────────────────────────────────────────────────────────────
st.markdown(f"### Анализ запасов (продажи за {DAYS_ANALYSIS} дн.)")

stock_by = stocks_df.groupby(["sku", "warehouse_name"])["free_to_sell_amount"].sum().reset_index()
stock_by.columns = ["sku", "warehouse_name", "stock"]

sales_by = sales_df.rename(columns={"sales_count": "sales_total"})
sales_by["daily_avg"] = (sales_by["sales_total"] / DAYS_ANALYSIS).round(2)

merged = stock_by.merge(sales_by[["sku", "warehouse_name", "sales_total", "daily_avg"]],
                        on=["sku", "warehouse_name"], how="left").fillna(0)
merged = merged.join(offer_map, on="sku")

def days_of_supply(row):
    return 999 if row["daily_avg"] <= 0 else round(row["stock"] / row["daily_avg"], 1)

def needed_qty(row):
    if row["daily_avg"] <= 0:
        return 0
    return max(0, math.ceil(GREEN_DAYS * row["daily_avg"]) - int(row["stock"]))

def status(days):
    if days >= GREEN_DAYS:  return "🟢 OK"
    if days >= YELLOW_DAYS: return "🟡 Предупреждение"
    if days >= RED_DAYS:    return "🟠 Скоро закончится"
    return "🔴 Критично"

merged["days_supply"] = merged.apply(days_of_supply, axis=1)
merged["needed"]      = merged.apply(needed_qty, axis=1)
merged["status"]      = merged["days_supply"].apply(status)
merged = merged[(merged["stock"] > 0) | (merged["sales_total"] > 0)]
merged = merged.sort_values("days_supply")

result = merged[["offer_id", "product_name", "sku", "warehouse_name",
                 "stock", "daily_avg", "days_supply", "needed", "status"]].rename(columns={
    "offer_id": "Артикул", "product_name": "Название", "sku": "SKU Ozon",
    "warehouse_name": "Склад", "stock": "Остаток", "daily_avg": "Продаж/день",
    "days_supply": "Запас (дней)", "needed": "Нужно поставить", "status": "Статус",
})

st.dataframe(result, use_container_width=True, hide_index=True, height=500,
    column_config={
        "Запас (дней)":    st.column_config.NumberColumn(format="%.1f"),
        "Продаж/день":     st.column_config.NumberColumn(format="%.2f"),
        "Нужно поставить": st.column_config.NumberColumn(
            help=f"Для достижения {GREEN_DAYS} дней запаса"),
    })

critical = result[result["Статус"].str.startswith("🔴")]
warning  = result[result["Статус"].str.startswith("🟠") | result["Статус"].str.startswith("🟡")]

if not critical.empty:
    st.error(f"🔴 Критично (менее {RED_DAYS} дней): {len(critical)} позиций")
    st.dataframe(critical[["Артикул", "Склад", "Остаток", "Продаж/день", "Запас (дней)", "Нужно поставить"]],
                 use_container_width=True, hide_index=True)
if not warning.empty:
    st.warning(f"🟡 Предупреждение (менее {GREEN_DAYS} дней): {len(warning)} позиций")
    st.dataframe(warning[["Артикул", "Склад", "Остаток", "Продаж/день", "Запас (дней)", "Нужно поставить"]],
                 use_container_width=True, hide_index=True)
