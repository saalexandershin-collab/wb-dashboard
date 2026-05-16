import math
import streamlit as st
import pandas as pd
import calendar
from datetime import datetime, timedelta
from sqlalchemy import text

from src.db.models import init_db, get_session_factory
from src.data_loader import load_wb_stocks
st.title("🏭 Остатки и продажи по складам FBO")

if "database" not in st.secrets:
    st.error("Не настроена база данных. Проверьте раздел ⚙️ Настройки.")
    st.stop()

DB_URL = st.secrets["database"]["url"]

DAYS_ANALYSIS = 14
GREEN_DAYS    = 30
YELLOW_DAYS   = 14
RED_DAYS      = 7

# ── Загрузка данных из базы (без WB API) ─────────────────────────────────────
# Остатки — из общего кэша (разделяется между всеми вкладками)
stocks_df, synced_at = load_wb_stocks(DB_URL)

# Продажи за последние N дней — отдельный запрос с raw SQL, кэшируется локально
@st.cache_data(ttl=300, show_spinner="Загружаю продажи по складам...")
def load_recent_sales(db_url: str, days: int) -> pd.DataFrame:
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        date_from = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = session.execute(text("""
            SELECT nm_id, warehouse_name, COUNT(*) as sales_count
            FROM sales
            WHERE sale_date >= :date_from
              AND sale_id NOT LIKE 'R%'
            GROUP BY nm_id, warehouse_name
        """), {"date_from": date_from}).fetchall()
    return (
        pd.DataFrame(rows, columns=["nm_id", "warehouse_name", "sales_count"])
        if rows else
        pd.DataFrame(columns=["nm_id", "warehouse_name", "sales_count"])
    )

sales_df = load_recent_sales(DB_URL, DAYS_ANALYSIS)

if stocks_df.empty:
    st.warning(
        "Нет данных об остатках в базе. "
        "Запустите синхронизацию остатков локально:\n\n"
        "```\nWB_API_TOKEN='...' DATABASE_URL='sqlite:///wb_local.db' "
        "python3 scripts/sync_stocks.py\n```\n\n"
        "Затем перенесите в Supabase:\n\n"
        "```\npython3 scripts/migrate_to_pg.py\n```"
    )
    st.stop()

# Показываем когда последний раз обновлялись остатки
if synced_at:
    age = datetime.utcnow() - synced_at
    age_str = f"{int(age.total_seconds() // 3600)} ч назад" if age.total_seconds() > 3600 else f"{int(age.total_seconds() // 60)} мин назад"
    st.caption(f"📦 Остатки обновлены: {age_str} · Для обновления запустите `sync_stocks.py`")

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
c1.metric("На складах",          f"{stocks_df['quantity'].sum():,}".replace(",", " "))
c2.metric("В пути к клиенту",    f"{stocks_df['in_way_to_client'].sum():,}".replace(",", " "))
c3.metric("В пути от клиента",   f"{stocks_df['in_way_from_client'].sum():,}".replace(",", " "))
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

# ── Анализ запасов ────────────────────────────────────────────────────────────
st.markdown(f"### Анализ запасов (продажи за {DAYS_ANALYSIS} дн. · порог поставки: {GREEN_DAYS} дн.)")

stock_by = stocks_df.groupby(["nm_id", "warehouse_name"])["quantity"].sum().reset_index()
stock_by.columns = ["nm_id", "warehouse_name", "stock"]

sales_by = sales_df.rename(columns={"sales_count": "sales_total"})
sales_by["daily_avg"] = (sales_by["sales_total"] / DAYS_ANALYSIS).round(2)

merged = stock_by.merge(sales_by[["nm_id", "warehouse_name", "sales_total", "daily_avg"]],
                        on=["nm_id", "warehouse_name"], how="left").fillna(0)
merged = merged.merge(latest[["nm_id", "brand", "supplier_article"]], on="nm_id", how="left")

def days_of_supply(row):
    if row["daily_avg"] <= 0:
        return 999
    return round(row["stock"] / row["daily_avg"], 1)

def needed_qty(row):
    if row["daily_avg"] <= 0:
        return 0
    target = math.ceil(GREEN_DAYS * row["daily_avg"])
    return max(0, target - int(row["stock"]))

def status(days):
    if days >= GREEN_DAYS:   return "🟢 OK"
    if days >= YELLOW_DAYS:  return "🟡 Предупреждение"
    if days >= RED_DAYS:     return "🟠 Скоро закончится"
    return "🔴 Критично"

merged["days_supply"] = merged.apply(days_of_supply, axis=1)
merged["needed"]      = merged.apply(needed_qty, axis=1)
merged["status"]      = merged["days_supply"].apply(status)
merged = merged[(merged["stock"] > 0) | (merged["sales_total"] > 0)]
merged = merged.sort_values("days_supply", ascending=True)

result = merged[[
    "brand", "supplier_article", "nm_id", "warehouse_name",
    "stock", "daily_avg", "days_supply", "needed", "status"
]].rename(columns={
    "brand": "Бренд", "supplier_article": "Артикул", "nm_id": "nmId WB",
    "warehouse_name": "Склад", "stock": "Остаток", "daily_avg": "Продаж/день",
    "days_supply": "Запас (дней)", "needed": "Нужно поставить", "status": "Статус",
})

st.dataframe(result, use_container_width=True, hide_index=True, height=500,
    column_config={
        "Запас (дней)":    st.column_config.NumberColumn(format="%.1f"),
        "Продаж/день":     st.column_config.NumberColumn(format="%.2f"),
        "Нужно поставить": st.column_config.NumberColumn(help=f"Для достижения {GREEN_DAYS} дней запаса"),
    },
)

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
