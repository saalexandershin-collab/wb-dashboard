import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import calendar
from datetime import date

from src.data_loader import load_ozon_postings, load_ozon_transactions

st.title("📊 Дашборд продаж Ozon")

if "database" not in st.secrets:
    st.error("Настройте базу данных в разделе ⚙️ Настройки.")
    st.stop()

DB_URL = st.secrets["database"]["url"]

today = date.today()
col_y, col_m = st.sidebar.columns(2)
year  = col_y.selectbox("Год",   list(range(today.year, today.year - 3, -1)), index=0)
month = col_m.selectbox("Месяц", list(range(1, 13)), index=today.month - 1,
                        format_func=lambda m: calendar.month_name[m])
st.sidebar.markdown("---")
st.sidebar.caption(f"Период: {calendar.month_name[month]} {year}")

posts = load_ozon_postings(DB_URL, year, month)
txs   = load_ozon_transactions(DB_URL, year, month)

if posts.empty and txs.empty:
    st.warning("Нет данных Ozon за этот период. Загрузите командой:")
    st.code(
        f"OZON_CLIENT_ID='...' OZON_API_KEY='...' DATABASE_URL='...' "
        f"SYNC_YEAR={year} SYNC_MONTH={month} python3 scripts/sync_ozon_orders.py"
    )
    st.stop()

days_in_month = calendar.monthrange(year, month)[1]
all_days = list(range(1, days_in_month + 1))

# ── Заказы и отмены — из постингов (дата создания заказа) ────────────────────
if not posts.empty:
    posts["created_at"] = pd.to_datetime(posts["created_at"])
    posts["day"] = posts["created_at"].dt.day
    mask_orders    = ~posts["is_cancelled"]
    mask_cancelled = posts["is_cancelled"]
    qty_orders    = int(posts[mask_orders]["quantity"].sum())
    qty_cancelled = int(posts[mask_cancelled]["quantity"].sum())
    orders_by_day = posts[mask_orders].groupby("day")["quantity"].sum().reindex(all_days, fill_value=0)
    cancel_by_day = posts[mask_cancelled].groupby("day")["quantity"].sum().reindex(all_days, fill_value=0)
else:
    qty_orders = qty_cancelled = 0
    orders_by_day = pd.Series(0, index=all_days)
    cancel_by_day = pd.Series(0, index=all_days)

# ── Выкупы — из транзакций (дата фактической доставки покупателю) ─────────────
if not txs.empty:
    txs["operation_date"] = pd.to_datetime(txs["operation_date"])
    txs["day"] = txs["operation_date"].dt.day
    sold_tx = txs[txs["operation_type"] == "OperationAgentDeliveredToCustomer"]
    # Каждая строка = 1 единица товара (quantity в транзакциях = 0, считаем строки)
    qty_sold    = len(sold_tx)
    sold_by_day = sold_tx.groupby("day").size().reindex(all_days, fill_value=0)
else:
    qty_sold    = 0
    sold_tx     = pd.DataFrame()
    sold_by_day = pd.Series(0, index=all_days)

redemption = round(qty_sold / qty_orders * 100, 1) if qty_orders else 0.0

# ── KPI ───────────────────────────────────────────────────────────────────────
st.markdown("### Итого за месяц")
k1, k2, k3, k4 = st.columns(4)
k1.metric("Заказов",  f"{qty_orders:,}".replace(",", " "))
k2.metric("Выкупов",  f"{qty_sold:,}".replace(",", " "))
k3.metric("Отменено", f"{qty_cancelled:,}".replace(",", " "))
k4.metric("% выкупа", f"{redemption} %")

st.caption("Выкупы — по дате фактической доставки покупателю (транзакционная модель Ozon)")
st.markdown("---")

# ── График по дням ────────────────────────────────────────────────────────────
fig = go.Figure()
fig.add_bar(x=all_days, y=orders_by_day.values, name="Заказы",  marker_color="#F97316")
fig.add_bar(x=all_days, y=sold_by_day.values,   name="Выкупы",  marker_color="#10B981")
fig.add_bar(x=all_days, y=cancel_by_day.values, name="Отмены",  marker_color="#EF4444")
fig.update_layout(barmode="group", height=340,
                  xaxis_title="День", yaxis_title="Количество (шт.)",
                  margin=dict(t=10, b=10), legend=dict(orientation="h"))
st.markdown("#### Заказы, выкупы и отмены по дням (шт.)")
st.plotly_chart(fig, use_container_width=True)

# ── Топ товаров по выкупам ────────────────────────────────────────────────────
st.markdown("---")
st.markdown("#### Топ товаров по выкупам")
if not sold_tx.empty:
    top = (
        sold_tx.groupby(["offer_id", "product_name"])
        .size()
        .reset_index(name="qty")
        .sort_values("qty", ascending=False)
        .head(10)
    )
    top["label"] = top["offer_id"] + " / " + top["product_name"].str[:35]
    fig3 = px.bar(top, x="qty", y="label", orientation="h",
                  color_discrete_sequence=["#F97316"],
                  labels={"qty": "Выкупов (шт.)", "label": ""})
    fig3.update_layout(yaxis=dict(autorange="reversed"),
                       height=max(280, len(top) * 36),
                       margin=dict(t=10, b=10))
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("Нет данных о выкупах за период.")

# ── По складам (из постингов) ─────────────────────────────────────────────────
if not posts.empty and "warehouse_name" in posts.columns:
    st.markdown("---")
    st.markdown("#### Выкупы по складам")
    delivered_posts = posts[(~posts["is_cancelled"]) & (posts["status"] == "delivered")]
    wh = delivered_posts.groupby("warehouse_name")["quantity"].sum().reset_index()
    wh = wh[wh["warehouse_name"].notna() & (wh["warehouse_name"] != "")]
    if not wh.empty:
        fig4 = px.pie(wh, values="quantity", names="warehouse_name",
                      color_discrete_sequence=px.colors.qualitative.Set2)
        fig4.update_layout(height=300, margin=dict(t=10, b=10))
        st.plotly_chart(fig4, use_container_width=True)
