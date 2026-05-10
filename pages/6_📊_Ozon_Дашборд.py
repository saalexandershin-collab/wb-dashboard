import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import calendar
from datetime import date

from src.db.models import init_db, get_session_factory
from src.db.repository import OzonPostingRepository

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

@st.cache_data(ttl=300, show_spinner="Загружаю данные Ozon...")
def load(db_url, year, month):
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        return OzonPostingRepository().get_by_month(session, year, month)

df = load(DB_URL, year, month)

if df.empty:
    st.warning("Нет данных Ozon за этот период. Загрузите командой:")
    st.code(
        f"OZON_CLIENT_ID='...' OZON_API_KEY='...' DATABASE_URL='...' "
        f"SYNC_YEAR={year} SYNC_MONTH={month} "
        "python3 scripts/sync_ozon_orders.py"
    )
    st.stop()

df["created_at"] = pd.to_datetime(df["created_at"])
df["day"] = df["created_at"].dt.day

mask_orders    = ~df["is_cancelled"]
mask_sold      = ~df["is_cancelled"] & (df["status"] == "delivered")
mask_cancelled = df["is_cancelled"]

qty_orders    = int(df[mask_orders]["quantity"].sum())
qty_sold      = int(df[mask_sold]["quantity"].sum())
qty_cancelled = int(df[mask_cancelled]["quantity"].sum())
redemption    = round(qty_sold / qty_orders * 100, 1) if qty_orders else 0.0

# ── KPI ───────────────────────────────────────────────────────────────────────
st.markdown("### Итого за месяц")
k1, k2, k3, k4 = st.columns(4)
k1.metric("Заказов",  f"{qty_orders:,}".replace(",", " "))
k2.metric("Выкупов",  f"{qty_sold:,}".replace(",", " "))
k3.metric("Отменено", f"{qty_cancelled:,}".replace(",", " "))
k4.metric("% выкупа", f"{redemption} %")
st.markdown("---")

# ── График по дням ────────────────────────────────────────────────────────────
days_in_month = calendar.monthrange(year, month)[1]
all_days = list(range(1, days_in_month + 1))

orders_by_day = df[mask_orders].groupby("day")["quantity"].sum().reindex(all_days, fill_value=0)
sold_by_day   = df[mask_sold].groupby("day")["quantity"].sum().reindex(all_days, fill_value=0)
cancel_by_day = df[mask_cancelled].groupby("day")["quantity"].sum().reindex(all_days, fill_value=0)

fig = go.Figure()
fig.add_bar(x=all_days, y=orders_by_day.values, name="Заказы",  marker_color="#F97316")
fig.add_bar(x=all_days, y=sold_by_day.values,   name="Выкупы",  marker_color="#10B981")
fig.add_bar(x=all_days, y=cancel_by_day.values, name="Отмены",  marker_color="#EF4444")
fig.update_layout(barmode="group", height=340,
                  xaxis_title="День", yaxis_title="Количество (шт.)",
                  margin=dict(t=10, b=10), legend=dict(orientation="h"))
st.markdown("#### Заказы, выкупы и отмены по дням (шт.)")
st.plotly_chart(fig, use_container_width=True)

# ── Топ товаров ───────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("#### Топ товаров по выкупам")
top = (
    df[mask_sold]
    .groupby(["offer_id", "product_name"])
    .agg(qty=("quantity", "sum"))
    .reset_index()
    .sort_values("qty", ascending=False)
    .head(10)
)
if not top.empty:
    top["label"] = top["offer_id"] + " / " + top["product_name"].str[:35]
    fig3 = px.bar(top, x="qty", y="label", orientation="h",
                  color_discrete_sequence=["#F97316"],
                  labels={"qty": "Выкупов (шт.)", "label": ""})
    fig3.update_layout(yaxis=dict(autorange="reversed"), height=max(280, len(top) * 36),
                       margin=dict(t=10, b=10))
    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("Нет выкупов за период.")

# ── По складам ────────────────────────────────────────────────────────────────
if "warehouse_name" in df.columns:
    st.markdown("---")
    st.markdown("#### Выкупы по складам")
    wh = df[mask_sold].groupby("warehouse_name")["quantity"].sum().reset_index()
    wh = wh[wh["warehouse_name"].notna() & (wh["warehouse_name"] != "")]
    if not wh.empty:
        fig4 = px.pie(wh, values="quantity", names="warehouse_name",
                      color_discrete_sequence=px.colors.qualitative.Set2)
        fig4.update_layout(height=300, margin=dict(t=10, b=10))
        st.plotly_chart(fig4, use_container_width=True)
