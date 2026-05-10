import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import calendar
from datetime import date

from src.db.models import init_db, get_session_factory
from src.db.repository import OrderRepository, SaleRepository, OzonPostingRepository, OzonTransactionRepository

st.title("🔀 Сводный отчёт WB + Ozon")

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


@st.cache_data(ttl=300, show_spinner="Загружаю данные...")
def load_all(db_url, year, month):
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        wb_orders = OrderRepository().get_by_month(session, year, month, platform="wb")
        wb_sales  = SaleRepository().get_by_month(session, year, month, platform="wb")
        oz_posts  = OzonPostingRepository().get_by_month(session, year, month)
        oz_txs    = OzonTransactionRepository().get_by_month(session, year, month)
    return wb_orders, wb_sales, oz_posts, oz_txs

wb_orders, wb_sales, oz_posts, oz_txs = load_all(DB_URL, year, month)

# ── Агрегаты WB ───────────────────────────────────────────────────────────────
if not wb_orders.empty:
    wb_orders["order_date"] = pd.to_datetime(wb_orders["order_date"])
    wb_orders["day"] = wb_orders["order_date"].dt.day
    wb_actual = wb_orders[~wb_orders["is_cancel"].fillna(False)]
    wb_qty_orders = int(wb_actual["quantity"].sum() if "quantity" in wb_actual.columns
                        else len(wb_actual))
else:
    wb_qty_orders = 0
    wb_actual = pd.DataFrame()

if not wb_sales.empty:
    wb_sales["sale_date"] = pd.to_datetime(wb_sales["sale_date"])
    wb_sales["day"] = wb_sales["sale_date"].dt.day
    wb_sold    = wb_sales[~wb_sales["is_return"].fillna(False)]
    wb_returns = wb_sales[wb_sales["is_return"].fillna(False)]
    wb_qty_sold     = len(wb_sold)
    wb_qty_returned = len(wb_returns)
else:
    wb_qty_sold = wb_qty_returned = 0
    wb_sold = wb_returns = pd.DataFrame()

# ── Агрегаты Ozon ─────────────────────────────────────────────────────────────
if not oz_posts.empty:
    oz_posts["created_at"] = pd.to_datetime(oz_posts["created_at"])
    oz_posts["day"] = oz_posts["created_at"].dt.day
    oz_orders    = oz_posts[~oz_posts["is_cancelled"]]
    oz_qty_orders    = int(oz_orders["quantity"].sum())
    oz_qty_cancelled = int(oz_posts[oz_posts["is_cancelled"]]["quantity"].sum())
else:
    oz_qty_orders = oz_qty_cancelled = 0
    oz_orders = pd.DataFrame()

# Выкупы Ozon — транзакционная модель (дата фактической доставки)
if not oz_txs.empty:
    oz_txs["operation_date"] = pd.to_datetime(oz_txs["operation_date"])
    oz_txs["day"] = oz_txs["operation_date"].dt.day
    oz_sold_tx   = oz_txs[oz_txs["operation_type"] == "OperationAgentDeliveredToCustomer"]
    oz_qty_sold  = len(oz_sold_tx)
else:
    oz_sold_tx  = pd.DataFrame()
    oz_qty_sold = 0

total_orders = wb_qty_orders + oz_qty_orders
total_sold   = wb_qty_sold + oz_qty_sold

# ── Сводные KPI ───────────────────────────────────────────────────────────────
st.markdown("### Сводные показатели")

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("#### 📦 WB")
    st.metric("Заказов",   f"{wb_qty_orders:,}".replace(",", " "))
    st.metric("Выкупов",   f"{wb_qty_sold:,}".replace(",", " "))
    st.metric("Возвратов", f"{wb_qty_returned:,}".replace(",", " "))
    wb_pct = round(wb_qty_sold / wb_qty_orders * 100, 1) if wb_qty_orders else 0
    st.metric("% выкупа",  f"{wb_pct} %")

with col2:
    st.markdown("#### 🟦 Ozon")
    st.metric("Заказов",  f"{oz_qty_orders:,}".replace(",", " "))
    st.metric("Выкупов",  f"{oz_qty_sold:,}".replace(",", " "))
    st.metric("Отменено", f"{oz_qty_cancelled:,}".replace(",", " "))
    oz_pct = round(oz_qty_sold / oz_qty_orders * 100, 1) if oz_qty_orders else 0
    st.metric("% выкупа", f"{oz_pct} %")

with col3:
    st.markdown("#### 📊 Итого")
    st.metric("Заказов",  f"{total_orders:,}".replace(",", " "))
    st.metric("Выкупов",  f"{total_sold:,}".replace(",", " "))
    total_returns = wb_qty_returned + oz_qty_cancelled
    st.metric("Возвратов/Отмен", f"{total_returns:,}".replace(",", " "))
    total_pct = round(total_sold / total_orders * 100, 1) if total_orders else 0
    st.metric("% выкупа", f"{total_pct} %")

st.markdown("---")

# ── Доля заказов по платформам ────────────────────────────────────────────────
st.markdown("#### Доля заказов по платформам")
if total_orders > 0:
    fig_pie = go.Figure(go.Pie(
        labels=["Wildberries", "Ozon"],
        values=[wb_qty_orders, oz_qty_orders],
        marker_colors=["#7C3AED", "#F97316"],
        hole=0.45,
        textinfo="label+percent",
    ))
    fig_pie.update_layout(height=300, margin=dict(t=10, b=10), showlegend=False)
    st.plotly_chart(fig_pie, use_container_width=True)
else:
    st.info("Нет данных для построения диаграммы.")

st.markdown("---")

# ── Динамика по дням: WB vs Ozon ──────────────────────────────────────────────
st.markdown("#### Выкупы по дням (шт.)")
days_in_month = calendar.monthrange(year, month)[1]
all_days = list(range(1, days_in_month + 1))

wb_by_day = (
    wb_sold.groupby("day").size().reindex(all_days, fill_value=0)
    if not wb_sold.empty else pd.Series(0, index=all_days)
)
oz_by_day = (
    oz_sold_tx.groupby("day").size().reindex(all_days, fill_value=0)
    if not oz_sold_tx.empty else pd.Series(0, index=all_days)
)

fig_days = go.Figure()
fig_days.add_bar(x=all_days, y=wb_by_day.values, name="WB",   marker_color="#7C3AED")
fig_days.add_bar(x=all_days, y=oz_by_day.values, name="Ozon", marker_color="#F97316")
fig_days.update_layout(barmode="group", height=320,
                       xaxis_title="День", yaxis_title="Выкупов (шт.)",
                       margin=dict(t=10, b=10),
                       legend=dict(orientation="h", y=1.1))
st.plotly_chart(fig_days, use_container_width=True)

# ── Заказы по дням ────────────────────────────────────────────────────────────
st.markdown("#### Заказы по дням (шт.)")
wb_ord_by_day = (
    wb_actual.groupby("day").size().reindex(all_days, fill_value=0)
    if not wb_actual.empty else pd.Series(0, index=all_days)
)
oz_ord_by_day = (
    oz_orders.groupby("day")["quantity"].sum().reindex(all_days, fill_value=0)
    if not oz_orders.empty else pd.Series(0, index=all_days)
)

fig_ord = go.Figure()
fig_ord.add_scatter(x=all_days, y=wb_ord_by_day.values, mode="lines+markers",
                    name="WB", line=dict(color="#7C3AED", width=2))
fig_ord.add_scatter(x=all_days, y=oz_ord_by_day.values, mode="lines+markers",
                    name="Ozon", line=dict(color="#F97316", width=2))
fig_ord.update_layout(height=300, margin=dict(t=10, b=10),
                      xaxis_title="День", yaxis_title="Заказов (шт.)",
                      legend=dict(orientation="h", y=1.1))
st.plotly_chart(fig_ord, use_container_width=True)

st.markdown("---")

# ── Сводная таблица по платформам ─────────────────────────────────────────────
st.markdown("#### Сводная таблица")
summary = pd.DataFrame([
    {"Платформа": "Wildberries", "Заказов": wb_qty_orders, "Выкупов": wb_qty_sold,
     "Возвратов/Отмен": wb_qty_returned,
     "% выкупа": round(wb_qty_sold / wb_qty_orders * 100, 1) if wb_qty_orders else 0},
    {"Платформа": "Ozon", "Заказов": oz_qty_orders, "Выкупов": oz_qty_sold,
     "Возвратов/Отмен": oz_qty_cancelled,
     "% выкупа": round(oz_qty_sold / oz_qty_orders * 100, 1) if oz_qty_orders else 0},
    {"Платформа": "ИТОГО", "Заказов": total_orders, "Выкупов": total_sold,
     "Возвратов/Отмен": total_returns,
     "% выкупа": round(total_sold / total_orders * 100, 1) if total_orders else 0},
])
st.dataframe(
    summary, use_container_width=True, hide_index=True,
    column_config={
        "% выкупа": st.column_config.NumberColumn(format="%.1f %%"),
    },
)
