import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import calendar
from datetime import date

from src.db.models import init_db, get_session_factory
from src.db.repository import (
    OrderRepository, SaleRepository,
    OzonPostingRepository, OzonTransactionRepository,
)

st.title("🔀 Сводный отчёт WB + Ozon")

if "database" not in st.secrets:
    st.error("Настройте базу данных в разделе ⚙️ Настройки.")
    st.stop()

DB_URL = st.secrets["database"]["url"]

today = date.today()
col_y, col_m = st.sidebar.columns(2)
year  = col_y.selectbox("Год",   list(range(today.year, today.year - 3, -1)), index=0)
month = col_m.selectbox(
    "Месяц", list(range(1, 13)), index=today.month - 1,
    format_func=lambda m: calendar.month_name[m],
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 🎯 План продаж")
monthly_plan = st.sidebar.number_input(
    "Выкупов в месяц (WB + Ozon)", min_value=0, value=5500, step=100,
)
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
    wb_qty_orders = len(wb_actual)
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
    oz_orders        = oz_posts[~oz_posts["is_cancelled"]]
    oz_qty_orders    = int(oz_orders["quantity"].sum())
    oz_qty_cancelled = int(oz_posts[oz_posts["is_cancelled"]]["quantity"].sum())
else:
    oz_qty_orders = oz_qty_cancelled = 0
    oz_orders = pd.DataFrame()

if not oz_txs.empty:
    oz_txs["operation_date"] = pd.to_datetime(oz_txs["operation_date"])
    oz_txs["day"] = oz_txs["operation_date"].dt.day
    oz_sold_tx  = oz_txs[oz_txs["operation_type"] == "OperationAgentDeliveredToCustomer"]
    oz_qty_sold = len(oz_sold_tx)
else:
    oz_sold_tx  = pd.DataFrame()
    oz_qty_sold = 0

total_orders  = wb_qty_orders + oz_qty_orders
total_sold    = wb_qty_sold + oz_qty_sold
total_returns = wb_qty_returned + oz_qty_cancelled

# ── Расчёт прогресса плана ───────────────────────────────────────────────────
days_in_month = calendar.monthrange(year, month)[1]

last_day_wb  = int(wb_sold["day"].max())    if not wb_sold.empty    else 0
last_day_oz  = int(oz_sold_tx["day"].max()) if not oz_sold_tx.empty else 0
days_elapsed = max(last_day_wb, last_day_oz, 1)

if monthly_plan > 0 and total_sold > 0:
    daily_rate   = total_sold / days_elapsed
    forecast     = round(daily_rate * days_in_month)
    plan_pct     = total_sold / monthly_plan * 100
    forecast_pct = forecast   / monthly_plan * 100
else:
    daily_rate = forecast = plan_pct = forecast_pct = 0


def fmt(n: int) -> str:
    """Форматирует число с пробелами как разделителями тысяч."""
    return f"{n:,}".replace(",", " ")


# ── Блок прогресса плана ─────────────────────────────────────────────────────
st.markdown("### 🎯 Выполнение плана")

p1, p2, p3, p4 = st.columns(4)

with p1:
    st.metric(
        "Выкупов (факт)", fmt(total_sold),
        help="WB выкупы + Ozon доставлено покупателю",
    )

with p2:
    st.metric("План", fmt(monthly_plan), help="Установлен в боковой панели")

with p3:
    color_pct = "#10B981" if plan_pct >= 100 else ("#F59E0B" if plan_pct >= 70 else "#EF4444")
    st.markdown(
        "<div style='line-height:1.35'>"
        "<p style='font-size:0.875rem;color:#6B7280;margin:0'>% выполнения</p>"
        f"<p style='font-size:2rem;font-weight:700;color:{color_pct};margin:0'>{plan_pct:.1f}%</p>"
        f"<p style='font-size:0.8rem;color:#6B7280;margin:0'>день {days_elapsed} из {days_in_month}</p>"
        "</div>",
        unsafe_allow_html=True,
    )

with p4:
    color_fc   = "#10B981" if forecast_pct >= 100 else "#EF4444"
    delta_sign = "+" if forecast_pct >= 100 else ""
    delta_val  = forecast_pct - 100
    st.markdown(
        "<div style='line-height:1.35'>"
        "<p style='font-size:0.875rem;color:#6B7280;margin:0'>Прогноз к концу месяца</p>"
        f"<p style='font-size:2rem;font-weight:700;color:{color_fc};margin:0'>{fmt(forecast)} шт.</p>"
        f"<p style='font-size:0.8rem;color:{color_fc};margin:0'>{delta_sign}{delta_val:.1f}% от плана</p>"
        "</div>",
        unsafe_allow_html=True,
    )

# Прогресс-бар
if monthly_plan > 0:
    bar_pct   = min(plan_pct, 100)
    fc_pct    = min(forecast_pct, 100)
    bar_color = "#10B981" if plan_pct >= 100 else ("#F59E0B" if plan_pct >= 70 else "#EF4444")
    fc_color  = "#10B981" if forecast_pct >= 100 else "#EF4444"
    st.markdown(
        f"<div style='margin-top:14px'>"
        f"<div style='display:flex;justify-content:space-between;font-size:0.8rem;color:#6B7280;margin-bottom:5px'>"
        f"<span>Факт: {fmt(total_sold)} шт.</span>"
        f"<span>Прогноз: {fmt(forecast)} шт.</span>"
        f"<span>План: {fmt(monthly_plan)} шт.</span>"
        f"</div>"
        f"<div style='background:#E5E7EB;border-radius:8px;height:14px;position:relative;overflow:hidden'>"
        f"<div style='position:absolute;left:0;top:0;height:100%;width:{fc_pct:.1f}%;"
        f"background:{fc_color};opacity:0.25;border-radius:8px'></div>"
        f"<div style='position:absolute;left:0;top:0;height:100%;width:{bar_pct:.1f}%;"
        f"background:{bar_color};border-radius:8px'></div>"
        f"</div></div>",
        unsafe_allow_html=True,
    )

st.markdown("---")

# ── KPI по платформам ─────────────────────────────────────────────────────────
st.markdown("### Показатели по платформам")

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("#### 📦 WB")
    st.metric("Заказов",       fmt(wb_qty_orders))
    st.metric("Выкупов",       fmt(wb_qty_sold))
    st.metric("Возвратов",     fmt(wb_qty_returned))
    wb_plan_pct = wb_qty_sold / monthly_plan * 100 if monthly_plan else 0
    st.metric("Вклад в план",  f"{wb_plan_pct:.1f}%",
              help=f"Доля WB выкупов от общего плана {fmt(monthly_plan)} шт.")

with col2:
    st.markdown("#### 🟦 Ozon")
    st.metric("Заказов",       fmt(oz_qty_orders))
    st.metric("Выкупов",       fmt(oz_qty_sold))
    st.metric("Отменено",      fmt(oz_qty_cancelled))
    oz_plan_pct = oz_qty_sold / monthly_plan * 100 if monthly_plan else 0
    st.metric("Вклад в план",  f"{oz_plan_pct:.1f}%",
              help=f"Доля Ozon выкупов от общего плана {fmt(monthly_plan)} шт.")

with col3:
    st.markdown("#### 📊 Итого")
    st.metric("Заказов",           fmt(total_orders))
    st.metric("Выкупов",           fmt(total_sold))
    st.metric("Возвратов/Отмен",   fmt(total_returns))
    st.metric("Выполнение плана",  f"{plan_pct:.1f}%",
              help=f"Всего выкупов / план {fmt(monthly_plan)} шт.")

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

# ── Выкупы по дням: WB + Ozon ────────────────────────────────────────────────
st.markdown("#### Выкупы по дням (шт.)")
all_days = list(range(1, days_in_month + 1))

wb_by_day = (
    wb_sold.groupby("day").size().reindex(all_days, fill_value=0)
    if not wb_sold.empty else pd.Series(0, index=all_days)
)
oz_by_day = (
    oz_sold_tx.groupby("day").size().reindex(all_days, fill_value=0)
    if not oz_sold_tx.empty else pd.Series(0, index=all_days)
)

daily_plan_line = monthly_plan / days_in_month if monthly_plan else None

fig_days = go.Figure()
fig_days.add_bar(x=all_days, y=wb_by_day.values, name="WB",   marker_color="#7C3AED")
fig_days.add_bar(x=all_days, y=oz_by_day.values, name="Ozon", marker_color="#F97316")
if daily_plan_line:
    fig_days.add_scatter(
        x=all_days, y=[daily_plan_line] * len(all_days),
        mode="lines",
        name=f"Дневной план ({daily_plan_line:.0f} шт.)",
        line=dict(color="#10B981", width=2, dash="dot"),
    )
fig_days.update_layout(
    barmode="stack", height=320,
    xaxis_title="День", yaxis_title="Выкупов (шт.)",
    margin=dict(t=10, b=10),
    legend=dict(orientation="h", y=1.1),
)
st.plotly_chart(fig_days, use_container_width=True)

# ── Накопительный факт vs план ────────────────────────────────────────────────
st.markdown("#### Накопительные выкупы vs план (шт.)")
combined_by_day = (wb_by_day + oz_by_day).reindex(all_days, fill_value=0)
cumulative = combined_by_day.cumsum()
plan_curve = [monthly_plan / days_in_month * d for d in all_days] if monthly_plan else [0] * len(all_days)

fig_cum = go.Figure()
fig_cum.add_scatter(
    x=all_days, y=cumulative.values,
    mode="lines+markers", name="Факт (накопительно)",
    line=dict(color="#7C3AED", width=2),
    fill="tozeroy", fillcolor="rgba(124,58,237,0.08)",
)
fig_cum.add_scatter(
    x=all_days, y=plan_curve,
    mode="lines", name="Плановая кривая",
    line=dict(color="#10B981", width=2, dash="dash"),
)
fig_cum.update_layout(
    height=300, margin=dict(t=10, b=10),
    xaxis_title="День", yaxis_title="Накопительно (шт.)",
    legend=dict(orientation="h", y=1.1),
)
st.plotly_chart(fig_cum, use_container_width=True)

st.markdown("---")

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
                    name="WB",   line=dict(color="#7C3AED", width=2))
fig_ord.add_scatter(x=all_days, y=oz_ord_by_day.values, mode="lines+markers",
                    name="Ozon", line=dict(color="#F97316", width=2))
fig_ord.update_layout(
    height=300, margin=dict(t=10, b=10),
    xaxis_title="День", yaxis_title="Заказов (шт.)",
    legend=dict(orientation="h", y=1.1),
)
st.plotly_chart(fig_ord, use_container_width=True)

st.markdown("---")

# ── Сводная таблица ───────────────────────────────────────────────────────────
st.markdown("#### Сводная таблица")
summary = pd.DataFrame([
    {
        "Платформа":       "Wildberries",
        "Заказов":         wb_qty_orders,
        "Выкупов":         wb_qty_sold,
        "Возвратов/Отмен": wb_qty_returned,
        "Вклад в план, %": round(wb_qty_sold / monthly_plan * 100, 1) if monthly_plan else 0,
    },
    {
        "Платформа":       "Ozon",
        "Заказов":         oz_qty_orders,
        "Выкупов":         oz_qty_sold,
        "Возвратов/Отмен": oz_qty_cancelled,
        "Вклад в план, %": round(oz_qty_sold / monthly_plan * 100, 1) if monthly_plan else 0,
    },
    {
        "Платформа":       "ИТОГО",
        "Заказов":         total_orders,
        "Выкупов":         total_sold,
        "Возвратов/Отмен": total_returns,
        "Вклад в план, %": round(total_sold / monthly_plan * 100, 1) if monthly_plan else 0,
    },
])
st.dataframe(
    summary, use_container_width=True, hide_index=True,
    column_config={
        "Вклад в план, %": st.column_config.NumberColumn(format="%.1f %%"),
    },
)
