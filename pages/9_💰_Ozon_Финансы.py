import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import calendar

from src.db.models import init_db, get_session_factory
from src.db.repository import OzonTransactionRepository
from src.auth import require_role

require_role(["admin"])
st.title("💰 Финансовые отчёты Ozon")

if "database" not in st.secrets:
    st.error("Настройте базу данных в разделе ⚙️ Настройки.")
    st.stop()

DB_URL = st.secrets["database"]["url"]

from datetime import date as date_cls
today = date_cls.today()
col_y, col_m = st.sidebar.columns(2)
year  = col_y.selectbox("Год",   list(range(today.year, today.year - 3, -1)), index=0)
month = col_m.selectbox("Месяц", list(range(1, 13)), index=today.month - 1,
                        format_func=lambda m: calendar.month_name[m])
st.sidebar.markdown("---")
st.sidebar.caption(f"Период: {calendar.month_name[month]} {year}")

@st.cache_data(ttl=300, show_spinner="Загружаю финансовые данные Ozon...")
def load_fin(db_url, year, month):
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        return OzonTransactionRepository().get_by_month(session, year, month)

df = load_fin(DB_URL, year, month)

if df.empty:
    st.warning("Нет финансовых данных Ozon за этот период. Загрузите командой:")
    st.code(
        f"OZON_CLIENT_ID='...' OZON_API_KEY='...' DATABASE_URL='...' "
        f"SYNC_YEAR={year} SYNC_MONTH={month} "
        "python3 scripts/sync_ozon_finances.py"
    )
    st.stop()

def safe_sum(s):
    return float(s.fillna(0).sum())

# Группируем по типу операции
accrual_mask = df["operation_type"].str.contains("accrual", case=False, na=False)
service_mask = df["operation_type"].str.contains("service|logistic|deliver", case=False, na=False)
return_mask  = df["operation_type"].str.contains("return|compensation", case=False, na=False)

revenue     = safe_sum(df[accrual_mask]["amount"].clip(lower=0))
commission  = abs(safe_sum(df["sale_commission"].fillna(0)))
logistics   = abs(safe_sum(df["delivery_charge"].fillna(0)))
returns     = abs(safe_sum(df[return_mask]["amount"].clip(upper=0)))
net_payout  = safe_sum(df["amount"])
other       = net_payout - revenue + commission + logistics - returns

qty_sold     = int(df[accrual_mask]["quantity"].fillna(0).sum())
qty_returned = int(df[return_mask]["quantity"].fillna(0).sum())

# ── KPI ───────────────────────────────────────────────────────────────────────
st.markdown("### Итого за месяц")
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Выручка Ozon",    f"{revenue:,.0f} ₽".replace(",", " "))
k2.metric("Комиссия Ozon",   f"−{commission:,.0f} ₽".replace(",", " "))
k3.metric("Логистика",       f"−{logistics:,.0f} ₽".replace(",", " "))
k4.metric("Возвраты",        f"−{returns:,.0f} ₽".replace(",", " ") if returns else "0 ₽")
k5.metric("К перечислению",  f"{net_payout:,.0f} ₽".replace(",", " "))

st.markdown("---")
col_a, col_b = st.columns(2)
col_a.metric("Продано (шт.)",   qty_sold)
col_b.metric("Возвратов (шт.)", qty_returned)

st.markdown("---")

# ── Waterfall ─────────────────────────────────────────────────────────────────
st.markdown("#### Структура выплаты")
wf_labels  = ["Выручка", "Комиссия", "Логистика", "Возвраты", "Прочее", "Итого"]
wf_measure = ["absolute", "relative", "relative", "relative", "relative", "total"]
wf_values  = [revenue, -commission, -logistics, -returns, other, net_payout]
fig_wf = go.Figure(go.Waterfall(
    orientation="v",
    measure=wf_measure,
    x=wf_labels,
    y=wf_values,
    text=[f"{abs(v):,.0f}".replace(",", " ") for v in wf_values],
    textposition="outside",
    connector={"line": {"color": "#9CA3AF"}},
    increasing={"marker": {"color": "#10B981"}},
    decreasing={"marker": {"color": "#EF4444"}},
    totals={"marker": {"color": "#F97316"}},
))
fig_wf.update_layout(margin=dict(t=30, b=10), height=380, showlegend=False)
st.plotly_chart(fig_wf, use_container_width=True)

st.markdown("---")

# ── Детализация по типам операций ────────────────────────────────────────────
st.markdown("#### Детализация по типам операций")
by_type = df.groupby("operation_type_name")["amount"].sum().reset_index()
by_type = by_type[by_type["operation_type_name"] != ""].sort_values("amount")
fig_bar = px.bar(by_type, x="amount", y="operation_type_name", orientation="h",
                 color="amount",
                 color_continuous_scale=["#EF4444", "#F97316", "#10B981"],
                 labels={"amount": "Сумма (₽)", "operation_type_name": ""})
fig_bar.update_layout(height=max(300, len(by_type) * 30),
                      margin=dict(t=10, b=10), coloraxis_showscale=False)
st.plotly_chart(fig_bar, use_container_width=True)

# ── Детализация по товарам ────────────────────────────────────────────────────
if not df[accrual_mask].empty:
    st.markdown("---")
    st.markdown("#### Детализация по товарам")
    grp = df[accrual_mask].groupby(["offer_id", "product_name"]).agg(
        qty_sold   =("quantity", "sum"),
        revenue    =("amount",   lambda x: x.clip(lower=0).sum()),
        commission =("sale_commission", lambda x: abs(x.fillna(0).sum())),
        logistics  =("delivery_charge", lambda x: abs(x.fillna(0).sum())),
        net_payout =("amount", "sum"),
    ).reset_index()
    grp = grp.sort_values("net_payout", ascending=False)
    st.dataframe(
        grp.rename(columns={
            "offer_id": "Артикул", "product_name": "Название",
            "qty_sold": "Продано (шт.)", "revenue": "Выручка (₽)",
            "commission": "Комиссия (₽)", "logistics": "Логистика (₽)",
            "net_payout": "Выплата (₽)",
        }),
        use_container_width=True, hide_index=True, height=400,
        column_config={
            "Выручка (₽)":   st.column_config.NumberColumn(format="%.0f ₽"),
            "Комиссия (₽)":  st.column_config.NumberColumn(format="%.0f ₽"),
            "Логистика (₽)": st.column_config.NumberColumn(format="%.0f ₽"),
            "Выплата (₽)":   st.column_config.NumberColumn(format="%.0f ₽"),
        },
    )
