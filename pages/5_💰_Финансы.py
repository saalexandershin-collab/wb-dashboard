import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import calendar

from src.db.models import init_db, get_session_factory
from src.db.repository import FinancialReportRepository
from src.auth import require_login

st.set_page_config(page_title="Финансы", page_icon="💰", layout="wide")
require_login()
st.title("💰 Финансовые отчёты")

if "database" not in st.secrets:
    st.error("Настройте базу данных в разделе ⚙️ Настройки.")
    st.stop()

DB_URL = st.secrets["database"]["url"]

# ── Выбор периода ─────────────────────────────────────────────────────────────
from datetime import date as date_cls
today = date_cls.today()
col_y, col_m = st.sidebar.columns(2)
year  = col_y.selectbox("Год",   list(range(today.year, today.year - 3, -1)), index=0)
month = col_m.selectbox("Месяц", list(range(1, 13)), index=today.month - 1,
                        format_func=lambda m: calendar.month_name[m])
st.sidebar.markdown("---")
st.sidebar.caption(f"Период: {calendar.month_name[month]} {year}")
st.sidebar.markdown("---")
st.sidebar.markdown(f"👤 {st.session_state.get('username', '')}")
if st.sidebar.button("Выйти"):
    st.session_state.clear()
    st.rerun()

# ── Загрузка данных ───────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Загружаю финансовые данные...")
def load_fin(db_url: str, year: int, month: int) -> pd.DataFrame:
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        return FinancialReportRepository().get_by_month(session, year, month)

df = load_fin(DB_URL, year, month)

if df.empty:
    st.warning("Нет финансовых данных за этот период. Загрузите их командой:")
    st.code(
        f"WB_API_TOKEN='...' DATABASE_URL='...' "
        f"SYNC_YEAR={year} SYNC_MONTH={month} "
        "python3 scripts/sync_finances.py"
    )
    st.stop()

# ── Типы операций ─────────────────────────────────────────────────────────────
# Продажи и возвраты — строки с nm_id (товарные операции)
# Логистика, хранение, штрафы — строки без nm_id (или с doc_type_name = "Услуги")
sales_mask   = df["doc_type_name"] == "Продажа"
returns_mask = df["doc_type_name"] == "Возврат"
service_mask = ~(sales_mask | returns_mask)

df_sales   = df[sales_mask].copy()
df_returns = df[returns_mask].copy()
df_service = df[service_mask].copy()

# ── Агрегаты ──────────────────────────────────────────────────────────────────
def safe_sum(series) -> float:
    return float(series.fillna(0).sum())

revenue      = safe_sum(df_sales["retail_price_withdisc_rub"] * df_sales["quantity"].clip(lower=0))
commission   = abs(safe_sum(df["ppvz_sales_commission"].fillna(0)))
logistics    = abs(safe_sum(df["delivery_rub"].fillna(0)))
storage      = abs(safe_sum(df["storage_fee"].fillna(0)))
penalties    = abs(safe_sum(df["penalty"].fillna(0)))
acquiring    = abs(safe_sum(df["acquiring_fee"].fillna(0)))
other        = safe_sum(df["additional_payment"].fillna(0))
net_payout   = safe_sum(df["ppvz_for_pay"].fillna(0))

qty_sold     = int(df_sales["quantity"].clip(lower=0).sum())
qty_returned = int(abs(df_returns["quantity"].fillna(0).sum()))

# ── KPI ───────────────────────────────────────────────────────────────────────
st.markdown("### Итого за месяц")
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Выручка (цена продажи)", f"{revenue:,.0f} ₽".replace(",", " "))
k2.metric("Комиссия WB",            f"−{commission:,.0f} ₽".replace(",", " "))
k3.metric("Логистика",              f"−{logistics:,.0f} ₽".replace(",", " "))
k4.metric("Хранение",               f"−{storage:,.0f} ₽".replace(",", " "))
k5.metric("Штрафы",                 f"−{penalties:,.0f} ₽".replace(",", " ") if penalties else "0 ₽")

st.markdown("---")
col_a, col_b, col_c, col_d = st.columns(4)
col_a.metric("Продано (шт.)",   qty_sold)
col_b.metric("Возвратов (шт.)", qty_returned)
col_c.metric("Прочие удержания / доплаты",
             f"{'−' if other < 0 else '+'}{abs(other):,.0f} ₽".replace(",", " "))
col_d.metric("К перечислению продавцу",
             f"{net_payout:,.0f} ₽".replace(",", " "),
             help="Итоговая сумма, которую WB перечисляет на счёт продавца")

st.markdown("---")

# ── Waterfall: структура выплаты ──────────────────────────────────────────────
st.markdown("#### Из чего складывается выплата продавцу")

wf_labels  = ["Выручка", "Комиссия WB", "Логистика", "Хранение", "Штрафы", "Прочее", "Итого"]
wf_measure = ["absolute", "relative", "relative", "relative", "relative", "relative", "total"]
wf_values  = [revenue, -commission, -logistics, -storage, -penalties, other, net_payout]
wf_colors  = ["#7C3AED", "#EF4444", "#F59E0B", "#F59E0B", "#EF4444",
               "#10B981" if other >= 0 else "#EF4444", "#10B981" if net_payout >= 0 else "#EF4444"]

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
    totals={"marker": {"color": "#7C3AED"}},
))
fig_wf.update_layout(margin=dict(t=30, b=10), height=380, showlegend=False)
st.plotly_chart(fig_wf, use_container_width=True)

st.markdown("---")

# ── Детализация по товарам ────────────────────────────────────────────────────
st.markdown("#### Детализация по товарам")

if not df_sales.empty or not df_returns.empty:
    # Объединяем продажи и возвраты
    prod_df = pd.concat([df_sales, df_returns], ignore_index=True)
    prod_df["ppvz_for_pay"]          = prod_df["ppvz_for_pay"].fillna(0)
    prod_df["ppvz_sales_commission"] = prod_df["ppvz_sales_commission"].fillna(0)
    prod_df["delivery_rub"]          = prod_df["delivery_rub"].fillna(0)
    prod_df["retail_price_withdisc_rub"] = prod_df["retail_price_withdisc_rub"].fillna(0)
    prod_df["quantity"]              = prod_df["quantity"].fillna(0)

    is_sale   = prod_df["doc_type_name"] == "Продажа"
    is_return = prod_df["doc_type_name"] == "Возврат"

    grp = prod_df.groupby(["nm_id", "supplier_article", "brand_name", "subject_name"]).agg(
        qty_sold    =("quantity",                  lambda x: x[is_sale.reindex(x.index, fill_value=False)].clip(lower=0).sum()),
        qty_returned=("quantity",                  lambda x: abs(x[is_return.reindex(x.index, fill_value=False)].fillna(0).sum())),
        avg_price   =("retail_price_withdisc_rub", lambda x: x[is_sale.reindex(x.index, fill_value=False)].mean()),
        commission  =("ppvz_sales_commission",     lambda x: abs(x.sum())),
        logistics   =("delivery_rub",              lambda x: abs(x.sum())),
        net_payout  =("ppvz_for_pay",              "sum"),
    ).reset_index()

    grp["qty_sold"]     = grp["qty_sold"].astype(int)
    grp["qty_returned"] = grp["qty_returned"].astype(int)
    grp["avg_price"]    = grp["avg_price"].round(0)
    grp["commission"]   = grp["commission"].round(0)
    grp["logistics"]    = grp["logistics"].round(0)
    grp["net_payout"]   = grp["net_payout"].round(0)

    grp = grp.sort_values("net_payout", ascending=False)

    st.dataframe(
        grp.rename(columns={
            "nm_id":           "nmId WB",
            "supplier_article":"Артикул",
            "brand_name":      "Бренд",
            "subject_name":    "Предмет",
            "qty_sold":        "Продано (шт.)",
            "qty_returned":    "Возвраты (шт.)",
            "avg_price":       "Средняя цена (₽)",
            "commission":      "Комиссия WB (₽)",
            "logistics":       "Логистика (₽)",
            "net_payout":      "Выплата продавцу (₽)",
        }),
        use_container_width=True,
        hide_index=True,
        height=500,
        column_config={
            "Средняя цена (₽)":      st.column_config.NumberColumn(format="%.0f ₽"),
            "Комиссия WB (₽)":       st.column_config.NumberColumn(format="%.0f ₽"),
            "Логистика (₽)":         st.column_config.NumberColumn(format="%.0f ₽"),
            "Выплата продавцу (₽)":  st.column_config.NumberColumn(format="%.0f ₽"),
        },
    )

    # ── Топ-10 по выплате ─────────────────────────────────────────────────────
    st.markdown("#### Топ-10 товаров по выплате продавцу")
    top10 = grp.nlargest(10, "net_payout")
    top10["label"] = top10["supplier_article"].fillna("") + " / " + top10["brand_name"].fillna("")
    fig_top = px.bar(top10, x="net_payout", y="label", orientation="h",
                     color_discrete_sequence=["#7C3AED"],
                     labels={"net_payout": "Выплата (₽)", "label": ""})
    fig_top.update_layout(yaxis=dict(autorange="reversed"),
                          margin=dict(t=10, b=10), height=320)
    st.plotly_chart(fig_top, use_container_width=True)
else:
    st.info("Нет товарных операций за выбранный период.")
