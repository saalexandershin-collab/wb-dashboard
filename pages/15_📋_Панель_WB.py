"""
Панель WB — управленческий P&L по месяцам 2026 года.
Продажи по продуктам + налоговая база + сводка по году.
"""
import calendar
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Панель WB", page_icon="📋", layout="wide")
st.title("📋 Панель WB — Управленческий отчёт 2026")

if "database" not in st.secrets:
    st.error("Настройте базу данных в разделе ⚙️ Настройки.")
    st.stop()

DB_URL = st.secrets["database"]["url"]

# ── Константы ──────────────────────────────────────────────────────────────────
MONTHS_RU = {
    1: "Январь", 2: "Февраль", 3: "Март",
    4: "Апрель", 5: "Май",     6: "Июнь",
    7: "Июль",   8: "Август",  9: "Сентябрь",
    10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}
AVAILABLE_MONTHS = [1, 2, 3, 4, 5]
TAX_RATE = 0.06   # УСН 6% — можно изменить в боковой панели


# ── Загрузка данных ────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner="Загружаю данные WB…")
def load_month(db_url: str, year: int, month: int) -> pd.DataFrame:
    """Финотчёт WB за все недельные периоды, начавшиеся в данном месяце."""
    engine = create_engine(db_url)
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT
                subject_name,
                supplier_article,
                nm_id,
                doc_type_name,
                supplier_oper_name,
                quantity,
                retail_price,
                retail_price_withdisc_rub,
                ppvz_for_pay,
                ppvz_sales_commission,
                delivery_rub,
                storage_fee,
                penalty,
                additional_payment,
                acquiring_fee
            FROM financial_reports
            WHERE platform = 'wb'
              AND EXTRACT(YEAR  FROM date_from) = :yr
              AND EXTRACT(MONTH FROM date_from) = :mo
        """), conn, params={"yr": year, "mo": month})
    return df


@st.cache_data(ttl=600, show_spinner="Загружаю годовые данные…")
def load_year(db_url: str, year: int) -> pd.DataFrame:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT
                EXTRACT(MONTH FROM date_from)::int  AS month,
                subject_name,
                supplier_article,
                nm_id,
                doc_type_name,
                quantity,
                retail_price,
                retail_price_withdisc_rub,
                ppvz_for_pay,
                ppvz_sales_commission,
                delivery_rub,
                storage_fee,
                penalty,
                additional_payment,
                acquiring_fee
            FROM financial_reports
            WHERE platform = 'wb'
              AND EXTRACT(YEAR FROM date_from) = :yr
              AND EXTRACT(MONTH FROM date_from) BETWEEN 1 AND 12
        """), conn, params={"yr": year})
    return df


# ── Боковая панель ─────────────────────────────────────────────────────────────
st.sidebar.header("Настройки")
selected_month = st.sidebar.selectbox(
    "Выберите месяц",
    AVAILABLE_MONTHS,
    format_func=lambda m: MONTHS_RU[m],
    index=3,  # апрель по умолчанию
)
tax_rate = st.sidebar.number_input(
    "Ставка налога (%)",
    min_value=1.0, max_value=20.0,
    value=6.0, step=0.5,
) / 100
st.sidebar.markdown("---")
st.sidebar.caption("Налоговая база = цена реализации (retail_price_withdisc_rub) за минусом возвратов")


# ── Вкладки ────────────────────────────────────────────────────────────────────
tab_month, tab_year = st.tabs([
    f"📅 {MONTHS_RU[selected_month]} 2026",
    "📊 Весь 2026 год",
])


# ═══════════════════════════════════════════════════════════════════════════════
# ВКЛАДКА 1 — ВЫБРАННЫЙ МЕСЯЦ
# ═══════════════════════════════════════════════════════════════════════════════
with tab_month:
    df = load_month(DB_URL, 2026, selected_month)

    if df.empty:
        st.warning(f"Нет данных за {MONTHS_RU[selected_month]} 2026.")
        st.stop()

    # Числовые колонки — приводим к float
    num_cols = ["quantity", "retail_price", "retail_price_withdisc_rub",
                "ppvz_for_pay", "ppvz_sales_commission",
                "delivery_rub", "storage_fee", "penalty",
                "additional_payment", "acquiring_fee"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    sales_df   = df[df["doc_type_name"] == "Продажа"].copy()
    returns_df = df[df["doc_type_name"] == "Возврат"].copy()

    # ── Агрегаты месяца ───────────────────────────────────────────────────────
    sold_qty   = int(sales_df["quantity"].clip(lower=0).sum())
    return_qty = int(abs(returns_df["quantity"].sum()))

    revenue    = float((sales_df["retail_price_withdisc_rub"] * sales_df["quantity"].clip(lower=0)).sum())
    rev_return = float((returns_df["retail_price_withdisc_rub"] * returns_df["quantity"].abs()).sum())
    net_revenue = revenue - rev_return                          # налоговая база

    payout      = float(df["ppvz_for_pay"].sum())               # начислено ВБ (может быть суммой за несколько недель)
    commission  = float(df["ppvz_sales_commission"].fillna(0).sum())
    logistics   = float(df["delivery_rub"].fillna(0).sum())
    storage     = float(df["storage_fee"].fillna(0).sum())
    penalties   = float(df["penalty"].fillna(0).sum())
    acquiring   = float(df["acquiring_fee"].fillna(0).sum())
    add_pay     = float(df["additional_payment"].fillna(0).sum())

    tax_amount  = net_revenue * tax_rate

    # ── KPI-карточки ──────────────────────────────────────────────────────────
    st.markdown(f"### {MONTHS_RU[selected_month]} 2026 — Сводка")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Продано, шт.",         f"{sold_qty:,}".replace(",", " "))
    c2.metric("Возвратов, шт.",        f"{return_qty:,}".replace(",", " "))
    c3.metric("Выручка (реализация)",  f"{revenue:,.0f} ₽".replace(",", " "))
    c4.metric("Начислено ВБ",          f"{payout:,.0f} ₽".replace(",", " "))
    c5.metric("Налоговая база",        f"{net_revenue:,.0f} ₽".replace(",", " "))
    c6.metric(f"Налог УСН {tax_rate*100:.0f}%", f"{tax_amount:,.0f} ₽".replace(",", " "))

    st.markdown("---")

    # ── РАЗДЕЛ 1: Продажи по продуктам ───────────────────────────────────────
    st.markdown("#### 📦 Продажи по продуктам")

    prod_sales = (
        sales_df[sales_df["quantity"] > 0]
        .groupby(["subject_name", "supplier_article"], dropna=False)
        .agg(
            qty         = ("quantity", "sum"),
            revenue     = ("retail_price_withdisc_rub", lambda x: (x * sales_df.loc[x.index, "quantity"].clip(lower=0)).sum()),
            wb_payout   = ("ppvz_for_pay", "sum"),
            commission  = ("ppvz_sales_commission", "sum"),
        )
        .reset_index()
        .sort_values("revenue", ascending=False)
    )
    # Возвраты по продуктам
    prod_returns = (
        returns_df.groupby(["subject_name", "supplier_article"], dropna=False)
        .agg(ret_qty=("quantity", lambda x: int(abs(x.sum()))))
        .reset_index()
    ) if not returns_df.empty else pd.DataFrame(columns=["subject_name", "supplier_article", "ret_qty"])

    prod = prod_sales.merge(prod_returns, on=["subject_name", "supplier_article"], how="left")
    prod["ret_qty"] = prod["ret_qty"].fillna(0).astype(int)
    prod["net_qty"] = prod["qty"] - prod["ret_qty"]

    # Форматируем для отображения
    prod_display = pd.DataFrame({
        "Продукт":            prod["subject_name"].fillna("—"),
        "Артикул":            prod["supplier_article"].fillna("—"),
        "Продано, шт.":       prod["qty"].astype(int),
        "Возвратов, шт.":     prod["ret_qty"],
        "Чистые продажи":     prod["net_qty"],
        "Выручка, ₽":         prod["revenue"].round(0).astype(int),
        "Начислено ВБ, ₽":    prod["wb_payout"].round(0).astype(int),
        "Комиссия ВБ, ₽":     prod["commission"].round(0).astype(int),
    })

    st.dataframe(
        prod_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Выручка, ₽":      st.column_config.NumberColumn(format="%d ₽"),
            "Начислено ВБ, ₽": st.column_config.NumberColumn(format="%d ₽"),
            "Комиссия ВБ, ₽":  st.column_config.NumberColumn(format="%d ₽"),
        }
    )

    # Итоговая строка под таблицей
    st.caption(
        f"Итого: **{int(prod['qty'].sum()):,} шт.** продано · "
        f"**{int(prod['ret_qty'].sum()):,} шт.** возвращено · "
        f"выручка **{prod['revenue'].sum():,.0f} ₽** · "
        f"начислено ВБ **{prod['wb_payout'].sum():,.0f} ₽**"
        .replace(",", " ")
    )

    st.markdown("---")

    # ── РАЗДЕЛ 2: Удержания ВБ ───────────────────────────────────────────────
    st.markdown("#### 🔻 Удержания и расходы WB")
    d1, d2, d3, d4, d5, d6 = st.columns(6)
    d1.metric("Комиссия",   f"{abs(commission):,.0f} ₽".replace(",", " "), delta=None)
    d2.metric("Логистика",  f"{abs(logistics):,.0f} ₽".replace(",", " "))
    d3.metric("Хранение",   f"{abs(storage):,.0f} ₽".replace(",", " "))
    d4.metric("Эквайринг",  f"{abs(acquiring):,.0f} ₽".replace(",", " "))
    d5.metric("Штрафы",     f"{abs(penalties):,.0f} ₽".replace(",", " "))
    d6.metric("Доплаты/прочее", f"{add_pay:,.0f} ₽".replace(",", " "))

    total_deductions = abs(commission) + abs(logistics) + abs(storage) + abs(acquiring) + abs(penalties)
    st.caption(f"Всего удержано ВБ: **{total_deductions:,.0f} ₽**  · "
               f"Чистый payout: **{payout:,.0f} ₽**".replace(",", " "))

    st.markdown("---")

    # ── РАЗДЕЛ 3: Налоговая база ─────────────────────────────────────────────
    st.markdown("#### 🧾 Налоговая база")

    col_l, col_r = st.columns([1, 1])
    with col_l:
        tax_df = pd.DataFrame({
            "Показатель": [
                "Цена реализации (продажи)",
                "Минус: возвраты покупателей",
                "= Налоговая база",
                f"Налог УСН {tax_rate*100:.0f}%",
            ],
            "Сумма, ₽": [
                f"{revenue:,.0f}",
                f"−{rev_return:,.0f}",
                f"{net_revenue:,.0f}",
                f"{tax_amount:,.0f}",
            ]
        })
        st.dataframe(tax_df, use_container_width=True, hide_index=True)
        st.caption("Налоговая база = цена, по которой ВБ реализовал товар покупателю (retail_price_withdisc_rub), за вычетом возвратов. ВБ — комиссионер, поэтому налог считается от цены продавца, а не от чистого payout.")

    with col_r:
        # Круговая диаграмма: из чего состоит выручка
        fig = go.Figure(go.Pie(
            labels=["Получено (payout)", "Комиссия ВБ", "Логистика", "Хранение", "Эквайринг"],
            values=[max(payout, 0), abs(commission), abs(logistics), abs(storage), abs(acquiring)],
            hole=0.45,
            textinfo="label+percent",
            marker=dict(colors=["#22c55e", "#ef4444", "#f59e0b", "#3b82f6", "#a855f7"]),
        ))
        fig.update_layout(
            title=f"Распределение выручки {MONTHS_RU[selected_month]}",
            height=320,
            margin=dict(t=40, b=0, l=0, r=0),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# ВКЛАДКА 2 — ВЕСЬ 2026 ГОД
# ═══════════════════════════════════════════════════════════════════════════════
with tab_year:
    st.markdown("### Сводный отчёт WB — 2026 год по месяцам")

    df_year = load_year(DB_URL, 2026)

    if df_year.empty:
        st.warning("Нет данных за 2026 год.")
        st.stop()

    num_cols = ["quantity", "retail_price_withdisc_rub", "ppvz_for_pay",
                "ppvz_sales_commission", "delivery_rub", "storage_fee",
                "penalty", "additional_payment", "acquiring_fee"]
    for c in num_cols:
        df_year[c] = pd.to_numeric(df_year[c], errors="coerce").fillna(0)

    yr_sales   = df_year[df_year["doc_type_name"] == "Продажа"].copy()
    yr_returns = df_year[df_year["doc_type_name"] == "Возврат"].copy()

    # Месячные агрегаты
    monthly = yr_sales[yr_sales["month"].isin(AVAILABLE_MONTHS)].groupby("month").agg(
        sold_qty  = ("quantity", lambda x: int(x.clip(lower=0).sum())),
        revenue   = ("retail_price_withdisc_rub",
                     lambda x: float((x * yr_sales.loc[x.index, "quantity"].clip(lower=0)).sum())),
        payout    = ("ppvz_for_pay", "sum"),
        commission= ("ppvz_sales_commission", "sum"),
        logistics = ("delivery_rub", "sum"),
        storage   = ("storage_fee", "sum"),
        penalties = ("penalty", "sum"),
        acquiring = ("acquiring_fee", "sum"),
        add_pay   = ("additional_payment", "sum"),
    ).reset_index()

    ret_monthly = yr_returns[yr_returns["month"].isin(AVAILABLE_MONTHS)].groupby("month").agg(
        ret_qty   = ("quantity", lambda x: int(abs(x.sum()))),
        rev_return= ("retail_price_withdisc_rub",
                     lambda x: float((x * yr_returns.loc[x.index, "quantity"].abs()).sum())),
    ).reset_index()

    monthly = monthly.merge(ret_monthly, on="month", how="left")
    monthly["ret_qty"]    = monthly["ret_qty"].fillna(0).astype(int)
    monthly["rev_return"] = monthly["rev_return"].fillna(0)
    monthly["tax_base"]   = monthly["revenue"] - monthly["rev_return"]
    monthly["tax_amount"] = monthly["tax_base"] * tax_rate
    monthly["month_name"] = monthly["month"].map(MONTHS_RU)

    # ── Таблица по месяцам ────────────────────────────────────────────────────
    display_year = pd.DataFrame({
        "Месяц":               monthly["month_name"],
        "Продано, шт.":        monthly["sold_qty"],
        "Возвратов, шт.":      monthly["ret_qty"],
        "Выручка, ₽":          monthly["revenue"].round(0).astype(int),
        "Начислено ВБ, ₽":     monthly["payout"].round(0).astype(int),
        "Комиссия ВБ, ₽":      monthly["commission"].abs().round(0).astype(int),
        "Логистика, ₽":        monthly["logistics"].abs().round(0).astype(int),
        "Хранение, ₽":         monthly["storage"].abs().round(0).astype(int),
        "Налоговая база, ₽":   monthly["tax_base"].round(0).astype(int),
        f"Налог УСН {tax_rate*100:.0f}%, ₽": monthly["tax_amount"].round(0).astype(int),
    })

    # Итоговая строка
    totals = {
        "Месяц": "ИТОГО",
        "Продано, шт.":        int(monthly["sold_qty"].sum()),
        "Возвратов, шт.":      int(monthly["ret_qty"].sum()),
        "Выручка, ₽":          int(monthly["revenue"].sum()),
        "Начислено ВБ, ₽":     int(monthly["payout"].sum()),
        "Комиссия ВБ, ₽":      int(monthly["commission"].abs().sum()),
        "Логистика, ₽":        int(monthly["logistics"].abs().sum()),
        "Хранение, ₽":         int(monthly["storage"].abs().sum()),
        "Налоговая база, ₽":   int(monthly["tax_base"].sum()),
        f"Налог УСН {tax_rate*100:.0f}%, ₽": int(monthly["tax_amount"].sum()),
    }
    display_year = pd.concat([display_year, pd.DataFrame([totals])], ignore_index=True)

    st.dataframe(
        display_year,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Выручка, ₽":          st.column_config.NumberColumn(format="%d ₽"),
            "Начислено ВБ, ₽":     st.column_config.NumberColumn(format="%d ₽"),
            "Комиссия ВБ, ₽":      st.column_config.NumberColumn(format="%d ₽"),
            "Логистика, ₽":        st.column_config.NumberColumn(format="%d ₽"),
            "Хранение, ₽":         st.column_config.NumberColumn(format="%d ₽"),
            "Налоговая база, ₽":   st.column_config.NumberColumn(format="%d ₽"),
            f"Налог УСН {tax_rate*100:.0f}%, ₽": st.column_config.NumberColumn(format="%d ₽"),
        }
    )

    st.markdown("---")

    # ── Графики ───────────────────────────────────────────────────────────────
    fig = go.Figure()
    fig.add_bar(
        x=monthly["month_name"], y=monthly["revenue"],
        name="Выручка", marker_color="#3b82f6"
    )
    fig.add_bar(
        x=monthly["month_name"], y=monthly["payout"],
        name="Начислено ВБ", marker_color="#22c55e"
    )
    fig.add_bar(
        x=monthly["month_name"], y=monthly["tax_base"],
        name="Налоговая база", marker_color="#f59e0b"
    )
    fig.update_layout(
        barmode="group",
        title="Выручка / Начислено / Налоговая база по месяцам",
        yaxis_title="₽",
        height=380,
        legend=dict(orientation="h", y=1.05),
        margin=dict(t=60, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    # График продаж в штуках
    fig2 = go.Figure()
    fig2.add_scatter(
        x=monthly["month_name"], y=monthly["sold_qty"],
        mode="lines+markers+text",
        name="Продано",
        text=monthly["sold_qty"].astype(str) + " шт.",
        textposition="top center",
        line=dict(color="#3b82f6", width=2),
        marker=dict(size=8),
    )
    fig2.add_scatter(
        x=monthly["month_name"], y=monthly["ret_qty"],
        mode="lines+markers",
        name="Возвраты",
        line=dict(color="#ef4444", width=2, dash="dot"),
        marker=dict(size=6),
    )
    fig2.update_layout(
        title="Продажи и возвраты по месяцам (шт.)",
        yaxis_title="шт.",
        height=300,
        margin=dict(t=50, b=20),
        legend=dict(orientation="h", y=1.05),
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")

    # ── Продажи по продуктам за год ───────────────────────────────────────────
    st.markdown("#### 📦 Продажи по продуктам за 2026 год")

    yr_prod = (
        yr_sales[yr_sales["quantity"] > 0]
        .groupby(["subject_name", "supplier_article"], dropna=False)
        .agg(
            sold_qty  = ("quantity", "sum"),
            revenue   = ("retail_price_withdisc_rub",
                         lambda x: float((x * yr_sales.loc[x.index, "quantity"].clip(lower=0)).sum())),
            wb_payout = ("ppvz_for_pay", "sum"),
        )
        .reset_index()
        .sort_values("revenue", ascending=False)
    )

    st.dataframe(
        pd.DataFrame({
            "Продукт":          yr_prod["subject_name"].fillna("—"),
            "Артикул":          yr_prod["supplier_article"].fillna("—"),
            "Продано, шт.":     yr_prod["sold_qty"].astype(int),
            "Выручка, ₽":       yr_prod["revenue"].round(0).astype(int),
            "Начислено ВБ, ₽":  yr_prod["wb_payout"].round(0).astype(int),
        }),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Выручка, ₽":      st.column_config.NumberColumn(format="%d ₽"),
            "Начислено ВБ, ₽": st.column_config.NumberColumn(format="%d ₽"),
        }
    )

    # ── Итоговые KPI года ─────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 🏁 Итого 2026 (январь — май)")
    y1, y2, y3, y4, y5 = st.columns(5)
    y1.metric("Продано, шт.",       f"{int(monthly['sold_qty'].sum()):,}".replace(",", " "))
    y2.metric("Выручка",            f"{int(monthly['revenue'].sum()):,} ₽".replace(",", " "))
    y3.metric("Начислено ВБ",       f"{int(monthly['payout'].sum()):,} ₽".replace(",", " "))
    y4.metric("Налоговая база",     f"{int(monthly['tax_base'].sum()):,} ₽".replace(",", " "))
    y5.metric(f"Налог УСН {tax_rate*100:.0f}%", f"{int(monthly['tax_amount'].sum()):,} ₽".replace(",", " "))
