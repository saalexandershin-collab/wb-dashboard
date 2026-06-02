"""
Панель Ozon — управленческий P&L по месяцам 2026 года.
Финансовые транзакции Ozon = основа для налогового и бухгалтерского учёта.
"""
import io
import calendar
from datetime import date

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Панель Ozon", page_icon="🟦", layout="wide")
st.title("🟦 Панель Ozon — Бухгалтерский отчёт 2026")

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
MONTHS_RU_GEN = {
    1: "января", 2: "февраля", 3: "марта",
    4: "апреля", 5: "мая",     6: "июня",
    7: "июля",   8: "августа", 9: "сентября",
    10: "октября", 11: "ноября", 12: "декабря",
}

_today = date.today()
REPORT_YEAR = 2026
AVAILABLE_MONTHS = (
    list(range(1, _today.month + 1))
    if _today.year == REPORT_YEAR
    else list(range(1, 13))
)
RANGE_LABEL = (
    f"{MONTHS_RU_GEN[AVAILABLE_MONTHS[0]]} — {MONTHS_RU_GEN[AVAILABLE_MONTHS[-1]]}"
    if len(AVAILABLE_MONTHS) > 1
    else MONTHS_RU_GEN[AVAILABLE_MONTHS[0]]
)

# Группировка типов операций Ozon
OP_INCOME   = "Доставка покупателю"
OP_RETURNS  = "Получение возврата, отмены, невыкупа от покупателя"
OP_RETURN_LOG = "Доставка и обработка возврата, отмены, невыкупа"
OP_ACQUIRING = "Оплата эквайринга"
OP_STORAGE   = "Услуга размещения товаров на складе"
OP_ADS_CLICK = "Оплата за клик"
OP_ADS_ORDER = "Продвижение с оплатой за заказ"
OP_CROSSDOCK = "Кросс-докинг"
OP_LOSS_OZON = "Потеря по вине Ozon в логистике"


# ── Загрузка данных ────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner="Загружаю транзакции Ozon…")
def load_transactions(db_url: str, year: int, month: int) -> pd.DataFrame:
    """Все финансовые транзакции Ozon за месяц (по operation_date)."""
    engine = create_engine(db_url)
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT operation_type_name, amount
            FROM ozon_transactions
            WHERE EXTRACT(YEAR  FROM operation_date) = :yr
              AND EXTRACT(MONTH FROM operation_date) = :mo
        """), conn, params={"yr": year, "mo": month})
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    return df


@st.cache_data(ttl=600, show_spinner="Загружаю постинги Ozon…")
def load_postings(db_url: str, year: int, month: int) -> pd.DataFrame:
    """Заказы Ozon за месяц (по created_at) с разбивкой по продуктам."""
    engine = create_engine(db_url)
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT
                offer_id,
                product_name,
                quantity,
                price,
                payout,
                commission_amount,
                is_cancelled
            FROM ozon_postings
            WHERE EXTRACT(YEAR  FROM created_at) = :yr
              AND EXTRACT(MONTH FROM created_at) = :mo
        """), conn, params={"yr": year, "mo": month})
    for c in ["quantity", "price", "payout", "commission_amount"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


@st.cache_data(ttl=600, show_spinner="Загружаю годовые данные Ozon…")
def load_year_transactions(db_url: str, year: int) -> pd.DataFrame:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT
                EXTRACT(MONTH FROM operation_date)::int AS month,
                operation_type_name,
                amount
            FROM ozon_transactions
            WHERE EXTRACT(YEAR FROM operation_date) = :yr
              AND EXTRACT(MONTH FROM operation_date) BETWEEN 1 AND 12
        """), conn, params={"yr": year})
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    return df


@st.cache_data(ttl=600, show_spinner="Загружаю годовые постинги Ozon…")
def load_year_postings(db_url: str, year: int) -> pd.DataFrame:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT
                EXTRACT(MONTH FROM created_at)::int AS month,
                offer_id,
                product_name,
                quantity,
                price,
                payout,
                commission_amount,
                is_cancelled
            FROM ozon_postings
            WHERE EXTRACT(YEAR FROM created_at) = :yr
              AND EXTRACT(MONTH FROM created_at) BETWEEN 1 AND 12
        """), conn, params={"yr": year})
    for c in ["quantity", "price", "payout", "commission_amount"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    return buf.getvalue()


def agg_op(df: pd.DataFrame, op_name: str) -> float:
    return float(df[df["operation_type_name"] == op_name]["amount"].sum())


def calc_month_metrics(tx: pd.DataFrame, po: pd.DataFrame):
    """Считает все метрики за месяц из транзакций и постингов."""
    # Из транзакций (по operation_date — правильная дата для налогового учёта)
    payout     = agg_op(tx, OP_INCOME)                  # начислено от Ozon
    returns    = abs(agg_op(tx, OP_RETURNS))             # возврат денег покупателям
    ret_log    = abs(agg_op(tx, OP_RETURN_LOG))          # логистика возвратов
    acquiring  = abs(agg_op(tx, OP_ACQUIRING))
    storage    = abs(agg_op(tx, OP_STORAGE))
    ads        = abs(agg_op(tx, OP_ADS_CLICK)) + abs(agg_op(tx, OP_ADS_ORDER))
    crossdock  = abs(agg_op(tx, OP_CROSSDOCK))
    ozon_loss  = max(0, agg_op(tx, OP_LOSS_OZON))       # компенсация от Ozon (положит.)
    # Прочие удержания — всё отрицательное, кроме основных категорий
    known_ops  = {OP_INCOME, OP_RETURNS, OP_RETURN_LOG, OP_ACQUIRING,
                  OP_STORAGE, OP_ADS_CLICK, OP_ADS_ORDER, OP_CROSSDOCK, OP_LOSS_OZON}
    other_neg  = abs(tx[~tx["operation_type_name"].isin(known_ops) & (tx["amount"] < 0)]["amount"].sum())
    total_fees = ret_log + acquiring + storage + ads + crossdock + other_neg

    # Выручка (реализационная цена) из постингов (по дате заказа)
    delivered  = po[~po["is_cancelled"]]
    revenue    = float((delivered["price"] * delivered["quantity"]).sum())
    sold_qty   = int(delivered["quantity"].sum())
    cancelled  = po[po["is_cancelled"]]
    cancel_qty = int(cancelled["quantity"].sum())

    # Налоговая база = реализационная цена − возвраты покупателей
    tax_base   = revenue - returns

    return dict(
        payout=payout, revenue=revenue, returns=returns,
        tax_base=tax_base, sold_qty=sold_qty, cancel_qty=cancel_qty,
        ret_log=ret_log, acquiring=acquiring, storage=storage,
        ads=ads, crossdock=crossdock, ozon_loss=ozon_loss,
        other_neg=other_neg, total_fees=total_fees,
    )


# ── Боковая панель ─────────────────────────────────────────────────────────────
st.sidebar.header("Настройки")
selected_month = st.sidebar.selectbox(
    "Выберите месяц",
    AVAILABLE_MONTHS,
    format_func=lambda m: MONTHS_RU[m],
    index=len(AVAILABLE_MONTHS) - 1,
)
tax_rate = st.sidebar.number_input(
    "Ставка налога (%)",
    min_value=1.0, max_value=20.0,
    value=6.0, step=0.5,
) / 100
st.sidebar.markdown("---")
st.sidebar.info(
    "**Источник:** транзакции Ozon (operation_date)\n\n"
    "**Налоговая база** = реализационная цена покупателю (price) "
    "за вычетом возвратов. Ozon — комиссионер, налог с полной "
    "цены реализации, а не с payout."
)


# ── Вкладки ────────────────────────────────────────────────────────────────────
tab_month, tab_year = st.tabs([
    f"📅 {MONTHS_RU[selected_month]} {REPORT_YEAR}",
    f"📊 Весь {REPORT_YEAR} год",
])


# ═══════════════════════════════════════════════════════════════════════════════
# ВКЛАДКА 1 — ВЫБРАННЫЙ МЕСЯЦ
# ═══════════════════════════════════════════════════════════════════════════════
with tab_month:
    tx = load_transactions(DB_URL, REPORT_YEAR, selected_month)
    po = load_postings(DB_URL, REPORT_YEAR, selected_month)

    if tx.empty and po.empty:
        st.warning(f"Нет данных за {MONTHS_RU[selected_month]} {REPORT_YEAR}.")
        st.stop()

    m = calc_month_metrics(tx, po)
    tax_amount = m["tax_base"] * tax_rate

    # ── KPI-карточки ──────────────────────────────────────────────────────────
    st.markdown(f"### {MONTHS_RU[selected_month]} {REPORT_YEAR} — Сводка")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Продано, шт.",          f"{m['sold_qty']:,}".replace(",", " "))
    c2.metric("Отменено / возвраты",   f"{m['cancel_qty']:,}".replace(",", " "))
    c3.metric("Выручка (реализация)",  f"{m['revenue']:,.0f} ₽".replace(",", " "))
    c4.metric("Начислено Ozon",        f"{m['payout']:,.0f} ₽".replace(",", " "))
    c5.metric("Налоговая база",        f"{m['tax_base']:,.0f} ₽".replace(",", " "))
    c6.metric(f"Налог УСН {tax_rate*100:.0f}%", f"{tax_amount:,.0f} ₽".replace(",", " "))

    st.markdown("---")

    # ── РАЗДЕЛ 1: Продажи по продуктам ───────────────────────────────────────
    st.markdown("#### 📦 Продажи по продуктам")

    delivered = po[~po["is_cancelled"]].copy()
    if not delivered.empty:
        prod = (
            delivered
            .groupby(["offer_id", "product_name"], dropna=False)
            .agg(
                qty       = ("quantity", "sum"),
                revenue   = ("price",    lambda x: (x * delivered.loc[x.index, "quantity"]).sum()),
                payout    = ("payout",   "sum"),
                commission= ("commission_amount", "sum"),
            )
            .reset_index()
            .sort_values("revenue", ascending=False)
        )

        # Возвраты по артикулам из отменённых постингов
        cancelled = po[po["is_cancelled"]].copy()
        if not cancelled.empty:
            c_prod = (
                cancelled
                .groupby(["offer_id"], dropna=False)
                .agg(cancel_qty=("quantity", "sum"))
                .reset_index()
            )
            prod = prod.merge(c_prod, on="offer_id", how="left")
        else:
            prod["cancel_qty"] = 0
        prod["cancel_qty"] = prod["cancel_qty"].fillna(0).astype(int)

        prod_display = pd.DataFrame({
            "Артикул":           prod["offer_id"].fillna("—"),
            "Продукт":           prod["product_name"].fillna("—"),
            "Продано, шт.":      prod["qty"].astype(int),
            "Отменено, шт.":     prod["cancel_qty"],
            "Выручка, ₽":        prod["revenue"].round(0).astype(int),
            "Начислено Ozon, ₽": prod["payout"].round(0).astype(int),
            "Комиссия Ozon, ₽":  prod["commission"].abs().round(0).astype(int),
        })

        st.dataframe(
            prod_display,
            use_container_width=True, hide_index=True,
            column_config={
                "Выручка, ₽":        st.column_config.NumberColumn(format="%d ₽"),
                "Начислено Ozon, ₽": st.column_config.NumberColumn(format="%d ₽"),
                "Комиссия Ozon, ₽":  st.column_config.NumberColumn(format="%d ₽"),
            }
        )
        st.caption(
            f"Итого: **{int(prod['qty'].sum()):,} шт.** продано · "
            f"**{int(prod['cancel_qty'].sum()):,} шт.** отменено · "
            f"выручка **{prod['revenue'].sum():,.0f} ₽** · "
            f"начислено Ozon **{prod['payout'].sum():,.0f} ₽**"
            .replace(",", " ")
        )
        st.download_button(
            label="⬇️ Скачать таблицу продаж (.xlsx)",
            data=to_excel_bytes(prod_display),
            file_name=f"ozon_продажи_{MONTHS_RU[selected_month].lower()}_{REPORT_YEAR}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        st.info("Нет доставленных заказов за выбранный период.")

    st.markdown("---")

    # ── РАЗДЕЛ 2: Удержания Ozon ──────────────────────────────────────────────
    st.markdown("#### 🔻 Удержания и расходы Ozon")
    d1, d2, d3, d4, d5, d6 = st.columns(6)
    d1.metric("Возвраты покупателям",  f"{m['returns']:,.0f} ₽".replace(",", " "))
    d2.metric("Логистика возвратов",   f"{m['ret_log']:,.0f} ₽".replace(",", " "))
    d3.metric("Эквайринг",             f"{m['acquiring']:,.0f} ₽".replace(",", " "))
    d4.metric("Хранение",              f"{m['storage']:,.0f} ₽".replace(",", " "))
    d5.metric("Реклама Ozon",          f"{m['ads']:,.0f} ₽".replace(",", " "))
    d6.metric("Кросс-докинг / прочее", f"{(m['crossdock'] + m['other_neg']):,.0f} ₽".replace(",", " "))

    st.caption(
        f"Всего удержано Ozon: **{m['total_fees']:,.0f} ₽** · "
        f"Компенсация от Ozon (потери): **{m['ozon_loss']:,.0f} ₽** · "
        f"Чистый payout: **{m['payout']:,.0f} ₽**".replace(",", " ")
    )

    # Детальная таблица по типам операций
    if not tx.empty:
        with st.expander("📋 Детализация операций Ozon"):
            op_detail = (
                tx.groupby("operation_type_name")["amount"]
                .agg(["sum", "count"])
                .reset_index()
                .rename(columns={"operation_type_name": "Тип операции",
                                  "sum": "Сумма, ₽", "count": "Кол-во"})
                .sort_values("Сумма, ₽")
            )
            st.dataframe(
                op_detail,
                use_container_width=True, hide_index=True,
                column_config={"Сумма, ₽": st.column_config.NumberColumn(format="%.0f ₽")},
            )

    st.markdown("---")

    # ── РАЗДЕЛ 3: Налоговая база ─────────────────────────────────────────────
    st.markdown("#### 🧾 Налоговая база (УСН)")

    col_l, col_r = st.columns([1, 1])
    with col_l:
        tax_df = pd.DataFrame({
            "Показатель": [
                "Реализовано покупателям (price × qty)",
                "Минус: возвраты покупателям",
                "= Налоговая база",
                f"Налог УСН {tax_rate*100:.0f}%",
            ],
            "Сумма, ₽": [
                f"{m['revenue']:,.0f}",
                f"−{m['returns']:,.0f}",
                f"{m['tax_base']:,.0f}",
                f"{tax_amount:,.0f}",
            ]
        })
        st.dataframe(tax_df, use_container_width=True, hide_index=True)
        st.caption(
            "**Основание:** Ozon — комиссионер. Налоговая база = "
            "цена реализации покупателю (price из отчёта о реализации) "
            "за вычетом фактических возвратов. Payout — это уже после "
            "вычета комиссии Ozon, которая не уменьшает базу при УСН «доходы» 6%."
        )
        st.download_button(
            label="⬇️ Скачать налоговый расчёт (.xlsx)",
            data=to_excel_bytes(tax_df),
            file_name=f"ozon_налог_{MONTHS_RU[selected_month].lower()}_{REPORT_YEAR}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    with col_r:
        fig = go.Figure(go.Pie(
            labels=["Начислено (payout)", "Логистика возвратов",
                    "Эквайринг", "Хранение", "Реклама", "Кросс-докинг/прочее"],
            values=[
                max(m["payout"], 0), m["ret_log"], m["acquiring"],
                m["storage"], m["ads"], m["crossdock"] + m["other_neg"],
            ],
            hole=0.45,
            textinfo="label+percent",
            marker=dict(colors=["#22c55e", "#ef4444", "#a855f7",
                                 "#3b82f6", "#f59e0b", "#6b7280"]),
        ))
        fig.update_layout(
            title=f"Структура выручки — {MONTHS_RU[selected_month]}",
            height=320,
            margin=dict(t=40, b=0, l=0, r=0),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# ВКЛАДКА 2 — ВЕСЬ ГОД
# ═══════════════════════════════════════════════════════════════════════════════
with tab_year:
    st.markdown(f"### Сводный отчёт Ozon — {REPORT_YEAR} год по месяцам")

    df_tx_year = load_year_transactions(DB_URL, REPORT_YEAR)
    df_po_year = load_year_postings(DB_URL, REPORT_YEAR)

    if df_tx_year.empty and df_po_year.empty:
        st.warning(f"Нет данных за {REPORT_YEAR} год.")
        st.stop()

    # Считаем метрики по каждому доступному месяцу
    monthly_rows = []
    for mo in AVAILABLE_MONTHS:
        tx_m = df_tx_year[df_tx_year["month"] == mo]
        po_m = df_po_year[df_po_year["month"] == mo]
        mm   = calc_month_metrics(tx_m, po_m)
        mm["month"] = mo
        mm["month_name"] = MONTHS_RU[mo]
        mm["tax_amount"] = mm["tax_base"] * tax_rate
        monthly_rows.append(mm)

    monthly = pd.DataFrame(monthly_rows)

    # ── Таблица по месяцам ────────────────────────────────────────────────────
    tax_col = f"Налог УСН {tax_rate*100:.0f}%, ₽"
    display_year = pd.DataFrame({
        "Месяц":              monthly["month_name"],
        "Продано, шт.":       monthly["sold_qty"],
        "Отменено, шт.":      monthly["cancel_qty"],
        "Выручка, ₽":         monthly["revenue"].round(0).astype(int),
        "Начислено Ozon, ₽":  monthly["payout"].round(0).astype(int),
        "Эквайринг, ₽":       monthly["acquiring"].round(0).astype(int),
        "Логист. возвр., ₽":  monthly["ret_log"].round(0).astype(int),
        "Хранение, ₽":        monthly["storage"].round(0).astype(int),
        "Реклама, ₽":         monthly["ads"].round(0).astype(int),
        "Налоговая база, ₽":  monthly["tax_base"].round(0).astype(int),
        tax_col:              monthly["tax_amount"].round(0).astype(int),
    })

    totals = {
        "Месяц":              "ИТОГО",
        "Продано, шт.":       int(monthly["sold_qty"].sum()),
        "Отменено, шт.":      int(monthly["cancel_qty"].sum()),
        "Выручка, ₽":         int(monthly["revenue"].sum()),
        "Начислено Ozon, ₽":  int(monthly["payout"].sum()),
        "Эквайринг, ₽":       int(monthly["acquiring"].sum()),
        "Логист. возвр., ₽":  int(monthly["ret_log"].sum()),
        "Хранение, ₽":        int(monthly["storage"].sum()),
        "Реклама, ₽":         int(monthly["ads"].sum()),
        "Налоговая база, ₽":  int(monthly["tax_base"].sum()),
        tax_col:              int(monthly["tax_amount"].sum()),
    }
    display_year_full = pd.concat(
        [display_year, pd.DataFrame([totals])], ignore_index=True
    )

    col_cfg = {
        "Выручка, ₽":         st.column_config.NumberColumn(format="%d ₽"),
        "Начислено Ozon, ₽":  st.column_config.NumberColumn(format="%d ₽"),
        "Эквайринг, ₽":       st.column_config.NumberColumn(format="%d ₽"),
        "Логист. возвр., ₽":  st.column_config.NumberColumn(format="%d ₽"),
        "Хранение, ₽":        st.column_config.NumberColumn(format="%d ₽"),
        "Реклама, ₽":         st.column_config.NumberColumn(format="%d ₽"),
        "Налоговая база, ₽":  st.column_config.NumberColumn(format="%d ₽"),
        tax_col:              st.column_config.NumberColumn(format="%d ₽"),
    }

    st.dataframe(
        display_year_full, use_container_width=True,
        hide_index=True, column_config=col_cfg,
    )
    st.download_button(
        label="⬇️ Скачать сводный отчёт (.xlsx)",
        data=to_excel_bytes(display_year_full),
        file_name=f"ozon_сводный_{REPORT_YEAR}_{MONTHS_RU_GEN[AVAILABLE_MONTHS[0]]}-{MONTHS_RU_GEN[AVAILABLE_MONTHS[-1]]}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.markdown("---")

    # ── Графики ───────────────────────────────────────────────────────────────
    fig = go.Figure()
    fig.add_bar(x=monthly["month_name"], y=monthly["revenue"],
                name="Выручка", marker_color="#3b82f6")
    fig.add_bar(x=monthly["month_name"], y=monthly["payout"],
                name="Начислено Ozon", marker_color="#22c55e")
    fig.add_bar(x=monthly["month_name"], y=monthly["tax_base"],
                name="Налоговая база", marker_color="#f59e0b")
    fig.update_layout(
        barmode="group",
        title="Выручка / Начислено / Налоговая база по месяцам",
        yaxis_title="₽", height=380,
        legend=dict(orientation="h", y=1.05),
        margin=dict(t=60, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    fig2 = go.Figure()
    fig2.add_scatter(
        x=monthly["month_name"], y=monthly["sold_qty"],
        mode="lines+markers+text", name="Продано",
        text=monthly["sold_qty"].astype(str) + " шт.",
        textposition="top center",
        line=dict(color="#3b82f6", width=2), marker=dict(size=8),
    )
    fig2.add_scatter(
        x=monthly["month_name"], y=monthly["cancel_qty"],
        mode="lines+markers", name="Отменено",
        line=dict(color="#ef4444", width=2, dash="dot"), marker=dict(size=6),
    )
    fig2.update_layout(
        title="Продажи и отмены по месяцам (шт.)",
        yaxis_title="шт.", height=300,
        margin=dict(t=50, b=20),
        legend=dict(orientation="h", y=1.05),
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")

    # ── Продажи по продуктам за год ───────────────────────────────────────────
    st.markdown(f"#### 📦 Продажи по продуктам за {REPORT_YEAR} год")

    yr_del = df_po_year[
        (~df_po_year["is_cancelled"]) &
        (df_po_year["month"].isin(AVAILABLE_MONTHS))
    ]
    if not yr_del.empty:
        yr_prod = (
            yr_del
            .groupby(["offer_id", "product_name"], dropna=False)
            .agg(
                sold_qty  = ("quantity", "sum"),
                revenue   = ("price",    lambda x: (x * yr_del.loc[x.index, "quantity"]).sum()),
                payout    = ("payout",   "sum"),
            )
            .reset_index()
            .sort_values("revenue", ascending=False)
        )
        yr_prod_display = pd.DataFrame({
            "Артикул":           yr_prod["offer_id"].fillna("—"),
            "Продукт":           yr_prod["product_name"].fillna("—"),
            "Продано, шт.":      yr_prod["sold_qty"].astype(int),
            "Выручка, ₽":        yr_prod["revenue"].round(0).astype(int),
            "Начислено Ozon, ₽": yr_prod["payout"].round(0).astype(int),
        })
        st.dataframe(
            yr_prod_display, use_container_width=True, hide_index=True,
            column_config={
                "Выручка, ₽":        st.column_config.NumberColumn(format="%d ₽"),
                "Начислено Ozon, ₽": st.column_config.NumberColumn(format="%d ₽"),
            }
        )
        st.download_button(
            label="⬇️ Скачать по продуктам (.xlsx)",
            data=to_excel_bytes(yr_prod_display),
            file_name=f"ozon_продукты_{REPORT_YEAR}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # ── Итоговые KPI года ─────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown(f"#### 🏁 Итого {REPORT_YEAR} ({RANGE_LABEL})")
    y1, y2, y3, y4, y5 = st.columns(5)
    y1.metric("Продано, шт.",      f"{int(monthly['sold_qty'].sum()):,}".replace(",", " "))
    y2.metric("Выручка",           f"{int(monthly['revenue'].sum()):,} ₽".replace(",", " "))
    y3.metric("Начислено Ozon",    f"{int(monthly['payout'].sum()):,} ₽".replace(",", " "))
    y4.metric("Налоговая база",    f"{int(monthly['tax_base'].sum()):,} ₽".replace(",", " "))
    y5.metric(f"Налог УСН {tax_rate*100:.0f}%",
              f"{int(monthly['tax_amount'].sum()):,} ₽".replace(",", " "))
