"""
Сверка: оборот по продажам vs. поступления на РС.

Показывает по каждому месяцу (Jan-Apr 2026 или любой период):
  • Оборот WB  — retail_price_withdisc_rub × qty (что покупатели заплатили)
  • Оборот Ozon — SUM(payout) по не-отменённым постингам
                  payout = реализация на МП (≈ «Отчёт о реализации» Ozon)
                  Ozon показывает payout как «Доход» в финансовом дашборде
  • Начислено к выплате — ppvz_for_pay (net after commissions & fees)
  • Дебиторка — разница: реализовано НА МП, но ещё не перечислено на РС

Методология:
  - Группировка по дате транзакции (create_dt) — «по факту продажи»
  - Возвраты вычитаются из оборота и из ppvz_for_pay
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import calendar
import io
from datetime import date as date_cls
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

from src.db.models import init_db, get_session_factory
from src.db.repository import FinancialReportRepository, OzonPostingRepository, OzonTransactionRepository
from src.auth import require_role

require_role(["admin"])
st.title("🔍 Сверка: реализация vs. выплаты")

if "database" not in st.secrets:
    st.error("Настройте базу данных в разделе ⚙️ Настройки.")
    st.stop()

DB_URL = st.secrets["database"]["url"]

# ── Период ──────────────────────────────────────────────────────────────────
today = date_cls.today()
st.sidebar.markdown("### Период сверки")
col1, col2 = st.sidebar.columns(2)
year_from  = col1.selectbox("С года",   list(range(today.year, today.year - 3, -1)), index=0, key="yf")
month_from = col2.selectbox("С месяца", list(range(1, 13)), index=0, key="mf",
                             format_func=lambda m: calendar.month_abbr[m])
col3, col4 = st.sidebar.columns(2)
year_to  = col3.selectbox("По год",    list(range(today.year, today.year - 3, -1)), index=0, key="yt")
month_to = col4.selectbox("По месяц",  list(range(1, 13)), index=today.month - 1, key="mt",
                           format_func=lambda m: calendar.month_abbr[m])
st.sidebar.markdown("---")
st.sidebar.caption(
    "**По факту продажи** — группировка по дате транзакции (create_dt). "
    "Это то, что реально продано в периоде, вне зависимости от даты перевода денег на РС."
)


# ── Загрузка данных ──────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Загружаю WB...")
def load_wb_months(db_url, yf, mf, yt, mt):
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    repo = FinancialReportRepository()
    frames = []
    y, m = yf, mf
    while (y, m) <= (yt, mt):
        df = repo.get_by_month(Session(), y, m, platform="wb")
        if not df.empty:
            df["_year"]  = y
            df["_month"] = m
            frames.append(df)
        m += 1
        if m > 12:
            m = 1; y += 1
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

@st.cache_data(ttl=300, show_spinner="Загружаю Ozon...")
def load_ozon_months(db_url, yf, mf, yt, mt):
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    repo = OzonPostingRepository()
    frames = []
    y, m = yf, mf
    while (y, m) <= (yt, mt):
        df = repo.get_by_month(Session(), y, m)
        if not df.empty:
            df["_year"]  = y
            df["_month"] = m
            frames.append(df)
        m += 1
        if m > 12:
            m = 1; y += 1
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

@st.cache_data(ttl=300, show_spinner="Загружаю Ozon транзакции...")
def load_ozon_tx_months(db_url, yf, mf, yt, mt):
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    try:
        repo = OzonTransactionRepository()
        frames = []
        y, m = yf, mf
        while (y, m) <= (yt, mt):
            df = repo.get_by_month(Session(), y, m)
            if not df.empty:
                df["_year"]  = y
                df["_month"] = m
                frames.append(df)
            m += 1
            if m > 12:
                m = 1; y += 1
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


wb_raw   = load_wb_months(DB_URL, year_from, month_from, year_to, month_to)
oz_raw   = load_ozon_months(DB_URL, year_from, month_from, year_to, month_to)
oz_tx    = load_ozon_tx_months(DB_URL, year_from, month_from, year_to, month_to)

# ── Расчёт по WB ─────────────────────────────────────────────────────────────
def calc_wb(df):
    if df.empty:
        return pd.DataFrame()
    rows = []
    for (y, m), g in df.groupby(["_year", "_month"]):
        sales   = g[g["doc_type_name"] == "Продажа"]
        returns = g[g["doc_type_name"] == "Возврат"]

        oborot_sales   = (sales["retail_price_withdisc_rub"] * sales["quantity"].abs()).sum()
        oborot_returns = (returns["retail_price_withdisc_rub"] * returns["quantity"].abs()).sum()
        oborot_net     = oborot_sales - oborot_returns

        ppvz_net       = g["ppvz_for_pay"].sum()            # уже со знаком (возвраты отрицательные)

        commission     = g["ppvz_sales_commission"].fillna(0).sum()
        delivery       = g["delivery_rub"].fillna(0).sum()
        storage        = g["storage_fee"].fillna(0).sum()
        penalty        = g["penalty"].fillna(0).sum()
        acquiring      = g["acquiring_fee"].fillna(0).sum()
        total_fees     = abs(commission) + abs(delivery) + abs(storage) + abs(penalty) + abs(acquiring)

        rows.append({
            "year": y, "month": m,
            "label": f"{calendar.month_abbr[m]} {y}",
            "wb_oborot": oborot_net,
            "wb_ppvz":   ppvz_net,
            "wb_commission": abs(commission),
            "wb_delivery":   abs(delivery),
            "wb_storage":    abs(storage),
            "wb_penalty":    abs(penalty),
            "wb_acquiring":  abs(acquiring),
            "wb_fees":       total_fees,
        })
    return pd.DataFrame(rows).sort_values(["year", "month"])


def calc_ozon(postings, transactions):
    rows = []
    # Оборот из постингов: используем payout (= реализация Ozon, «Доход» в финансовом дашборде)
    # payout уже учитывает промо-скидки Ozon; price × qty давало завышенный результат (~10M vs ~5.9M)
    if not postings.empty:
        for (y, m), g in postings.groupby(["_year", "_month"]):
            active = g[~g["is_cancelled"]] if "is_cancelled" in g.columns else g
            if "payout" in active.columns:
                oborot = active["payout"].fillna(0).sum()
            elif "price" in active.columns:
                oborot = (active["price"].fillna(0) * active["quantity"].abs()).sum()
            else:
                oborot = 0
            rows.append({"year": y, "month": m, "oz_oborot": oborot})

    # Выплаты из транзакций
    tx_rows = {}
    if not transactions.empty and "amount" in transactions.columns:
        for (y, m), g in transactions.groupby(["_year", "_month"]):
            tx_rows[(y, m)] = g["amount"].sum()

    # Объединяем
    result = []
    months_set = set()
    if rows:
        for r in rows:
            months_set.add((r["year"], r["month"]))
    if tx_rows:
        for k in tx_rows:
            months_set.add(k)

    for (y, m) in sorted(months_set):
        oz_oborot = next((r["oz_oborot"] for r in rows if r["year"] == y and r["month"] == m), 0)
        oz_ppvz   = tx_rows.get((y, m), 0)
        result.append({
            "year": y, "month": m,
            "label": f"{calendar.month_abbr[m]} {y}",
            "oz_oborot": oz_oborot,
            "oz_ppvz":   oz_ppvz,
        })
    return pd.DataFrame(result).sort_values(["year", "month"]) if result else pd.DataFrame()


df_wb = calc_wb(wb_raw)
df_oz = calc_ozon(oz_raw, oz_tx)

# Объединяем в сводную таблицу
months_all = sorted(set(
    list(zip(df_wb["year"], df_wb["month"])) if not df_wb.empty else [] +
    list(zip(df_oz["year"], df_oz["month"])) if not df_oz.empty else []
))

if not months_all:
    st.warning("Нет данных за выбранный период.")
    st.stop()

summary_rows = []
for (y, m) in months_all:
    wb_row = df_wb[(df_wb["year"] == y) & (df_wb["month"] == m)].iloc[0] if not df_wb.empty and len(df_wb[(df_wb["year"] == y) & (df_wb["month"] == m)]) > 0 else None
    oz_row = df_oz[(df_oz["year"] == y) & (df_oz["month"] == m)].iloc[0] if not df_oz.empty and len(df_oz[(df_oz["year"] == y) & (df_oz["month"] == m)]) > 0 else None

    wb_oborot = wb_row["wb_oborot"] if wb_row is not None else 0
    wb_ppvz   = wb_row["wb_ppvz"]   if wb_row is not None else 0
    wb_fees   = wb_row["wb_fees"]   if wb_row is not None else 0
    oz_oborot = oz_row["oz_oborot"] if oz_row is not None else 0
    oz_ppvz   = oz_row["oz_ppvz"]   if oz_row is not None else 0

    total_oborot = wb_oborot + oz_oborot
    total_ppvz   = wb_ppvz + oz_ppvz
    debitorka    = total_oborot - total_ppvz - wb_fees   # удержания WB уже сняты

    summary_rows.append({
        "Период":          f"{calendar.month_name[m]} {y}",
        "WB оборот":       wb_oborot,
        "WB начислено":    wb_ppvz,
        "WB удержания":    wb_fees,
        "Ozon оборот":     oz_oborot,
        "Ozon начислено":  oz_ppvz,
        "Итого оборот":    total_oborot,
        "Итого начислено": total_ppvz,
        "_debitorka":      max(0, debitorka),
        "_y": y, "_m": m,
    })

df_sum = pd.DataFrame(summary_rows)

# ── Итоговые метрики ──────────────────────────────────────────────────────────
total_oborot  = df_sum["Итого оборот"].sum()
total_ppvz    = df_sum["Итого начислено"].sum()
total_wb_fees = df_sum["WB удержания"].sum()
debitorka_est = df_sum["_debitorka"].sum()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Общий оборот (реализовано)", f"{total_oborot:,.0f} ₽")
col2.metric("Начислено к выплате (net)",  f"{total_ppvz:,.0f} ₽")
col3.metric("Удержания WB",               f"{total_wb_fees:,.0f} ₽")
col4.metric("Расч. дебиторка на МП",      f"{debitorka_est:,.0f} ₽",
            help="Ориентировочно: оборот − удержания − начислено. "
                 "Точная дебиторка = сверка с выписками РС.")

st.markdown("---")

# ── Таблица по месяцам ──────────────────────────────────────────────────────
st.subheader("📋 Помесячная разбивка")

disp = df_sum[[
    "Период", "WB оборот", "WB начислено", "WB удержания",
    "Ozon оборот", "Ozon начислено", "Итого оборот", "Итого начислено"
]].copy()

# Строка итогов
totals = {
    "Период":          "ИТОГО",
    "WB оборот":       df_sum["WB оборот"].sum(),
    "WB начислено":    df_sum["WB начислено"].sum(),
    "WB удержания":    df_sum["WB удержания"].sum(),
    "Ozon оборот":     df_sum["Ozon оборот"].sum(),
    "Ozon начислено":  df_sum["Ozon начислено"].sum(),
    "Итого оборот":    df_sum["Итого оборот"].sum(),
    "Итого начислено": df_sum["Итого начислено"].sum(),
}
disp = pd.concat([disp, pd.DataFrame([totals])], ignore_index=True)

money_cols = [c for c in disp.columns if c != "Период"]
st.dataframe(
    disp.style.format({c: "{:,.0f} ₽" for c in money_cols})
              .apply(lambda row: ["font-weight:bold" if row["Период"] == "ИТОГО" else "" for _ in row], axis=1),
    use_container_width=True, hide_index=True,
)

# ── Детализация удержаний WB ─────────────────────────────────────────────────
if not df_wb.empty:
    st.subheader("🔎 Детализация удержаний WB по месяцам")
    wb_detail = df_wb[["label", "wb_commission", "wb_delivery", "wb_storage", "wb_penalty", "wb_acquiring", "wb_fees"]].copy()
    wb_detail.columns = ["Период", "Комиссия WB", "Логистика", "Хранение", "Штрафы", "Эквайринг", "Итого удержания"]

    total_det = {
        "Период": "ИТОГО",
        **{c: wb_detail[c].sum() for c in wb_detail.columns if c != "Период"}
    }
    wb_detail = pd.concat([wb_detail, pd.DataFrame([total_det])], ignore_index=True)
    st.dataframe(
        wb_detail.style.format({c: "{:,.0f} ₽" for c in wb_detail.columns if c != "Период"})
                       .apply(lambda row: ["font-weight:bold" if row["Период"] == "ИТОГО" else "" for _ in row], axis=1),
        use_container_width=True, hide_index=True,
    )

# ── График: оборот vs. начислено ─────────────────────────────────────────────
st.subheader("📊 Оборот vs. начислено к выплате")
fig = go.Figure()
months_labels = df_sum["Период"].tolist()

fig.add_bar(name="Оборот (реализовано)", x=months_labels,
            y=df_sum["Итого оборот"], marker_color="#4C8BF5")
fig.add_bar(name="Начислено к выплате", x=months_labels,
            y=df_sum["Итого начислено"], marker_color="#34A853")

fig.update_layout(
    barmode="group",
    yaxis_tickformat=",.0f",
    yaxis_title="₽",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    height=400,
)
st.plotly_chart(fig, use_container_width=True)

# ── Пояснение дебиторки ──────────────────────────────────────────────────────
st.markdown("---")
st.subheader("💡 Что такое дебиторка на МП")
st.info(
    """
    **Дебиторка** = деньги, которые WB/Ozon уже получили от покупателей, но ещё не перечислили тебе.

    Возникает из-за лага выплат:
    - **WB** платит раз в неделю, с задержкой ~7-14 дней после закрытия отчётного периода
    - **Ozon** платит дважды в месяц (1-го и 15-го)

    Поэтому продажи последних 2 недель любого месяца, как правило, приходят на РС в следующем месяце.

    **Как уточнить точную сумму:**
    Сравни «Итого начислено» в этой таблице с фактическими поступлениями по банковской выписке за тот же период.
    Разница = дебиторка, которую МП ещё держит у себя.
    """,
    icon="ℹ️"
)

# ── Excel-экспорт ────────────────────────────────────────────────────────────
st.markdown("---")
if st.button("📥 Скачать Excel"):
    wb_xls = Workbook()
    ws = wb_xls.active
    ws.title = "Сверка"

    header_font  = Font(bold=True, color="FFFFFF")
    header_fill  = PatternFill("solid", start_color="1F4E79")
    total_fill   = PatternFill("solid", start_color="D6E4F0")
    center       = Alignment(horizontal="center")
    money_format = '#,##0 "₽";-#,##0 "₽";"-"'

    headers = ["Период", "WB оборот", "WB начислено", "WB удержания",
               "Ozon оборот", "Ozon начислено", "Итого оборот", "Итого начислено"]
    for ci, h in enumerate(headers, 1):
        cell = ws.cell(1, ci, h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center

    for ri, row in enumerate(summary_rows, 2):
        ws.cell(ri, 1, row["Период"])
        vals = [row["WB оборот"], row["WB начислено"], row["WB удержания"],
                row["Ozon оборот"], row["Ozon начислено"],
                row["Итого оборот"], row["Итого начислено"]]
        for ci, v in enumerate(vals, 2):
            c = ws.cell(ri, ci, round(v))
            c.number_format = money_format

    # Итоговая строка
    tr = len(summary_rows) + 2
    ws.cell(tr, 1, "ИТОГО").font = Font(bold=True)
    ws.cell(tr, 1).fill = total_fill
    for ci in range(2, len(headers) + 1):
        col_letter = get_column_letter(ci)
        c = ws.cell(tr, ci, f"=SUM({col_letter}2:{col_letter}{tr-1})")
        c.number_format = money_format
        c.font = Font(bold=True)
        c.fill = total_fill

    ws.column_dimensions["A"].width = 20
    for ci in range(2, len(headers) + 1):
        ws.column_dimensions[get_column_letter(ci)].width = 18

    buf = io.BytesIO()
    wb_xls.save(buf)
    buf.seek(0)
    st.download_button("⬇️ Скачать сверку.xlsx", buf,
                       "сверка_реализация_выплаты.xlsx",
                       "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
