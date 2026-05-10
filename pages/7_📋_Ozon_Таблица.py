import streamlit as st
import pandas as pd
import calendar
import io
from datetime import date
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

from src.db.models import init_db, get_session_factory
from src.db.repository import OzonPostingRepository

st.title("📋 Детализация по товарам Ozon")

if "database" not in st.secrets:
    st.error("Настройте базу данных в разделе ⚙️ Настройки.")
    st.stop()

DB_URL = st.secrets["database"]["url"]

today = date.today()
col_y, col_m = st.sidebar.columns(2)
year  = col_y.selectbox("Год",   list(range(today.year, today.year - 3, -1)))
month = col_m.selectbox("Месяц", list(range(1, 13)), index=today.month - 1,
                        format_func=lambda m: calendar.month_name[m])

@st.cache_data(ttl=300, show_spinner="Загружаю данные Ozon...")
def load(db_url, year, month):
    engine = init_db(db_url)
    Session = get_session_factory(engine)
    with Session() as session:
        return OzonPostingRepository().get_by_month(session, year, month)

df = load(DB_URL, year, month)

if df.empty:
    st.warning("Нет данных за выбранный период.")
    st.stop()

df["created_at"] = pd.to_datetime(df["created_at"])
df["day"] = df["created_at"].dt.day

# ── Фильтры ───────────────────────────────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.markdown("### Фильтры")

def sidebar_filter(label, series):
    vals = sorted(series.dropna().unique().tolist())
    return st.sidebar.multiselect(label, vals)

sel_offers = sidebar_filter("Артикул (offer_id)", df["offer_id"])
sel_wh     = sidebar_filter("Склад", df["warehouse_name"])

if sel_offers:
    df = df[df["offer_id"].isin(sel_offers)]
if sel_wh:
    df = df[df["warehouse_name"].isin(sel_wh)]

# ── Метрика ───────────────────────────────────────────────────────────────────
st.markdown("### Показатель")
metric = st.radio(
    "Выберите показатель:",
    ["Заказы (шт.)", "Выкупы (шт.)", "Отмены (шт.)", "% выкупа"],
    horizontal=True,
)

days_in_month = calendar.monthrange(year, month)[1]
all_days = list(range(1, days_in_month + 1))

def build_pivot(mask, item_col="offer_id"):
    sub = df[mask]
    if sub.empty:
        return pd.DataFrame()
    piv = sub.groupby([item_col, "day"]).size().reset_index(name="val")
    piv = piv.pivot(index=item_col, columns="day", values="val").reindex(columns=all_days).fillna(0)
    return piv

mask_orders = ~df["is_cancelled"]
mask_sold   = (~df["is_cancelled"]) & (df["status"] == "delivered")
mask_cancel = df["is_cancelled"]

if metric == "Заказы (шт.)":
    piv = build_pivot(mask_orders)
elif metric == "Выкупы (шт.)":
    piv = build_pivot(mask_sold)
elif metric == "Отмены (шт.)":
    piv = build_pivot(mask_cancel)
else:
    o = build_pivot(mask_orders)
    s = build_pivot(mask_sold)
    all_idx = o.index.union(s.index)
    o = o.reindex(all_idx, fill_value=0)
    s = s.reindex(all_idx, fill_value=0)
    piv = (s / o.replace(0, float("nan")) * 100).round(1).fillna(0)

if piv.empty:
    st.info("Нет данных.")
    st.stop()

piv = piv.reset_index()

# Добавляем название товара
name_map = df.drop_duplicates("offer_id").set_index("offer_id")["product_name"]
piv["product_name"] = piv["offer_id"].map(name_map).fillna("")

day_cols = [c for c in piv.columns if isinstance(c, int)]
piv["Итого"] = piv[day_cols].sum(axis=1).round(1)
rename = {d: f"{d:02d}" for d in day_cols}
piv = piv.rename(columns=rename)
str_day_cols = [f"{d:02d}" for d in day_cols]

total_row = {"offer_id": "", "product_name": "ИТОГО"}
for c in str_day_cols:
    total_row[c] = piv[c].sum().round(1)
total_row["Итого"] = piv["Итого"].sum().round(1)
piv = pd.concat([piv, pd.DataFrame([total_row])], ignore_index=True)

display_cols = ["offer_id", "product_name"] + str_day_cols + ["Итого"]
piv = piv[display_cols].rename(columns={"offer_id": "Артикул", "product_name": "Название"})

st.markdown(f"**{metric}** — {calendar.month_name[month]} {year}")
st.dataframe(piv, use_container_width=True, height=500)

# ── Экспорт Excel ─────────────────────────────────────────────────────────────
def to_excel(df_: pd.DataFrame) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = f"{calendar.month_abbr[month]}{year}"
    hfill = PatternFill("solid", fgColor="F97316")
    tfill = PatternFill("solid", fgColor="FED7AA")
    ws.cell(1, 1, f"Ozon — {metric}").font = Font(bold=True, size=12)
    ws.cell(2, 1, f"{calendar.month_name[month]} {year}").font = Font(italic=True)
    for ci, col in enumerate(df_.columns, 1):
        c = ws.cell(4, ci, str(col))
        c.font = Font(bold=True, color="FFFFFF")
        c.fill = hfill
        c.alignment = Alignment(horizontal="center")
    for ri, row in enumerate(df_.itertuples(index=False), 5):
        is_total = str(row[0]) == "ИТОГО"
        for ci, val in enumerate(row, 1):
            cell = ws.cell(ri, ci, val)
            if is_total:
                cell.font = Font(bold=True)
                cell.fill = tfill
    for col in ws.columns:
        mx = max((len(str(c.value or "")) for c in col), default=8)
        ws.column_dimensions[get_column_letter(col[0].column)].width = min(mx + 2, 25)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

excel_data = to_excel(piv)
st.download_button(
    "⬇️ Скачать Excel",
    data=excel_data,
    file_name=f"ozon_{year}_{month:02d}_{metric[:10].replace(' ', '_')}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
