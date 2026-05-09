import streamlit as st
from src.auth import require_login

st.set_page_config(page_title="WB Дашборд", page_icon="📦", layout="wide")

require_login()

st.title("📦 Wildberries — Дашборд продаж")
st.markdown(
    "Используйте меню слева для навигации между разделами.\n\n"
    "- **📊 Дашборд** — графики и KPI за выбранный месяц\n"
    "- **📋 Таблица** — детализация по товарам и дням\n"
    "- **⚙️ Настройки** — статус синхронизации и история загрузок"
)

st.sidebar.markdown(f"👤 {st.session_state.get('username', '')}")
if st.sidebar.button("Выйти"):
    st.session_state.clear()
    st.rerun()
