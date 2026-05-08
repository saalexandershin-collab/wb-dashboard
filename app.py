import streamlit as st

st.set_page_config(
    page_title="WB Дашборд продаж",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📦 Дашборд продаж Wildberries")
st.markdown(
    """
    Выберите раздел в меню слева:

    - **📊 Дашборд** — KPI за месяц и графики динамики
    - **📋 Таблица** — детализация по товарам и дням, экспорт в Excel
    - **⚙️ Настройки** — токен WB API, ручное обновление данных
    """
)

st.info(
    "Перед первым использованием перейдите в **⚙️ Настройки** "
    "и введите токен WB API, затем нажмите «Загрузить данные».",
    icon="ℹ️",
)
