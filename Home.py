import streamlit as st
from src.auth import get_authenticator, set_role, get_role, do_logout

st.set_page_config(page_title="WB Дашборд", page_icon="📦", layout="wide")

# Кнопку «Выйти» и имя пользователя добавляем В САМОМ НАЧАЛЕ сайдбара,
# ДО st.navigation() — иначе они уходят вниз за видимую область.
_sidebar_user   = st.sidebar.empty()
_sidebar_sep    = st.sidebar.empty()
_sidebar_logout = st.sidebar.empty()

authenticator = get_authenticator()
name, auth_status, username = authenticator.login("🔐 Вход в дашборд", "main")

if auth_status:
    resolved_username = username or st.session_state.get("username", "")
    set_role(resolved_username)
    role = get_role()

    # Заполняем зарезервированные места вверху сайдбара
    _sidebar_user.markdown(f"👤 **{resolved_username}**")
    _sidebar_sep.markdown("---")
    if _sidebar_logout.button("🚪 Выйти"):
        do_logout(authenticator)
        st.rerun()

    pages = [
        st.Page("pages/1_📊_Дашборд.py",       title="Дашборд WB",      icon="📊"),
        st.Page("pages/2_📋_Таблица.py",       title="Таблица WB",      icon="📋"),
        st.Page("pages/4_🏭_Склады.py",        title="Склады WB",       icon="🏭"),
        st.Page("pages/6_📊_Ozon_Дашборд.py", title="Дашборд Ozon",    icon="📊"),
        st.Page("pages/7_📋_Ozon_Таблица.py", title="Таблица Ozon",    icon="📋"),
        st.Page("pages/8_🏭_Ozon_Склады.py",  title="Склады Ozon",     icon="🏭"),
        st.Page("pages/10_🔀_Сводный.py",     title="Сводный отчёт",   icon="🔀"),
        st.Page("pages/3_⚙️_Настройки.py",    title="Настройки",       icon="⚙️"),
    ]
    if role == "admin":
        pages.insert(3, st.Page("pages/5_💰_Финансы.py",       title="Финансы WB",   icon="💰"))
        pages.insert(7, st.Page("pages/9_💰_Ozon_Финансы.py",  title="Финансы Ozon", icon="💰"))

    pg = st.navigation(pages)
    pg.run()

elif auth_status is False:
    st.error("Неверный логин или пароль")
