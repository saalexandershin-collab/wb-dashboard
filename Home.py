import streamlit as st
from src.auth import get_authenticator, set_role, get_role, do_logout

st.set_page_config(page_title="WB Дашборд", page_icon="📦", layout="wide")

# Резервируем места вверху сайдбара ДО навигации
_sidebar_user   = st.sidebar.empty()
_sidebar_sep    = st.sidebar.empty()
_sidebar_logout = st.sidebar.empty()

authenticator = get_authenticator()

# stauth 0.3.3: login(location='main') — без названия формы первым аргументом
name, auth_status, username = authenticator.login(location="main")

if auth_status:
    # Получаем username из всех возможных источников (cookie-логин может вернуть None)
    resolved_username = (username
                         or st.session_state.get("username", "")
                         or st.session_state.get("name", ""))
    if resolved_username:
        set_role(resolved_username)
    role = get_role()

    _sidebar_user.markdown(f"👤 **{resolved_username}**")
    _sidebar_sep.markdown("---")
    if _sidebar_logout.button("🚪 Выйти"):
        do_logout(authenticator)
        st.rerun()

    pages = [
        st.Page("pages/1_📊_WB_Дашборд.py",      title="WB Дашборд",      icon="📊"),
        st.Page("pages/2_📋_WB_Таблица.py",      title="WB Таблица",      icon="📋"),
        st.Page("pages/4_🏭_WB_Склады.py",       title="WB Склады",       icon="🏭"),
        st.Page("pages/6_📊_Ozon_Дашборд.py",   title="Ozon Дашборд",    icon="📊"),
        st.Page("pages/7_📋_Ozon_Таблица.py",   title="Ozon Таблица",    icon="📋"),
        st.Page("pages/8_🏭_Ozon_Склады.py",    title="Ozon Склады",     icon="🏭"),
        st.Page("pages/10_🔀_Сводный.py",        title="Сводный отчёт",   icon="🔀"),
        st.Page("pages/3_⚙️_Настройки.py",       title="Настройки",       icon="⚙️"),
    ]
    if role == "admin":
        pages.insert(3,  st.Page("pages/5_💰_WB_Финансы.py",         title="WB Финансы",          icon="💰"))
        pages.insert(7,  st.Page("pages/9_💰_Ozon_Финансы.py",       title="Ozon Финансы",        icon="💰"))
        pages.append(    st.Page("pages/11_🧾_Расчёт_налога.py",     title="Расчёт налога",       icon="🧾"))
        pages.append(    st.Page("pages/12_🧾_Налоги.py",            title="Налоги (год)",        icon="🧾"))
        pages.append(    st.Page("pages/13_🔍_Сверка.py",            title="Сверка / Дебиторка", icon="🔍"))
        pages.append(    st.Page("pages/14_📊_Управленческий.py",    title="Управленческий",     icon="📊"))

    pg = st.navigation(pages)
    pg.run()

elif auth_status is False:
    st.error("Неверный логин или пароль")

# auth_status is None — форма отрисована, ждём ввода
