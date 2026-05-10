import streamlit as st
from src.auth import get_authenticator, set_role, get_role, do_logout

st.set_page_config(page_title="WB Дашборд", page_icon="📦", layout="wide")

authenticator = get_authenticator()

# login() проверяет cookie (авто-вход) или показывает форму.
# Возвращает (name, True/False/None, username).
name, auth_status, username = authenticator.login("🔐 Вход в дашборд", "main")

if auth_status:
    # username может прийти из cookie (восстановление сессии)
    # или из формы — в обоих случаях stauth кладёт его в session_state
    resolved_username = username or st.session_state.get("username", "")
    set_role(resolved_username)
    role = get_role()

    pages = [
        # ── Wildberries ──────────────────────────────────────────────────────
        st.Page("pages/1_📊_Дашборд.py",       title="Дашборд WB",      icon="📊"),
        st.Page("pages/2_📋_Таблица.py",       title="Таблица WB",      icon="📋"),
        st.Page("pages/4_🏭_Склады.py",        title="Склады WB",       icon="🏭"),
        # ── Ozon ─────────────────────────────────────────────────────────────
        st.Page("pages/6_📊_Ozon_Дашборд.py", title="Дашборд Ozon",    icon="📊"),
        st.Page("pages/7_📋_Ozon_Таблица.py", title="Таблица Ozon",    icon="📋"),
        st.Page("pages/8_🏭_Ozon_Склады.py",  title="Склады Ozon",     icon="🏭"),
        # ── Сводный ──────────────────────────────────────────────────────────
        st.Page("pages/10_🔀_Сводный.py",     title="Сводный отчёт",   icon="🔀"),
        # ── Прочее ───────────────────────────────────────────────────────────
        st.Page("pages/3_⚙️_Настройки.py",    title="Настройки",       icon="⚙️"),
    ]
    if role == "admin":
        pages.insert(3, st.Page("pages/5_💰_Финансы.py",       title="Финансы WB",   icon="💰"))
        pages.insert(7, st.Page("pages/9_💰_Ozon_Финансы.py",  title="Финансы Ozon", icon="💰"))

    pg = st.navigation(pages)

    st.sidebar.markdown("---")
    st.sidebar.markdown(f"👤 {resolved_username}")

    # Кнопка выхода — в основном потоке скрипта, не в on_click-колбэке.
    # do_logout() удаляет cookie напрямую через stauth, затем чистит session_state.
    if st.sidebar.button("Выйти", type="secondary"):
        do_logout(authenticator)
        st.rerun()

    pg.run()

elif auth_status is False:
    st.error("Неверный логин или пароль")

# auth_status is None — форма уже отрисована, ждём ввода
