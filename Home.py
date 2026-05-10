import streamlit as st
from src.auth import get_authenticator, set_role, get_role

st.set_page_config(page_title="WB Дашборд", page_icon="📦", layout="wide")

# get_authenticator() проверяет cookie и рендерит форму если нужно.
# Возвращает (name, True/False/None, username).
# True  — вошёл (из формы или cookie)
# False — неверный пароль
# None  — форма показана, ждём ввода
authenticator = get_authenticator()
name, auth_status, username = authenticator.login("🔐 Вход в дашборд", "main")

if auth_status:
    # Устанавливаем роль на каждом запросе — username всегда актуален из stauth
    set_role(username or st.session_state.get("username", ""))
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
    st.sidebar.markdown(f"👤 {st.session_state.get('username', '')}")
    # logout() рендерит кнопку «Выйти» и обрабатывает клик сам —
    # включая удаление cookie. Нельзя вызывать внутри другого button-колбэка.
    authenticator.logout("Выйти", "sidebar")

    pg.run()

elif auth_status is False:
    st.error("Неверный логин или пароль")

# Если auth_status is None — форма уже показана, ждём ввода, ничего не делаем
