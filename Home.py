import streamlit as st
from src.auth import get_role, logout

st.set_page_config(page_title="WB Дашборд", page_icon="📦", layout="wide")

if st.session_state.get("authenticated"):
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
    if st.sidebar.button("Выйти"):
        logout()
        st.rerun()

    pg.run()
else:
    from src.auth import require_login
    require_login()
