import streamlit as st
from src.auth import get_role

st.set_page_config(page_title="WB Дашборд", page_icon="📦", layout="wide")

if st.session_state.get("authenticated"):
    role = get_role()
    pages = [
        st.Page("pages/1_📊_Дашборд.py",   title="Дашборд",   icon="📊"),
        st.Page("pages/2_📋_Таблица.py",   title="Таблица",   icon="📋"),
        st.Page("pages/3_⚙️_Настройки.py", title="Настройки", icon="⚙️"),
        st.Page("pages/4_🏭_Склады.py",    title="Склады",    icon="🏭"),
    ]
    if role == "admin":
        pages.append(st.Page("pages/5_💰_Финансы.py", title="Финансы", icon="💰"))

    pg = st.navigation(pages)

    st.sidebar.markdown("---")
    st.sidebar.markdown(f"👤 {st.session_state.get('username', '')}")
    if st.sidebar.button("Выйти"):
        st.session_state.clear()
        st.rerun()

    pg.run()
else:
    from src.auth import require_login
    require_login()
