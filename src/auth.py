import streamlit as st


def require_login():
    if st.session_state.get("authenticated"):
        return

    st.markdown(
        "<style>section.main > div {max-width: 400px; margin: 80px auto;}</style>",
        unsafe_allow_html=True,
    )
    st.markdown("## 🔐 Вход в дашборд")

    with st.form("login_form"):
        username = st.text_input("Логин")
        password = st.text_input("Пароль", type="password")
        submitted = st.form_submit_button("Войти", use_container_width=True)

    if submitted:
        users = st.secrets.get("auth", {}).get("users", {})
        if username in users and users[username] == password:
            roles = st.secrets.get("auth", {}).get("roles", {})
            st.session_state["authenticated"] = True
            st.session_state["username"] = username
            st.session_state["role"] = roles.get(username, "marketer")
            st.rerun()
        else:
            st.error("Неверный логин или пароль")

    st.stop()


def get_role() -> str:
    return st.session_state.get("role", "marketer")


def require_role(allowed: list[str]):
    """Останавливает страницу, если роль пользователя не входит в allowed."""
    if get_role() not in allowed:
        st.error("У вас нет доступа к этому разделу.")
        st.stop()
