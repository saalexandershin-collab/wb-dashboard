import bcrypt
import streamlit as st

# Кэш хэшированных паролей на весь срок жизни процесса.
# bcrypt.hashpw занимает ~0.3с — вычисляется один раз при первом логине.
_credentials_cache: dict | None = None


def _get_credentials() -> dict:
    global _credentials_cache
    if _credentials_cache is not None:
        return _credentials_cache
    users = st.secrets.get("auth", {}).get("users", {})
    creds: dict = {"usernames": {}}
    for username, password in users.items():
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        creds["usernames"][username] = {"name": username, "password": hashed}
    _credentials_cache = creds
    return _credentials_cache


def _get_authenticator():
    import streamlit_authenticator as stauth
    # cookie_key — секретный ключ подписи cookie.
    # Задай [auth] cookie_key = "..." в secrets.toml / Streamlit Cloud secrets.
    cookie_key = st.secrets.get("auth", {}).get("cookie_key", "wb_dashboard_cookie_key_42")
    return stauth.Authenticate(
        _get_credentials(),
        cookie_name="wb_auth",
        key=cookie_key,
        cookie_expiry_days=30,
    )


def require_login():
    """Проверяет авторизацию. Если не авторизован — показывает форму и останавливает выполнение."""
    # Быстрый путь: уже авторизован в этой сессии
    if st.session_state.get("authenticated"):
        return

    authenticator = _get_authenticator()

    # login() сначала проверяет cookie — если валидный, форму не показывает
    # и сразу возвращает (name, True, username)
    name, auth_status, username = authenticator.login("🔐 Вход в дашборд", "main")

    if auth_status:
        roles = st.secrets.get("auth", {}).get("roles", {})
        st.session_state["authenticated"] = True
        st.session_state["username"] = username
        st.session_state["role"] = roles.get(username, "marketer")
        st.rerun()
    elif auth_status is False:
        st.error("Неверный логин или пароль")
        st.stop()
    else:
        st.stop()


def logout():
    """Выход: очищает cookie и session_state."""
    try:
        _get_authenticator().logout("Выйти", "sidebar")
    except Exception:
        pass
    st.session_state.clear()


def get_role() -> str:
    return st.session_state.get("role", "marketer")


def require_role(allowed: list[str]):
    """Останавливает страницу, если роль пользователя не входит в allowed."""
    if get_role() not in allowed:
        st.error("У вас нет доступа к этому разделу.")
        st.stop()
