import bcrypt
import streamlit as st

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


def get_authenticator():
    import streamlit_authenticator as stauth
    cookie_key = st.secrets.get("auth", {}).get("cookie_key", "wb_dashboard_cookie_42")
    return stauth.Authenticate(
        _get_credentials(),
        cookie_name="wb_auth",
        key=cookie_key,
        cookie_expiry_days=30,
    )


def do_logout(authenticator) -> None:
    """Удаляет cookie и очищает session_state.
    Вызывается в основном потоке скрипта (не внутри on_click-колбэка),
    поэтому stauth успевает удалить cookie до следующего рендера.
    """
    # Пробуем удалить cookie через внутренний хендлер stauth
    # (атрибут называется по-разному в разных версиях 0.3.x)
    for attr in ("cookie_handler", "cookie_manager"):
        handler = getattr(authenticator, attr, None)
        if handler is None:
            continue
        for method in ("delete_cookie", "delete"):
            fn = getattr(handler, method, None)
            if fn is not None:
                try:
                    fn()
                except Exception:
                    pass
                break
        break

    # Очищаем session_state
    st.session_state.clear()


def set_role(username: str) -> None:
    roles = st.secrets.get("auth", {}).get("roles", {})
    st.session_state["role"] = roles.get(username or "", "marketer")


def get_role() -> str:
    return st.session_state.get("role", "marketer")


def require_role(allowed: list[str]) -> None:
    if get_role() not in allowed:
        st.error("У вас нет доступа к этому разделу.")
        st.stop()
