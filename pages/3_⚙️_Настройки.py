import streamlit as st
import calendar
import pandas as pd
from datetime import date, timezone, timedelta
from sqlalchemy import select

from src.db.models import init_db, get_session_factory, SyncLog
from src.db.repository import SyncLogRepository
from src.auth import require_login

MSK = timezone(timedelta(hours=3))

st.set_page_config(page_title="Настройки", page_icon="⚙️", layout="wide")
require_login()
st.title("⚙️ Настройки")

# ── Статус конфигурации ───────────────────────────────────────────────────────
has_db = "database" in st.secrets and st.secrets["database"].get("url")
has_token = "wildberries" in st.secrets and st.secrets["wildberries"].get("api_token")

st.sidebar.markdown(f"👤 {st.session_state.get('username', '')}")
if st.sidebar.button("Выйти"):
    st.session_state.clear()
    st.rerun()

col_s1, col_s2 = st.columns(2)
col_s1.metric("База данных", "✅ Подключена" if has_db else "❌ Не настроена")
col_s2.metric("Токен WB", "✅ Настроен" if has_token else "❌ Не настроен")

if not (has_db and has_token):
    st.stop()

st.markdown("---")

# ── История загрузок ──────────────────────────────────────────────────────────
st.markdown("### История загрузок")
st.caption("Данные синхронизируются автоматически каждый день в 00:00 МСК.")

DB_URL = st.secrets["database"]["url"]

def load_logs():
    engine = init_db(DB_URL)
    Session = get_session_factory(engine)
    with Session() as session:
        return session.execute(
            select(SyncLog).order_by(SyncLog.started_at.desc()).limit(30)
        ).scalars().all()

def fmt_time(dt):
    if not dt:
        return "—"
    return dt.replace(tzinfo=timezone.utc).astimezone(MSK).strftime("%d.%m.%Y %H:%M")

def fmt_period(log):
    if log.date_from and log.date_to:
        m = log.date_from.month
        y = log.date_from.year
        return f"{calendar.month_name[m]} {y}"
    return "—"

if "deleted_ids" not in st.session_state:
    st.session_state.deleted_ids = set()

logs = load_logs()

if not logs:
    st.info("Загрузок ещё не было.")
else:
    success_logs = [l for l in logs if l.status == "success"]
    error_logs = [l for l in logs if l.status != "success"]

    # ── Успешные загрузки ─────────────────────────────────────────────────────
    if success_logs:
        rows = []
        for l in success_logs:
            rows.append({
                "Период": fmt_period(l),
                "Заказов": l.orders_loaded or 0,
                "Продаж": l.sales_loaded or 0,
                "Время (МСК)": fmt_time(l.finished_at),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # ── Неудачные загрузки ────────────────────────────────────────────────────
    if error_logs:
        with st.expander(f"❌ Неудачные загрузки ({len(error_logs)})", expanded=False):
            log_repo = SyncLogRepository()
            engine = init_db(DB_URL)
            Session = get_session_factory(engine)

            for l in error_logs:
                col_info, col_btn = st.columns([5, 1])
                err_text = l.error_message or l.status
                col_info.markdown(
                    f"**{fmt_period(l)}** · {fmt_time(l.started_at)} · "
                    f"`{err_text[:120]}`"
                )
                if col_btn.button("🗑️ Удалить", key=f"del_{l.id}"):
                    with Session() as session:
                        log_repo.delete_by_id(session, l.id)
                    st.rerun()
