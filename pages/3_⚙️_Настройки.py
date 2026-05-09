import streamlit as st
import calendar
from datetime import date, timezone, timedelta

MSK = timezone(timedelta(hours=3))

st.set_page_config(page_title="Настройки", page_icon="⚙️", layout="wide")
st.title("⚙️ Настройки и синхронизация")

# ── Статус конфигурации ───────────────────────────────────────────────────────
has_db = "database" in st.secrets and st.secrets["database"].get("url")
has_token = "wildberries" in st.secrets and st.secrets["wildberries"].get("api_token")

if has_db and has_token:
    st.success("✅ Токен WB и база данных настроены.")
else:
    st.error("❌ Конфигурация не завершена. Следуйте инструкции ниже.")

# ── Инструкция по настройке ───────────────────────────────────────────────────
with st.expander("📖 Как настроить токен и базу данных", expanded=not (has_db and has_token)):
    st.markdown("""
    ### 1. Получите токен Wildberries API

    1. Войдите в [личный кабинет WB](https://seller.wildberries.ru/)
    2. Перейдите: **Настройки → Доступ к API**
    3. Нажмите **«Создать новый токен»**
    4. Введите название (например: `Дашборд`)
    5. Выберите категорию **«Статистика»** → уровень **«Только чтение»**
    6. Нажмите **«Создать токен»** и скопируйте его — он показывается один раз!

    ### 2. Создайте бесплатную базу данных Supabase

    1. Зарегистрируйтесь на [supabase.com](https://supabase.com)
    2. Нажмите **«New project»**
    3. Введите название и пароль (запомните пароль!)
    4. После создания перейдите: **Settings → Database**
    5. Найдите раздел **Connection string → URI** и скопируйте строку
    6. Замените `[YOUR-PASSWORD]` в строке на ваш пароль

    ### 3. Добавьте секреты в Streamlit Cloud

    В вашем приложении на [share.streamlit.io](https://share.streamlit.io):
    1. Откройте **Settings → Secrets**
    2. Вставьте и заполните:

    ```toml
    [database]
    url = "postgresql://postgres.xxxx:PASSWORD@aws-0-eu-central-1.pooler.supabase.com:6543/postgres"

    [wildberries]
    api_token = "ваш_токен_wildberries"
    ```

    ### 4. Для локального запуска

    Создайте файл `.streamlit/secrets.toml` (он уже в .gitignore):
    ```toml
    [database]
    url = "postgresql://..."

    [wildberries]
    api_token = "..."
    ```
    """)

st.markdown("---")

# ── Ручная синхронизация ──────────────────────────────────────────────────────
if not (has_db and has_token):
    st.warning("Настройте токен и базу данных, чтобы загружать данные.")
    st.stop()

st.markdown("### Загрузка данных")

today = date.today()
col1, col2, col3 = st.columns([1, 1, 2])
year = col1.selectbox("Год", list(range(today.year, today.year - 3, -1)))
month = col2.selectbox("Месяц", list(range(1, 13)), index=today.month - 1,
                       format_func=lambda m: calendar.month_name[m])

st.info(
    f"Загрузит все заказы и продажи за **{calendar.month_name[month]} {year}**. "
    "Повторная загрузка не создаёт дублей — данные обновляются.",
    icon="ℹ️"
)

if st.button("🔄 Загрузить данные сейчас", type="primary"):
    DB_URL = st.secrets["database"]["url"]
    WB_TOKEN = st.secrets["wildberries"]["api_token"]

    from src.sync.sync_manager import SyncManager
    from src.api.wb_client import WBApiError

    progress_box = st.empty()
    status_msgs = []

    def on_progress(msg: str):
        status_msgs.append(msg)
        progress_box.info("\n\n".join(status_msgs[-5:]))

    try:
        manager = SyncManager(DB_URL, WB_TOKEN)
        result = manager.sync_month(year, month, on_progress=on_progress)
        st.success(
            f"✅ Готово! Загружено: **{result['orders']}** заказов, "
            f"**{result['sales']}** продаж/возвратов."
        )
        st.cache_data.clear()
    except WBApiError as e:
        st.error(f"Ошибка WB API: {e}")
    except Exception as e:
        st.error(f"Ошибка: {e}")

st.markdown("---")

# ── История синхронизаций ─────────────────────────────────────────────────────
st.markdown("### История загрузок")

try:
    from src.db.models import init_db, get_session_factory
    from src.db.repository import SyncLogRepository
    from sqlalchemy import select
    from src.db.models import SyncLog
    import pandas as pd

    DB_URL = st.secrets["database"]["url"]
    engine = init_db(DB_URL)
    Session = get_session_factory(engine)

    with Session() as session:
        logs = session.execute(
            select(SyncLog).order_by(SyncLog.started_at.desc()).limit(20)
        ).scalars().all()

    if logs:
        rows = []
        for l in logs:
            rows.append({
                "Платформа": l.platform,
                "Период": f"{l.date_from} — {l.date_to}" if l.date_from else "—",
                "Заказов": l.orders_loaded or 0,
                "Продаж": l.sales_loaded or 0,
                "Статус": "✅ Успех" if l.status == "success" else f"❌ {l.status}",
                "Время (МСК)": l.finished_at.replace(tzinfo=timezone.utc).astimezone(MSK).strftime("%d.%m.%Y %H:%M") if l.finished_at else "—",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True)
    else:
        st.info("Загрузок ещё не было.")
except Exception as e:
    st.warning(f"Не удалось загрузить историю: {e}")
