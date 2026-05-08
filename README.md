# 📦 WB Dashboard — Дашборд продаж Wildberries

Автоматический дашборд продаж WB: заказы, выкупы, возвраты по дням и товарам.

## Возможности

- 📊 KPI за месяц: заказы, выкупы, возвраты, суммы, % выкупа
- 📋 Сводная таблица товары × дни с переключателем показателя
- 📈 Графики динамики по дням, топ товаров, % выкупа
- ⬇️ Экспорт в Excel
- 🔄 Ручное обновление + автообновление через GitHub Actions каждую ночь
- 🔒 Токен WB хранится в Streamlit Secrets, не в коде

## Стек

Streamlit + Supabase (PostgreSQL) + SQLAlchemy + Plotly + GitHub Actions

## Быстрый старт (локально)

```bash
git clone https://github.com/saalexandershin-collab/wb-dashboard.git
cd wb-dashboard
pip install -r requirements.txt
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Отредактируйте secrets.toml: вставьте токен WB и строку Supabase
streamlit run app.py
```
