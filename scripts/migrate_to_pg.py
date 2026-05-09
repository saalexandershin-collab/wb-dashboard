"""
Миграция данных из локальной SQLite в PostgreSQL (Supabase).
Запуск: python3 scripts/migrate_to_pg.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from src.db.models import init_db, get_session_factory

SQLITE_URL = "sqlite:////Users/alexander/wb-dashboard/wb_local.db"
PG_URL = "postgresql://postgres.uuhgslolrfytzjrmxwte:umQAcAnEXxUgk5XF@aws-0-eu-west-1.pooler.supabase.com:5432/postgres"

BOOL_COLS = {"is_cancel", "is_return"}

def cast_row(col_names, row):
    d = {}
    for col, val in zip(col_names, row):
        if col in BOOL_COLS and val is not None:
            d[col] = bool(val)
        else:
            d[col] = val
    return d

print("Подключаюсь к SQLite...")
sqlite_engine = init_db(SQLITE_URL)
SqliteSession = get_session_factory(sqlite_engine)

print("Подключаюсь к PostgreSQL и создаю таблицы...")
pg_engine = init_db(PG_URL)
PgSession = get_session_factory(pg_engine)

with SqliteSession() as src, PgSession() as dst:
    dst.execute(text("SET statement_timeout = 0"))
    # Мигрируем заказы
    orders = src.execute(text("SELECT * FROM orders")).fetchall()
    order_cols = list(src.execute(text("SELECT * FROM orders LIMIT 0")).keys())
    print(f"Миграция {len(orders)} заказов...")
    if orders:
        dst.execute(text("TRUNCATE TABLE orders"))
        placeholders = ", ".join(f":{c}" for c in order_cols)
        col_str = ", ".join(order_cols)
        for row in orders:
            dst.execute(text(f"INSERT INTO orders ({col_str}) VALUES ({placeholders})"),
                        cast_row(order_cols, row))

    # Мигрируем продажи
    sales = src.execute(text("SELECT * FROM sales")).fetchall()
    sale_cols = list(src.execute(text("SELECT * FROM sales LIMIT 0")).keys())
    print(f"Миграция {len(sales)} продаж...")
    if sales:
        dst.execute(text("TRUNCATE TABLE sales"))
        placeholders = ", ".join(f":{c}" for c in sale_cols)
        col_str = ", ".join(sale_cols)
        for row in sales:
            dst.execute(text(f"INSERT INTO sales ({col_str}) VALUES ({placeholders})"),
                        cast_row(sale_cols, row))

    # Мигрируем логи синхронизации
    logs = src.execute(text("SELECT * FROM sync_log")).fetchall()
    log_cols = list(src.execute(text("SELECT * FROM sync_log LIMIT 0")).keys())
    print(f"Миграция {len(logs)} логов...")
    if logs:
        dst.execute(text("TRUNCATE TABLE sync_log"))
        placeholders = ", ".join(f":{c}" for c in log_cols)
        col_str = ", ".join(log_cols)
        for row in logs:
            dst.execute(text(f"INSERT INTO sync_log ({col_str}) VALUES ({placeholders})"),
                        cast_row(log_cols, row))

    dst.commit()

print("✅ Миграция завершена!")
