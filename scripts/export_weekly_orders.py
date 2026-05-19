"""
Экспорт еженедельных заказов WB по артикулам для анализа корреляции с блогерами.
Выводит JSON с данными по неделям.
"""
import os
import json
from datetime import date
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    # Получаем все артикулы и темы чтобы понять продукты
    result = conn.execute(text("""
        SELECT
            supplier_article,
            subject,
            nm_id,
            COUNT(*) as cnt
        FROM wb_orders
        WHERE is_cancel = false OR is_cancel IS NULL
        GROUP BY supplier_article, subject, nm_id
        ORDER BY cnt DESC
        LIMIT 50
    """))
    products = [dict(row._mapping) for row in result]

    print("=== ПРОДУКТЫ (топ-50 по заказам) ===")
    for p in products:
        print(f"  nm_id={p['nm_id']:>12}  cnt={p['cnt']:>5}  article={str(p['supplier_article']):<20}  subject={p['subject']}")

    print("\n=== НЕДЕЛЬНЫЕ ЗАКАЗЫ ПО АРТИКУЛАМ ===")
    # Еженедельные заказы — все артикулы, ноябрь 2025+
    result2 = conn.execute(text("""
        SELECT
            DATE_TRUNC('week', order_date::timestamp) as week_start,
            supplier_article,
            subject,
            nm_id,
            COUNT(*) as orders
        FROM wb_orders
        WHERE (is_cancel = false OR is_cancel IS NULL)
          AND order_date >= '2025-11-01'
        GROUP BY week_start, supplier_article, subject, nm_id
        ORDER BY week_start, orders DESC
    """))
    weekly = [dict(row._mapping) for row in result2]

    for row in weekly:
        row['week_start'] = str(row['week_start'])[:10]

    print(json.dumps(weekly, ensure_ascii=False, indent=2))
