import os, json
from sqlalchemy import create_engine, text
engine = create_engine(os.environ["DATABASE_URL"])

with engine.connect() as conn:
    # Последние 14 дней заказов CH6 по дням
    r = conn.execute(text("""
        SELECT
            order_date::date as day,
            COUNT(*) FILTER (WHERE is_cancel = false OR is_cancel IS NULL) as orders,
            COUNT(*) FILTER (WHERE is_cancel = true) as cancels
        FROM orders
        WHERE platform = 'wb'
          AND nm_id IN (221075308, 245534038, 245534311, 453244404, 377583082)
          AND order_date >= NOW() - INTERVAL '14 days'
        GROUP BY day
        ORDER BY day
    """))
    rows = [{"day": str(row.day), "orders": row.orders, "cancels": row.cancels} for row in r]

    # Также посмотрим источники заказов (склад, регион) за последние 3 дня
    r2 = conn.execute(text("""
        SELECT
            order_date::date as day,
            warehouse_name,
            region_name,
            COUNT(*) as cnt
        FROM orders
        WHERE platform = 'wb'
          AND nm_id IN (221075308, 245534038, 245534311, 453244404, 377583082)
          AND (is_cancel = false OR is_cancel IS NULL)
          AND order_date >= NOW() - INTERVAL '5 days'
        GROUP BY day, warehouse_name, region_name
        ORDER BY day, cnt DESC
    """))
    geo = [{"day": str(row.day), "warehouse": row.warehouse_name, "region": row.region_name, "cnt": row.cnt} for row in r2]

    # Средняя цена заказа по дням (индикатор акций/скидок)
    r3 = conn.execute(text("""
        SELECT
            order_date::date as day,
            ROUND(AVG(finish_price)::numeric, 0) as avg_price,
            ROUND(AVG(discount_percent)::numeric, 1) as avg_discount
        FROM orders
        WHERE platform = 'wb'
          AND nm_id IN (221075308, 245534038, 245534311, 453244404, 377583082)
          AND (is_cancel = false OR is_cancel IS NULL)
          AND order_date >= NOW() - INTERVAL '14 days'
        GROUP BY day
        ORDER BY day
    """))
    prices = [{"day": str(row.day), "avg_price": float(row.avg_price or 0), "avg_discount": float(row.avg_discount or 0)} for row in r3]

print(json.dumps({"daily": rows, "geo": geo, "prices": prices}, ensure_ascii=False, indent=2))
