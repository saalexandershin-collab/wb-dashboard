"""
Comprehensive Blogger Analysis v2
- Daily + monthly orders from DB
- Outputs JSON for local analysis
"""
import os, json, sys
from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])

NM_CH6 = [221075308, 245534038, 245534311, 453244404, 377583082]
NM_SPF = [414889490, 414903698, 414902144, 418106362, 414869497, 414911621, 414895483, 414898520]

with engine.connect() as conn:
    # Daily orders by product from Nov 2025
    r = conn.execute(text("""
        SELECT
            order_date::date as day,
            nm_id,
            COUNT(*) as orders
        FROM orders
        WHERE platform = 'wb'
          AND (is_cancel = false OR is_cancel IS NULL)
          AND order_date >= '2025-11-01'
        GROUP BY day, nm_id
        ORDER BY day, nm_id
    """))
    daily = [{"day": str(row.day), "nm_id": row.nm_id, "orders": row.orders} for row in r]

    # Monthly orders by product
    r2 = conn.execute(text("""
        SELECT
            TO_CHAR(order_date, 'YYYY-MM') as month,
            nm_id,
            COUNT(*) as orders
        FROM orders
        WHERE platform = 'wb'
          AND (is_cancel = false OR is_cancel IS NULL)
          AND order_date >= '2025-11-01'
        GROUP BY month, nm_id
        ORDER BY month, nm_id
    """))
    monthly = [{"month": row.month, "nm_id": row.nm_id, "orders": row.orders} for row in r2]

print(json.dumps({"daily": daily, "monthly": monthly}, ensure_ascii=False))
