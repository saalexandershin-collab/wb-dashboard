"""Диагностика Ozon постингов и транзакций"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text
from src.db.models import init_db, get_session_factory

DB_URL = os.environ["DATABASE_URL"]
engine = init_db(DB_URL)
Session = get_session_factory(engine)

with Session() as session:
    # Примеры постингов — смотрим сырые значения price, payout, quantity
    print("=== 10 примеров Ozon постингов Jan-Apr 2026 ===")
    res = session.execute(text("""
        SELECT posting_number, sku, offer_id, quantity, price, payout, status, is_cancelled,
               created_at::date
        FROM ozon_postings
        WHERE created_at >= '2026-01-01' AND created_at < '2026-05-01'
        ORDER BY created_at DESC LIMIT 10
    """))
    for r in res.fetchall():
        print(f"  pn={str(r[0])[:20]:<20} qty={r[3]} price={r[4]} payout={r[5]} status={r[6]} cancelled={r[7]} dt={r[8]}")

    # Статистика по полю price
    print("\n=== Статистика price в постингах ===")
    res2 = session.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE price IS NULL OR price = 0) as price_zero,
            COUNT(*) FILTER (WHERE price > 0) as price_ok,
            ROUND(AVG(price) FILTER (WHERE price > 0)::numeric, 0) as avg_price,
            ROUND(MIN(price) FILTER (WHERE price > 0)::numeric, 0) as min_price,
            ROUND(MAX(price)::numeric, 0) as max_price
        FROM ozon_postings
        WHERE created_at >= '2026-01-01' AND created_at < '2026-05-01'
          AND is_cancelled = false
    """))
    r = res2.fetchone()
    print(f"  price=0/NULL: {r[0]}, price>0: {r[1]}, avg={r[2]}, min={r[3]}, max={r[4]}")

    # Примеры транзакций
    print("\n=== 5 примеров Ozon транзакций ===")
    res3 = session.execute(text("""
        SELECT operation_type_name, posting_number, quantity, amount, accruals_for_sale,
               sale_commission, operation_date::date
        FROM ozon_transactions
        WHERE operation_date >= '2026-01-01' AND operation_date < '2026-05-01'
        ORDER BY operation_date DESC LIMIT 5
    """))
    for r in res3.fetchall():
        print(f"  type={str(r[0])[:30]:<30} qty={r[2]} amount={r[3]} accruals={r[4]} comm={r[5]} dt={r[6]}")

    # Ozon транзакции по типам
    print("\n=== Транзакции по типам операций ===")
    res4 = session.execute(text("""
        SELECT operation_type_name,
               COUNT(*) as cnt,
               ROUND(SUM(amount)::numeric, 0) as total_amount,
               ROUND(SUM(accruals_for_sale)::numeric, 0) as total_accruals
        FROM ozon_transactions
        WHERE operation_date >= '2026-01-01' AND operation_date < '2026-05-01'
        GROUP BY 1 ORDER BY ABS(SUM(amount)) DESC LIMIT 15
    """))
    for r in res4.fetchall():
        print(f"  {str(r[0])[:45]:<45} cnt={r[1]:>4} amount={float(r[2] or 0):>10,.0f} accruals={float(r[3] or 0):>10,.0f}")
