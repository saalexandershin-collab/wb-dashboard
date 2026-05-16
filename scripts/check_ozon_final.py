import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text
from src.db.models import init_db, get_session_factory

DB_URL = os.environ["DATABASE_URL"]
engine = init_db(DB_URL)
Session = get_session_factory(engine)

with Session() as session:
    res = session.execute(text("""
        SELECT
            EXTRACT(MONTH FROM created_at)::int as m,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE price IS NOT NULL AND price > 0) as with_price,
            ROUND(AVG(price) FILTER (WHERE price > 0)::numeric, 0) as avg_price,
            ROUND(SUM(price * quantity) FILTER (WHERE is_cancelled = false AND price > 0)::numeric, 0) as oborot,
            ROUND(SUM(payout) FILTER (WHERE is_cancelled = false)::numeric, 0) as total_payout
        FROM ozon_postings
        WHERE created_at >= '2026-01-01' AND created_at < '2026-05-01'
        GROUP BY 1 ORDER BY 1
    """))
    print("=== Ozon постинги после ресинка ===")
    tot_ob = tot_py = 0
    for r in res.fetchall():
        ob = float(r[4] or 0); py = float(r[5] or 0)
        print(f"  Мес {r[0]}: всего={r[1]} with_price={r[2]} avg_price={r[3]} | оборот={ob:>10,.0f} | payout={py:>10,.0f}")
        tot_ob += ob; tot_py += py
    print(f"  ИТОГО:  оборот={tot_ob:>10,.0f} | payout={tot_py:>10,.0f}")
    
    NDS, USN = 5.0, 6.0
    nds = tot_ob * NDS/(100+NDS)
    usn = (tot_ob-nds)*USN/100
    print(f"\n  НДС 5%: {nds:>10,.0f} ₽")
    print(f"  УСН 6%: {usn:>10,.0f} ₽")
    print(f"  Итого:  {nds+usn:>10,.0f} ₽")
    
    # Транзакции для сравнения
    res2 = session.execute(text("""
        SELECT EXTRACT(MONTH FROM operation_date)::int as m,
               ROUND(SUM(amount)::numeric,0) as net_payout
        FROM ozon_transactions
        WHERE operation_date >= '2026-01-01' AND operation_date < '2026-05-01'
        GROUP BY 1 ORDER BY 1
    """))
    print("\n=== Ozon транзакции (начислено на РС) ===")
    for r in res2.fetchall():
        print(f"  Мес {r[0]}: {float(r[1] or 0):>10,.0f} ₽")
