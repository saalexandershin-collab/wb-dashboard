"""Быстрая проверка итогов в БД"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text
from src.db.models import init_db, get_session_factory

DB_URL = os.environ["DATABASE_URL"]
engine = init_db(DB_URL)
Session = get_session_factory(engine)

with Session() as session:
    # WB по месяцам
    res = session.execute(text("""
        SELECT EXTRACT(MONTH FROM date_from)::int as m,
               COUNT(*) as cnt,
               ROUND(SUM(retail_price_withdisc_rub * quantity)::numeric,0) as oborot,
               ROUND(SUM(ppvz_for_pay)::numeric,0) as ppvz
        FROM financial_reports
        WHERE platform='wb' AND date_from>='2025-12-29' AND date_from<='2026-04-30'
        GROUP BY 1 ORDER BY 1
    """))
    print("=== WB Jan-Apr 2026 ===")
    tot_ob = tot_pv = 0
    for r in res.fetchall():
        print(f"  Мес {r[0]}: {r[1]:>6,} строк | оборот {float(r[2] or 0):>13,.0f} | ppvz {float(r[3] or 0):>13,.0f}")
        tot_ob += float(r[2] or 0); tot_pv += float(r[3] or 0)
    print(f"  ИТОГО:        оборот {tot_ob:>13,.0f} | ppvz {tot_pv:>13,.0f}")

    # Ozon постинги
    res2 = session.execute(text("""
        SELECT EXTRACT(MONTH FROM created_at)::int as m,
               COUNT(*) as cnt,
               ROUND(SUM(price * quantity)::numeric,0) as oborot,
               ROUND(SUM(payout * quantity)::numeric,0) as payout
        FROM ozon_postings
        WHERE created_at>='2026-01-01' AND created_at<'2026-05-01'
          AND is_cancelled = false
        GROUP BY 1 ORDER BY 1
    """))
    print("\n=== OZON постинги Jan-Apr 2026 (не отменённые) ===")
    tot2_ob = tot2_pay = 0
    for r in res2.fetchall():
        print(f"  Мес {r[0]}: {r[1]:>6,} строк | оборот {float(r[2] or 0):>13,.0f} | выплата {float(r[3] or 0):>13,.0f}")
        tot2_ob += float(r[2] or 0); tot2_pay += float(r[3] or 0)
    print(f"  ИТОГО:        оборот {tot2_ob:>13,.0f} | выплата {tot2_pay:>13,.0f}")

    # Ozon транзакции
    res3 = session.execute(text("""
        SELECT EXTRACT(MONTH FROM operation_date)::int as m,
               COUNT(*) as cnt,
               ROUND(SUM(amount)::numeric,0) as amount
        FROM ozon_transactions
        WHERE operation_date>='2026-01-01' AND operation_date<'2026-05-01'
        GROUP BY 1 ORDER BY 1
    """))
    print("\n=== OZON транзакции Jan-Apr 2026 ===")
    tot3 = 0
    for r in res3.fetchall():
        print(f"  Мес {r[0]}: {r[1]:>6,} строк | сумма {float(r[2] or 0):>13,.0f}")
        tot3 += float(r[2] or 0)
    print(f"  ИТОГО:        сумма {tot3:>13,.0f}")

    print(f"\n=== ОБЩИЙ ИТОГ Jan-Apr 2026 ===")
    print(f"  WB оборот:    {tot_ob:>13,.0f} ₽")
    print(f"  Ozon оборот:  {tot2_ob:>13,.0f} ₽")
    print(f"  ИТОГО оборот: {tot_ob+tot2_ob:>13,.0f} ₽")
    nds = (tot_ob+tot2_ob)*5/105
    usn = ((tot_ob+tot2_ob)-nds)*6/100
    print(f"  НДС 5%:       {nds:>13,.0f} ₽")
    print(f"  УСН 6%:       {usn:>13,.0f} ₽")
    print(f"  Итого налогов:{nds+usn:>13,.0f} ₽")
