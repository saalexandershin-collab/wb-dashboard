"""
Диагностика и исправление дублей в financial_reports:
  - Синтетические rrd_id (из Excel-импорта): report_id * 100_000 + row_idx → ~14 знаков
  - Реальные rrd_id (из WB API): обычно 8-12 знаков
Оставляем ТОЛЬКО синтетические (Excel) — они содержат все колонки сборов.
Реальные API-записи для Jan-Apr 2026 удаляем.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sqlalchemy import text
from src.db.models import init_db, get_session_factory

DB_URL = os.environ["DATABASE_URL"]
engine = init_db(DB_URL)
Session = get_session_factory(engine)

SYNTHETIC_THRESHOLD = 10_000_000_000_000  # 10^13 — синтетические ID больше этого

with Session() as session:
    # Диагностика: сколько записей каждого типа за Jan-Apr 2026
    res = session.execute(text("""
        SELECT
            CASE WHEN rrd_id >= :thr THEN 'synthetic_excel' ELSE 'real_api' END as source,
            COUNT(*) as cnt,
            MIN(date_from) as min_date,
            MAX(date_from) as max_date,
            ROUND(SUM(retail_price_withdisc_rub * quantity)::numeric, 0) as oborot
        FROM financial_reports
        WHERE platform = 'wb'
          AND date_from >= '2025-12-29'
          AND date_from <= '2026-04-30'
        GROUP BY 1
        ORDER BY 1
    """), {"thr": SYNTHETIC_THRESHOLD})
    rows = res.fetchall()
    print("=== Диагностика БД (WB, Jan-Apr 2026) ===")
    for r in rows:
        print(f"  {r[0]:<20} cnt={r[1]:>7,}  dates={r[2]}..{r[3]}  oborot={float(r[4] or 0):>15,.0f} ₽")

    # Всего записей WB
    total = session.execute(text(
        "SELECT COUNT(*) FROM financial_reports WHERE platform='wb'"
    )).scalar()
    print(f"\nВсего WB записей в БД: {total:,}")

    # Удаляем реальные API-записи для Jan-Apr 2026 (они дублируют Excel)
    del_res = session.execute(text("""
        DELETE FROM financial_reports
        WHERE platform = 'wb'
          AND rrd_id < :thr
          AND date_from >= '2025-12-29'
          AND date_from <= '2026-04-30'
    """), {"thr": SYNTHETIC_THRESHOLD})
    deleted = del_res.rowcount
    session.commit()
    print(f"\nУдалено API-дублей: {deleted:,}")

    # Итог после очистки
    res2 = session.execute(text("""
        SELECT COUNT(*),
               ROUND(SUM(retail_price_withdisc_rub * quantity)::numeric, 0) as oborot,
               ROUND(SUM(ppvz_for_pay)::numeric, 0) as ppvz
        FROM financial_reports
        WHERE platform = 'wb'
          AND date_from >= '2025-12-29'
          AND date_from <= '2026-04-30'
    """))
    r = res2.fetchone()
    print(f"\nПосле очистки — WB Jan-Apr 2026:")
    print(f"  Записей: {r[0]:,}")
    print(f"  Оборот:  {float(r[1] or 0):>15,.0f} ₽")
    print(f"  ppvz:    {float(r[2] or 0):>15,.0f} ₽")
    print("\n✅ Готово!")
