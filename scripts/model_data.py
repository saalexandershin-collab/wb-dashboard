"""Сбор данных для пересчёта полной налоговой модели."""
import psycopg2, os, sys, json
sys.stdout.reconfigure(line_buffering=True)
conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=15)
cur = conn.cursor()

# Сначала узнаём реальные колонки таблицы
print("=== Колонки financial_reports ===")
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'financial_reports'
    ORDER BY ordinal_position
""")
cols = cur.fetchall()
date_cols = []
for c in cols:
    print(f"  {c[0]}: {c[1]}")
    if 'date' in c[0].lower() or 'dt' in c[0].lower() or 'time' in c[1].lower():
        date_cols.append(c[0])

print(f"\nДатовые колонки: {date_cols}")

# Смотрим sample строку для WB
print("\n=== Sample строка financial_reports (WB) ===")
cur.execute("SELECT * FROM financial_reports WHERE platform='wb' LIMIT 1")
row = cur.fetchone()
col_names = [desc[0] for desc in cur.description]
if row:
    for n, v in zip(col_names, row):
        print(f"  {n}: {v}")

print()
print("=== WB данные по месяцам ===")

# Определяем дату реализации: ищем rr_dt, date_to, rrd_id дату
# или используем date_from (начало периода отчёта)
# Попробуем разные варианты
for date_col in date_cols:
    try:
        cur.execute(f"""
            SELECT
                EXTRACT(YEAR  FROM {date_col})::int  AS yr,
                EXTRACT(MONTH FROM {date_col})::int  AS mo,
                SUM(CASE WHEN supplier_oper_name='Продажа' THEN retail_price_withdisc_rub * ABS(quantity) ELSE 0 END) AS oborot_sales,
                SUM(CASE WHEN supplier_oper_name='Возврат'  THEN retail_price_withdisc_rub * ABS(quantity) ELSE 0 END) AS oborot_ret,
                SUM(CASE WHEN supplier_oper_name='Продажа' THEN ABS(quantity) ELSE 0 END) AS sales_qty,
                SUM(CASE WHEN supplier_oper_name='Возврат'  THEN ABS(quantity) ELSE 0 END) AS ret_qty,
                SUM(CASE WHEN supplier_oper_name IN ('Продажа','Возврат') THEN ppvz_for_pay ELSE 0 END) AS ppvz_sales
            FROM financial_reports
            WHERE platform = 'wb'
              AND {date_col} >= '2026-01-01' AND {date_col} < '2026-05-01'
            GROUP BY 1, 2 ORDER BY 1, 2
        """)
        rows = cur.fetchall()
        if rows:
            total_oborot = sum((r[2] or 0)-(r[3] or 0) for r in rows)
            total_ppvz = sum(r[6] or 0 for r in rows)
            print(f"\n  [{date_col}] -> итого оборот={total_oborot:,.0f}, ppvz={total_ppvz:,.0f}")
            for r in rows:
                net = (r[2] or 0) - (r[3] or 0)
                print(f"    {r[0]}-{r[1]:02d}: оборот={net:,.0f} | qty={r[4]}-{r[5]} | ppvz={r[6]:,.0f}")
    except Exception as e:
        print(f"\n  [{date_col}] -> ОШИБКА: {e}")
        conn.rollback()

print()
print("=== Ozon постинги по месяцам (payout = реализация) ===")
cur.execute("""
    SELECT
        EXTRACT(YEAR  FROM created_at)::int AS yr,
        EXTRACT(MONTH FROM created_at)::int AS mo,
        COUNT(DISTINCT posting_number)      AS postings,
        SUM(quantity)                       AS qty,
        ROUND(SUM(payout)::numeric, 0)      AS payout_sum,
        ROUND(SUM(price * quantity)::numeric, 0) AS price_qty
    FROM ozon_postings
    WHERE created_at >= '2026-01-01' AND created_at < '2026-05-01'
      AND is_cancelled = false
    GROUP BY 1, 2 ORDER BY 1, 2
""")
oz_rows = []
for r in cur.fetchall():
    print(f"  {r[0]}-{r[1]:02d}: постингов={r[2]}, qty={r[3]}, payout={r[4]:,} ₽ (price×qty={r[5]:,})")
    oz_rows.append({"yr": int(r[0]), "mo": int(r[1]), "payout": float(r[4])})

print()
print("=== Ozon транзакции по месяцам ===")
cur.execute("""
    SELECT
        EXTRACT(YEAR  FROM operation_date)::int AS yr,
        EXTRACT(MONTH FROM operation_date)::int AS mo,
        ROUND(SUM(amount)::numeric, 0) AS total_amount
    FROM ozon_transactions
    WHERE operation_date >= '2026-01-01' AND operation_date < '2026-05-01'
    GROUP BY 1, 2 ORDER BY 1, 2
""")
for r in cur.fetchall():
    print(f"  {r[0]}-{r[1]:02d}: amount={r[2]:,} ₽")

conn.close()
