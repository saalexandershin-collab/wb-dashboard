"""Сбор данных для пересчёта полной налоговой модели."""
import psycopg2, os, sys, json
sys.stdout.reconfigure(line_buffering=True)
conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=15)
cur = conn.cursor()

print("=== WB оборот по месяцам (retail_price_withdisc_rub × qty) ===")
cur.execute("""
    SELECT
        EXTRACT(YEAR  FROM create_dt)::int  AS yr,
        EXTRACT(MONTH FROM create_dt)::int  AS mo,
        SUM(CASE WHEN supplier_oper_name='Продажа'  THEN retail_price_withdisc_rub * ABS(quantity) ELSE 0 END) AS sales_rub,
        SUM(CASE WHEN supplier_oper_name='Возврат'  THEN retail_price_withdisc_rub * ABS(quantity) ELSE 0 END) AS ret_rub,
        SUM(CASE WHEN supplier_oper_name='Продажа'  THEN ABS(quantity) ELSE 0 END) AS sales_qty,
        SUM(CASE WHEN supplier_oper_name='Возврат'  THEN ABS(quantity) ELSE 0 END) AS ret_qty,
        SUM(ppvz_for_pay) AS ppvz
    FROM financial_reports
    WHERE platform = 'wb'
      AND create_dt >= '2026-01-01' AND create_dt < '2026-05-01'
    GROUP BY 1, 2 ORDER BY 1, 2
""")
wb_rows = []
for r in cur.fetchall():
    netto = r[2] - r[3]
    print(f"  {r[0]}-{r[1]:02d}: продажи={r[2]:,.0f} возвраты={r[3]:,.0f} нетто={netto:,.0f} | qty={r[4]}-{r[5]}={r[4]-r[5]} | ppvz={r[6]:,.0f}")
    wb_rows.append({"yr": r[0], "mo": r[1], "sales": float(r[2]), "returns": float(r[3]), "netto": float(netto),
                    "sales_qty": int(r[4]), "ret_qty": int(r[5]), "ppvz": float(r[6] or 0)})

print()
print("=== Ozon оборот по месяцам (payout = реализация) ===")
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
    oz_rows.append({"yr": int(r[0]), "mo": int(r[1]), "postings": int(r[2]),
                    "qty": int(r[3]), "payout": float(r[4]), "price_qty": float(r[5])})

print()
print("=== Ozon транзакции по месяцам (фактические выплаты) ===")
cur.execute("""
    SELECT
        EXTRACT(YEAR  FROM operation_date)::int AS yr,
        EXTRACT(MONTH FROM operation_date)::int AS mo,
        ROUND(SUM(amount)::numeric, 0) AS total_amount
    FROM ozon_transactions
    WHERE operation_date >= '2026-01-01' AND operation_date < '2026-05-01'
    GROUP BY 1, 2 ORDER BY 1, 2
""")
oz_tx_rows = []
for r in cur.fetchall():
    print(f"  {r[0]}-{r[1]:02d}: amount={r[2]:,} ₽")
    oz_tx_rows.append({"yr": int(r[0]), "mo": int(r[1]), "amount": float(r[2])})

print()
print("=== ИТОГО ===")
wb_total_sales = sum(r["sales"] for r in wb_rows)
wb_total_ret   = sum(r["returns"] for r in wb_rows)
wb_total_netto = sum(r["netto"]  for r in wb_rows)
wb_total_ppvz  = sum(r["ppvz"]  for r in wb_rows)
oz_total_payout = sum(r["payout"] for r in oz_rows)
oz_total_tx     = sum(r["amount"] for r in oz_tx_rows)

print(f"  WB продажи:           {wb_total_sales:>14,.0f} ₽")
print(f"  WB возвраты:          {wb_total_ret:>14,.0f} ₽")
print(f"  WB нетто (база ФНС):  {wb_total_netto:>14,.0f} ₽")
print(f"  WB ppvz:              {wb_total_ppvz:>14,.0f} ₽")
print(f"  Ozon payout:          {oz_total_payout:>14,.0f} ₽  (новая база ФНС)")
print(f"  Ozon транзакции:      {oz_total_tx:>14,.0f} ₽")
print(f"  ИТОГО база ФНС:       {wb_total_netto+oz_total_payout:>14,.0f} ₽")

# JSON для передачи
output = {"wb": wb_rows, "oz": oz_rows, "oz_tx": oz_tx_rows}
print("\n###JSON###")
print(json.dumps(output))
print("###END###")

conn.close()
