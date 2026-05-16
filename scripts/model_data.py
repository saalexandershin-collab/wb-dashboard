"""Сбор данных для пересчёта полной налоговой модели."""
import psycopg2, os, sys, json
sys.stdout.reconfigure(line_buffering=True)
conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=15)
cur = conn.cursor()

print("=== WB данные по месяцам ===")
cur.execute("""
    SELECT
        EXTRACT(YEAR  FROM date_from)::int  AS yr,
        EXTRACT(MONTH FROM date_from)::int  AS mo,
        -- База ФНС = retail_price (до скидок) × qty
        SUM(CASE WHEN supplier_oper_name='Продажа' THEN retail_price * ABS(quantity) ELSE 0 END) AS base_sales,
        SUM(CASE WHEN supplier_oper_name='Возврат'  THEN retail_price * ABS(quantity) ELSE 0 END) AS base_ret,
        -- Фактический оборот = retail_price_withdisc_rub × qty
        SUM(CASE WHEN supplier_oper_name='Продажа' THEN retail_price_withdisc_rub * ABS(quantity) ELSE 0 END) AS oborot_sales,
        SUM(CASE WHEN supplier_oper_name='Возврат'  THEN retail_price_withdisc_rub * ABS(quantity) ELSE 0 END) AS oborot_ret,
        SUM(CASE WHEN supplier_oper_name='Продажа' THEN ABS(quantity) ELSE 0 END) AS sales_qty,
        SUM(CASE WHEN supplier_oper_name='Возврат'  THEN ABS(quantity) ELSE 0 END) AS ret_qty,
        SUM(ppvz_for_pay) AS ppvz
    FROM financial_reports
    WHERE platform = 'wb'
      AND date_from >= '2026-01-01' AND date_from < '2026-05-01'
    GROUP BY 1, 2 ORDER BY 1, 2
""")
wb_rows = []
for r in cur.fetchall():
    base_netto = (r[2] or 0) - (r[3] or 0)
    oborot_netto = (r[4] or 0) - (r[5] or 0)
    print(f"  {r[0]}-{r[1]:02d}: база={base_netto:,.0f} | оборот={oborot_netto:,.0f} | qty={r[6]}-{r[7]}={r[6]-r[7]} | ppvz={r[8]:,.0f}")
    wb_rows.append({"yr": int(r[0]), "mo": int(r[1]),
                    "base_sales": float(r[2] or 0), "base_ret": float(r[3] or 0), "base": float(base_netto),
                    "oborot": float(oborot_netto),
                    "sales_qty": int(r[6]), "ret_qty": int(r[7]), "ppvz": float(r[8] or 0)})

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
wb_total_base   = sum(r["base"]   for r in wb_rows)
wb_total_oborot = sum(r["oborot"] for r in wb_rows)
wb_total_ppvz   = sum(r["ppvz"]  for r in wb_rows)
oz_total_payout = sum(r["payout"] for r in oz_rows)
oz_total_tx     = sum(r["amount"] for r in oz_tx_rows)
total_base = wb_total_base + oz_total_payout

NDS_RATE = 5 / 105
USN_RATE = 0.06
nds  = total_base * NDS_RATE
usn  = (total_base - nds) * USN_RATE
total_tax = nds + usn

print(f"  WB база ФНС (retail×qty):   {wb_total_base:>14,.0f} ₽")
print(f"  WB оборот (с учётом скидок):{wb_total_oborot:>14,.0f} ₽")
print(f"  WB ppvz:                     {wb_total_ppvz:>14,.0f} ₽")
print(f"  Ozon payout (реализация):    {oz_total_payout:>14,.0f} ₽")
print(f"  Ozon транзакции (ppvz):      {oz_total_tx:>14,.0f} ₽")
print(f"  ─────────────────────────────────────────")
print(f"  ИТОГО база ФНС:              {total_base:>14,.0f} ₽")
print(f"  НДС 5% (изнутри):            {nds:>14,.0f} ₽")
print(f"  УСН 6% на (база−НДС):        {usn:>14,.0f} ₽")
print(f"  ИТОГО НАЛОГОВ:               {total_tax:>14,.0f} ₽")

# JSON для передачи
output = {"wb": wb_rows, "oz": oz_rows, "oz_tx": oz_tx_rows,
          "totals": {"wb_base": wb_total_base, "wb_oborot": wb_total_oborot,
                     "wb_ppvz": wb_total_ppvz, "oz_payout": oz_total_payout,
                     "oz_tx": oz_total_tx, "total_base": total_base,
                     "nds": nds, "usn": usn, "total_tax": total_tax}}
print("\n###JSON###")
print(json.dumps(output))
print("###END###")

conn.close()
