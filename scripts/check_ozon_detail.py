import psycopg2, os, sys
sys.stdout.reconfigure(line_buffering=True)
conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=15)
cur = conn.cursor()

print("=== Детали по CH6 (первые 10 постингов в январе) ===")
cur.execute("""
    SELECT posting_number, quantity, price, payout, commission_amount, old_price, order_type
    FROM ozon_postings
    WHERE created_at >= '2026-01-01' AND created_at < '2026-02-01'
      AND offer_id = 'CH6' AND is_cancelled = false
    ORDER BY created_at LIMIT 10
""")
for r in cur.fetchall():
    print(f"  posting={r[0][:25]}  qty={r[1]}  price={r[2]}  payout={r[3]}  commission={r[4]}  old_price={r[5]}  type={r[6]}")

print()
print("=== Агрегаты по CH6 (январь, доставленные) ===")
cur.execute("""
    SELECT 
        order_type,
        SUM(quantity) as qty,
        ROUND(AVG(price)::numeric,2) as avg_price,
        ROUND(AVG(payout)::numeric,2) as avg_payout,
        ROUND(AVG(commission_amount)::numeric,2) as avg_commission,
        ROUND(AVG(old_price)::numeric,2) as avg_old_price,
        ROUND(SUM(price*quantity)::numeric,0) as total_price,
        ROUND(SUM(payout)::numeric,0) as total_payout,
        COUNT(CASE WHEN payout IS NULL THEN 1 END) as null_payout
    FROM ozon_postings
    WHERE created_at >= '2026-01-01' AND created_at < '2026-02-01'
      AND offer_id = 'CH6' AND is_cancelled = false
    GROUP BY 1
""")
for r in cur.fetchall():
    print(f"  type={r[0]}: qty={r[1]}, avg_price={r[2]}, avg_payout={r[3]}, avg_commission={r[4]}, avg_old_price={r[5]}")
    print(f"         total_price={r[6]:,}, total_payout={r[7]:,}, null_payout={r[8]}")

print()
print("=== Сводка по всем товарам (янв-апр): price vs payout vs commission ===")
cur.execute("""
    SELECT 
        ROUND(SUM(price * quantity)::numeric, 0) as total_price_qty,
        ROUND(SUM(payout)::numeric, 0) as total_payout,
        ROUND(SUM(commission_amount)::numeric, 0) as total_commission,
        COUNT(CASE WHEN payout IS NULL THEN 1 END) as null_payout_cnt,
        COUNT(CASE WHEN commission_amount IS NULL THEN 1 END) as null_commission_cnt
    FROM ozon_postings
    WHERE created_at >= '2026-01-01' AND created_at < '2026-05-01'
      AND is_cancelled = false
""")
r = cur.fetchone()
print(f"  total_price*qty = {r[0]:,} ₽")
print(f"  total_payout    = {r[1]:,} ₽  (null: {r[3]})")
print(f"  total_commission= {r[2]:,} ₽  (null: {r[4]})")
if r[0] and r[2]:
    print(f"  commission_rate = {r[2]/r[0]*100:.1f}%")
    print(f"  check: price - commission = {r[0]-r[2]:,} ₽  (vs реализация из отчёта: 5,704,852 ₽)")

conn.close()
