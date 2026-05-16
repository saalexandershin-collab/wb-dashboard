import psycopg2
import os
import sys

sys.stdout.reconfigure(line_buffering=True)

conn = psycopg2.connect(os.environ["DATABASE_URL"], connect_timeout=15)
cur = conn.cursor()

print("=== Ozon постинги по месяцам + статус ===")
cur.execute("""
    SELECT
        DATE_TRUNC('month', created_at)::date as month,
        status,
        COUNT(*) as rows,
        COUNT(DISTINCT posting_number) as postings,
        SUM(quantity) as qty,
        ROUND(SUM(price * quantity)::numeric, 0) as oborot,
        ROUND(AVG(price)::numeric, 0) as avg_price,
        COUNT(CASE WHEN price IS NULL OR price = 0 THEN 1 END) as null_price
    FROM ozon_postings
    WHERE created_at >= '2026-01-01' AND created_at < '2026-05-01'
    GROUP BY 1, 2 ORDER BY 1, 2
""")
for r in cur.fetchall():
    print(f"  {r[0]} | {r[1]:<30} | rows={r[2]:>5} | postings={r[3]:>4} | qty={r[4]:>5} | оборот={r[5]:>12,} | avg={r[6]:>6} | null_price={r[7]}")

print()
print("=== Итого БЕЗ отменённых (is_cancelled=false) ===")
cur.execute("""
    SELECT
        DATE_TRUNC('month', created_at)::date as month,
        COUNT(DISTINCT posting_number) as postings,
        SUM(quantity) as qty,
        ROUND(SUM(price * quantity)::numeric, 0) as oborot
    FROM ozon_postings
    WHERE created_at >= '2026-01-01' AND created_at < '2026-05-01'
      AND is_cancelled = false
    GROUP BY 1 ORDER BY 1
""")
total = 0
for r in cur.fetchall():
    total += r[3]
    print(f"  {r[0]}: postings={r[1]}, qty={r[2]}, оборот={r[3]:>12,} ₽")
print(f"  ИТОГО: {total:,} ₽")

print()
print("=== Итого СО всеми (включая отменённые) ===")
cur.execute("""
    SELECT
        DATE_TRUNC('month', created_at)::date as month,
        COUNT(DISTINCT posting_number) as postings,
        SUM(quantity) as qty,
        ROUND(SUM(price * quantity)::numeric, 0) as oborot
    FROM ozon_postings
    WHERE created_at >= '2026-01-01' AND created_at < '2026-05-01'
    GROUP BY 1 ORDER BY 1
""")
total = 0
for r in cur.fetchall():
    total += r[3]
    print(f"  {r[0]}: postings={r[1]}, qty={r[2]}, оборот={r[3]:>12,} ₽")
print(f"  ИТОГО: {total:,} ₽")

print()
print("=== Дубли posting_number в ozon_postings ===")
cur.execute("""
    SELECT COUNT(*) as dups
    FROM (
        SELECT posting_number, sku
        FROM ozon_postings
        WHERE created_at >= '2026-01-01' AND created_at < '2026-05-01'
        GROUP BY posting_number, sku
        HAVING COUNT(*) > 1
    ) t
""")
r = cur.fetchone()
print(f"  Дублей (posting_number, sku): {r[0]}")

print()
print("=== Топ-10 товаров по обороту (не отменённые) ===")
cur.execute("""
    SELECT
        offer_id,
        LEFT(product_name, 35) as name,
        SUM(quantity) as qty,
        ROUND(AVG(price)::numeric, 0) as avg_price,
        ROUND(SUM(price * quantity)::numeric, 0) as oborot
    FROM ozon_postings
    WHERE created_at >= '2026-01-01' AND created_at < '2026-05-01'
      AND is_cancelled = false AND price > 0
    GROUP BY 1, 2 ORDER BY 5 DESC LIMIT 10
""")
for r in cur.fetchall():
    print(f"  {r[0]:<20} | {r[1]:<35} | qty={r[2]:>4} | avg={r[3]:>6} ₽ | оборот={r[4]:>10,} ₽")

print()
print("=== Проверка order_type (FBO vs FBS) ===")
cur.execute("""
    SELECT order_type,
           COUNT(DISTINCT posting_number) as postings,
           ROUND(SUM(price * quantity)::numeric, 0) as oborot
    FROM ozon_postings
    WHERE created_at >= '2026-01-01' AND created_at < '2026-05-01'
      AND is_cancelled = false
    GROUP BY 1
""")
for r in cur.fetchall():
    print(f"  {r[0]}: postings={r[1]}, оборот={r[2]:,} ₽")

conn.close()
print("\nДиагностика завершена.")
