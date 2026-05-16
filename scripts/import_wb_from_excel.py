"""
Загружает данные из data/wb_fin_import.json.gz в облачную БД.
Использует psycopg2 execute_values — самый быстрый способ bulk insert.
Запуск: DATABASE_URL='postgresql://...' python scripts/import_wb_from_excel.py
"""
import gzip, json, os, sys, time
from datetime import date, datetime

# Unbuffered output
sys.stdout.reconfigure(line_buffering=True)

DB_URL = os.environ.get("DATABASE_URL", "")
if not DB_URL:
    print("❌ Нужен DATABASE_URL", flush=True)
    sys.exit(1)

print(f"DB_URL получен (длина {len(DB_URL)})", flush=True)

import psycopg2
import psycopg2.extras

# Подключаемся с коротким таймаутом
conn_str = DB_URL
if "connect_timeout" not in conn_str:
    sep = "&" if "?" in conn_str else "?"
    conn_str = conn_str + sep + "connect_timeout=15"

print("Подключаюсь к БД...", flush=True)
try:
    conn = psycopg2.connect(conn_str, connect_timeout=15)
    conn.autocommit = False
    print("✅ Подключился!", flush=True)
except Exception as e:
    print(f"❌ Ошибка подключения: {e}", flush=True)
    sys.exit(1)

# Читаем данные
data_file = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "wb_fin_import.json.gz"
)
print(f"Читаю {data_file}...", flush=True)
with gzip.open(data_file, "rt", encoding="utf-8") as f:
    records = json.load(f)
print(f"Записей в файле: {len(records)}", flush=True)


def fix(rec):
    out = dict(rec)
    for field in ("date_from", "date_to"):
        v = out.get(field)
        out[field] = v[:10] if v and isinstance(v, str) else None
    for field in ("create_dt",):
        v = out.get(field)
        out[field] = v[:19] if v and isinstance(v, str) else None
    for field in ("nm_id", "quantity", "rrd_id", "realizationreport_id"):
        v = out.get(field)
        if v is not None:
            try:
                out[field] = int(float(v))
            except Exception:
                out[field] = None
    for field in ("retail_price", "retail_price_withdisc_rub", "ppvz_for_pay",
                  "ppvz_sales_commission", "delivery_rub", "penalty",
                  "additional_payment", "storage_fee", "acquiring_fee"):
        v = out.get(field)
        if v is not None:
            try:
                out[field] = float(v)
            except Exception:
                out[field] = None
    return out


COLS = [
    "platform", "rrd_id", "realizationreport_id",
    "date_from", "date_to", "create_dt",
    "nm_id", "supplier_article", "brand_name", "subject_name",
    "doc_type_name", "supplier_oper_name", "quantity",
    "retail_price", "retail_price_withdisc_rub", "ppvz_for_pay",
    "ppvz_sales_commission", "delivery_rub", "penalty",
    "additional_payment", "storage_fee", "acquiring_fee",
]

col_list = ", ".join(COLS)
SQL = f"""
    INSERT INTO financial_reports ({col_list})
    VALUES %s
    ON CONFLICT ON CONSTRAINT uq_fin_platform_rrd_id DO NOTHING
"""

print("Конвертирую записи...", flush=True)
records = [fix(r) for r in records]
# Преобразуем в кортежи в порядке COLS
tuples = [tuple(r.get(c) for c in COLS) for r in records]
print(f"Конвертировано {len(tuples)} записей", flush=True)

BATCH = 5000
total_inserted = 0
t0 = time.time()

cur = conn.cursor()
for i in range(0, len(tuples), BATCH):
    batch = tuples[i:i + BATCH]
    try:
        psycopg2.extras.execute_values(cur, SQL, batch, page_size=BATCH)
        inserted = cur.rowcount if cur.rowcount >= 0 else len(batch)
        conn.commit()
        total_inserted += inserted
        elapsed = time.time() - t0
        done = min(i + BATCH, len(tuples))
        print(f"  {done}/{len(tuples)} | вставлено {inserted} | {elapsed:.0f}s", flush=True)
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка в батче {i}-{i+BATCH}: {e}", flush=True)
        raise

cur.close()
conn.close()
print(f"\n✅ Готово! Вставлено: {total_inserted} из {len(tuples)} за {time.time()-t0:.0f}s", flush=True)
