"""
Загружает данные из data/wb_fin_import.json.gz в облачную БД.
Использует INSERT ... ON CONFLICT DO NOTHING для максимальной скорости.
Запуск: DATABASE_URL='...' python scripts/import_wb_from_excel.py
"""
import gzip, json, os, sys, time
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from src.db.models import init_db, get_session_factory, FinancialReport

DB_URL = os.environ.get("DATABASE_URL", "")
if not DB_URL:
    print("❌ Нужен DATABASE_URL")
    sys.exit(1)

engine = init_db(DB_URL)
Session = get_session_factory(engine)

data_file = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "wb_fin_import.json.gz"
)
print(f"Читаю {data_file}...")
with gzip.open(data_file, "rt", encoding="utf-8") as f:
    records = json.load(f)
print(f"Записей в файле: {len(records)}")


def fix(rec):
    out = dict(rec)
    for field in ("date_from", "date_to"):
        v = out.get(field)
        if v and isinstance(v, str):
            try:
                out[field] = v[:10]          # строка ISO, PostgreSQL примет сам
            except Exception:
                out[field] = None
    for field in ("create_dt",):
        v = out.get(field)
        if v and isinstance(v, str):
            out[field] = v[:19]              # '2026-01-15T12:00:00'
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


records = [fix(r) for r in records]

# Колонки таблицы (без id и created_at — они автоматические)
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
val_list = ", ".join(f":{c}" for c in COLS)

SQL = text(f"""
    INSERT INTO financial_reports ({col_list})
    VALUES ({val_list})
    ON CONFLICT ON CONSTRAINT uq_fin_platform_rrd_id DO NOTHING
""")

BATCH = 2000
total_inserted = 0
t0 = time.time()

with Session() as session:
    for i in range(0, len(records), BATCH):
        batch = records[i:i + BATCH]
        # Оставляем только нужные ключи (лишние игнорируем)
        clean = [{c: r.get(c) for c in COLS} for r in batch]
        result = session.execute(SQL, clean)
        session.commit()
        inserted = result.rowcount if result.rowcount >= 0 else len(batch)
        total_inserted += inserted
        elapsed = time.time() - t0
        done = min(i + BATCH, len(records))
        print(f"  {done}/{len(records)} | вставлено {inserted} | {elapsed:.0f}s")

print(f"\n✅ Готово! Вставлено: {total_inserted} из {len(records)} записей за {time.time()-t0:.0f}s")
