"""
Загружает данные из data/wb_fin_import.json.gz в облачную БД.
Запуск: DATABASE_URL='...' python scripts/import_wb_from_excel.py
"""
import gzip, json, os, sys
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.models import init_db, get_session_factory
from src.db.repository import FinancialReportRepository

DB_URL = os.environ.get("DATABASE_URL", "")
if not DB_URL:
    print("❌ Нужен DATABASE_URL")
    sys.exit(1)

engine = init_db(DB_URL)
Session = get_session_factory(engine)

data_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "wb_fin_import.json.gz")
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
                out[field] = date.fromisoformat(v[:10])
            except Exception:
                out[field] = None
    for field in ("create_dt",):
        v = out.get(field)
        if v and isinstance(v, str):
            try:
                out[field] = datetime.fromisoformat(v[:19])
            except Exception:
                out[field] = None
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

BATCH = 5000
repo = FinancialReportRepository()
total_saved = 0

with Session() as session:
    for i in range(0, len(records), BATCH):
        batch = records[i:i + BATCH]
        saved = repo.upsert_many(session, batch)
        total_saved += saved
        print(f"  Загружено {min(i + BATCH, len(records))}/{len(records)} ({saved} в батче)")

print(f"\n✅ Готово! Загружено в БД: {total_saved} записей")
