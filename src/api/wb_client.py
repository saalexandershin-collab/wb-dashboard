import time
import requests
from datetime import datetime
from typing import Optional

BASE_URL = "https://statistics-api.wildberries.ru"
FINANCE_URL = "https://statistics-api.wildberries.ru"

# Лимиты WB API (задокументированные и опытным путём):
# /api/v1/supplier/orders  — 1 req/мин
# /api/v1/supplier/sales   — 1 req/мин
# /api/v5/supplier/reportDetailByPeriod — значительно жёстче, ставим 5 мин
MIN_INTERVAL = 90          # секунд между обычными запросами (orders, sales, stocks)
MIN_INTERVAL_FINANCE = 310 # секунд между запросами финансового отчёта (страховой запас 5 мин)

# Единый глобальный файл-таймер: WB ограничивает по аккаунту продавца, не по эндпоинту
_GLOBAL_LOCK_FILE = "/tmp/wb_last_any_request.txt"
_LOCK_FILES = {
    "orders":  "/tmp/wb_last_orders.txt",
    "sales":   "/tmp/wb_last_sales.txt",
    "stocks":  "/tmp/wb_last_stocks.txt",
    "finance": "/tmp/wb_last_finance.txt",
}
_LOCK_FILE = "/tmp/wb_last_request.txt"  # legacy


class WBApiError(Exception):
    pass


class WBClient:

    def __init__(self, token: str):
        self.token = token
        self.session = requests.Session()
        self.session.headers.update({"Authorization": token})

    def _last_global_request_time(self) -> float:
        """Время последнего запроса к WB API (любой эндпоинт)."""
        try:
            with open(_GLOBAL_LOCK_FILE) as f:
                return float(f.read().strip())
        except Exception:
            return 0.0

    def _last_request_time(self, key: str = "default") -> float:
        lock_file = _LOCK_FILES.get(key, _LOCK_FILE)
        try:
            with open(lock_file) as f:
                return float(f.read().strip())
        except Exception:
            return 0.0

    def _save_request_time(self, key: str = "default"):
        now = str(time.time())
        # Сохраняем в глобальный файл (единый таймер для всех эндпоинтов)
        try:
            with open(_GLOBAL_LOCK_FILE, "w") as f:
                f.write(now)
        except Exception:
            pass
        # И в per-endpoint файл (для совместимости)
        lock_file = _LOCK_FILES.get(key, _LOCK_FILE)
        try:
            with open(lock_file, "w") as f:
                f.write(now)
        except Exception:
            pass

    def _wait_if_needed(self, key: str = "default", on_progress=None):
        # Ждём относительно ГЛОБАЛЬНОГО таймера (последний запрос к любому эндпоинту WB).
        # Финансовый отчёт имеет более жёсткий лимит — используем MIN_INTERVAL_FINANCE.
        interval = MIN_INTERVAL_FINANCE if key == "finance" else MIN_INTERVAL
        elapsed = time.time() - self._last_global_request_time()
        if elapsed < interval:
            wait = int(interval - elapsed + 2)
            if on_progress:
                on_progress(f"Жду {wait} сек перед следующим запросом WB API...")
            time.sleep(wait)

    def _get(self, path: str, params: dict, key: str = "default", on_progress=None) -> list[dict]:
        self._wait_if_needed(key=key, on_progress=on_progress)
        url = f"{BASE_URL}{path}"
        self._save_request_time(key)
        response = self.session.get(url, params=params, timeout=60)

        if response.status_code == 401:
            raise WBApiError("Неверный токен WB API. Проверьте токен в настройках.")
        if response.status_code == 429:
            retry_after = int(response.headers.get("X-Ratelimit-Reset", response.headers.get("Retry-After", 90)))
            retry_after = min(retry_after, 120)
            if on_progress:
                on_progress(f"WB API вернул 429 — жду {retry_after} сек и повторяю...")
            time.sleep(retry_after)
            self._save_request_time(key)
            response = self.session.get(url, params=params, timeout=60)
            if response.status_code == 429:
                reset = int(response.headers.get("X-Ratelimit-Reset", 300))
                mins = max(1, round(reset / 60))
                raise WBApiError(
                    f"WB API заблокировал токен на ~{mins} мин. "
                    "Создайте новый токен в личном кабинете WB (Настройки → Доступ к API) "
                    "или подождите и попробуйте позже."
                )
        if response.status_code != 200:
            raise WBApiError(f"Ошибка WB API {response.status_code}: {response.text[:300]}")

        data = response.json()
        return data if isinstance(data, list) else []

    def get_orders(self, date_from: datetime, flag: int = 0, on_progress=None) -> list[dict]:
        return self._get(
            "/api/v1/supplier/orders",
            {"dateFrom": date_from.strftime("%Y-%m-%dT%H:%M:%S"), "flag": flag},
            key="orders",
            on_progress=on_progress,
        )

    def get_stocks(self, date_from: datetime, on_progress=None) -> list[dict]:
        return self._get(
            "/api/v1/supplier/stocks",
            {"dateFrom": date_from.strftime("%Y-%m-%dT%H:%M:%S")},
            key="stocks",
            on_progress=on_progress,
        )

    def get_financial_report(self, date_from: datetime, date_to: datetime, on_progress=None) -> list[dict]:
        """Загружает детализированный финансовый отчёт WB с пагинацией по rrdid."""
        path = "/api/v5/supplier/reportDetailByPeriod"
        params = {
            "dateFrom": date_from.strftime("%Y-%m-%d"),
            "dateTo": date_to.strftime("%Y-%m-%d"),
            "rrdid": 0,
            "limit": 100000,
        }
        all_rows: list[dict] = []
        page = 0
        while True:
            page += 1
            self._wait_if_needed(key="finance", on_progress=on_progress)
            url = f"{BASE_URL}{path}"
            self._save_request_time("finance")
            resp = self.session.get(url, params=params, timeout=120)
            if resp.status_code == 401:
                raise WBApiError("Неверный токен WB API.")
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", resp.headers.get("X-Ratelimit-Reset", 90)))
                retry_after = min(retry_after, 120)
                if on_progress:
                    on_progress(f"WB API 429 — жду {retry_after} сек...")
                time.sleep(retry_after)
                resp = self.session.get(url, params=params, timeout=120)
                if resp.status_code == 429:
                    reset = int(resp.headers.get("X-Ratelimit-Reset", 300))
                    mins = max(1, round(reset / 60))
                    raise WBApiError(
                        f"WB API заблокировал токен на ~{mins} мин. "
                        "Создайте новый токен или подождите."
                    )
            if resp.status_code != 200:
                raise WBApiError(f"Ошибка WB API {resp.status_code}: {resp.text[:300]}")
            rows = resp.json()
            if not rows:
                break
            all_rows.extend(rows)
            if on_progress:
                on_progress(f"  Страница {page}: получено {len(rows)} строк (итого {len(all_rows)})")
            # Пагинация: следующая страница начинается с последнего rrd_id
            params["rrdid"] = rows[-1].get("rrd_id", 0)
            if len(rows) < params["limit"]:
                break
        return all_rows

    def get_sales(self, date_from: datetime, flag: int = 0, on_progress=None) -> list[dict]:
        return self._get(
            "/api/v1/supplier/sales",
            {"dateFrom": date_from.strftime("%Y-%m-%dT%H:%M:%S"), "flag": flag},
            key="sales",
            on_progress=on_progress,
        )


def parse_financial_report(raw: list[dict], platform: str = "wb") -> list[dict]:
    result = []
    for r in raw:
        result.append({
            "platform": platform,
            "rrd_id": r.get("rrd_id"),
            "realizationreport_id": r.get("realizationreport_id"),
            "date_from": _parse_date(r.get("date_from")),
            "date_to": _parse_date(r.get("date_to")),
            "create_dt": _parse_dt(r.get("create_dt")),
            "nm_id": r.get("nm_id"),
            "supplier_article": r.get("sa_name") or r.get("supplier_article"),
            "brand_name": r.get("brand_name"),
            "subject_name": r.get("subject_name"),
            "doc_type_name": r.get("doc_type_name"),
            "supplier_oper_name": r.get("supplier_oper_name"),
            "quantity": _int(r.get("quantity")) or 0,
            "retail_price": _float(r.get("retail_price")),
            "retail_price_withdisc_rub": _float(r.get("retail_price_withdisc_rub")),
            "ppvz_for_pay": _float(r.get("ppvz_for_pay")),
            "ppvz_sales_commission": _float(r.get("ppvz_sales_commission")),
            "delivery_rub": _float(r.get("delivery_rub")),
            "penalty": _float(r.get("penalty")),
            "additional_payment": _float(r.get("additional_payment")),
            "storage_fee": _float(r.get("storage_fee")),
            "acquiring_fee": _float(r.get("acquiring_fee")),
        })
    return result


def _parse_date(value):
    if not value:
        return None
    try:
        from datetime import date
        return date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def parse_stocks(raw: list[dict]) -> list[dict]:
    result = []
    for r in raw:
        result.append({
            "nm_id": r.get("nmId"),
            "supplier_article": r.get("supplierArticle"),
            "barcode": r.get("barcode"),
            "brand": r.get("brand"),
            "subject": r.get("subject"),
            "category": r.get("category"),
            "warehouse_name": r.get("warehouseName"),
            "quantity": int(r.get("quantity") or 0),
            "in_way_to_client": int(r.get("inWayToClient") or 0),
            "in_way_from_client": int(r.get("inWayFromClient") or 0),
            "quantity_full": int(r.get("quantityFull") or 0),
        })
    return result


def parse_orders(raw: list[dict], platform: str = "wb") -> list[dict]:
    result = []
    for r in raw:
        order_date = _parse_dt(r.get("date") or r.get("dateCreated"))
        if not order_date:
            continue
        result.append({
            "platform": platform,
            "srid": r.get("srid") or r.get("odid") or str(r.get("nmId", "")) + str(r.get("date", "")),
            "nm_id": r.get("nmId"),
            "supplier_article": r.get("supplierArticle"),
            "barcode": r.get("barcode"),
            "brand": r.get("brand"),
            "subject": r.get("subject"),
            "category": r.get("category"),
            "warehouse_name": r.get("warehouseName"),
            "region_name": r.get("regionName"),
            "country_name": r.get("countryName"),
            "total_price": _float(r.get("totalPrice")),
            "discount_percent": _int(r.get("discountPercent")),
            "spp": _float(r.get("spp")),
            "finished_price": _float(r.get("finishedPrice")),
            "price_with_disc": _float(r.get("priceWithDisc")),
            "order_date": order_date,
            "last_change_date": _parse_dt(r.get("lastChangeDate")),
            "is_cancel": bool(r.get("isCancel", False)),
            "cancel_dt": _parse_dt(r.get("cancelDt")),
        })
    return result


def parse_sales(raw: list[dict], platform: str = "wb") -> list[dict]:
    result = []
    for r in raw:
        sale_date = _parse_dt(r.get("date") or r.get("dateSale"))
        if not sale_date:
            continue
        sale_id = str(r.get("saleID", ""))
        result.append({
            "platform": platform,
            "sale_id": sale_id,
            "nm_id": r.get("nmId"),
            "supplier_article": r.get("supplierArticle"),
            "barcode": r.get("barcode"),
            "brand": r.get("brand"),
            "subject": r.get("subject"),
            "category": r.get("category"),
            "warehouse_name": r.get("warehouseName"),
            "region_name": r.get("regionName"),
            "country_name": r.get("countryName"),
            "price_with_disc": _float(r.get("priceWithDisc")),
            "finished_price": _float(r.get("finishedPrice")),
            "for_pay": _float(r.get("forPay")),
            "total_price": _float(r.get("totalPrice")),
            "discount_percent": _int(r.get("discountPercent")),
            "spp": _float(r.get("spp")),
            "sale_date": sale_date,
            "last_change_date": _parse_dt(r.get("lastChangeDate")),
            "is_return": sale_id.startswith("R"),
        })
    return result


def _parse_dt(value) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(value)[:19], fmt)
        except ValueError:
            continue
    return None


def _float(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v) -> Optional[int]:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
