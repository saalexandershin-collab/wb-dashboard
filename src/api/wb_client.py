import time
import requests
from datetime import datetime
from typing import Optional

BASE_URL = "https://statistics-api.wildberries.ru"
MIN_INTERVAL = 75  # минимум секунд между любыми двумя запросами к WB API
_LOCK_FILE = "/tmp/wb_last_request.txt"  # сохраняем время на диск между перезапусками


class WBApiError(Exception):
    pass


class WBClient:

    def __init__(self, token: str):
        self.token = token
        self.session = requests.Session()
        self.session.headers.update({"Authorization": token})

    def _last_request_time(self) -> float:
        try:
            with open(_LOCK_FILE) as f:
                return float(f.read().strip())
        except Exception:
            return 0.0

    def _save_request_time(self):
        try:
            with open(_LOCK_FILE, "w") as f:
                f.write(str(time.time()))
        except Exception:
            pass

    def _wait_if_needed(self, on_progress=None):
        elapsed = time.time() - self._last_request_time()
        if elapsed < MIN_INTERVAL:
            wait = int(MIN_INTERVAL - elapsed + 2)
            if on_progress:
                on_progress(f"Жду {wait} сек перед следующим запросом (лимит WB API)...")
            time.sleep(wait)

    def _get(self, path: str, params: dict, on_progress=None) -> list[dict]:
        self._wait_if_needed(on_progress=on_progress)
        url = f"{BASE_URL}{path}"
        self._save_request_time()
        response = self.session.get(url, params=params, timeout=60)

        if response.status_code == 401:
            raise WBApiError("Неверный токен WB API. Проверьте токен в настройках.")
        if response.status_code == 429:
            retry_after = int(response.headers.get("X-Ratelimit-Reset", response.headers.get("Retry-After", 90)))
            retry_after = min(retry_after, 120)  # ждём не более 2 минут за раз
            if on_progress:
                on_progress(f"WB API вернул 429 — жду {retry_after} сек и повторяю...")
            time.sleep(retry_after)
            self._save_request_time()
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

    def get_orders(self, date_from: datetime, flag: int = 1, on_progress=None) -> list[dict]:
        return self._get(
            "/api/v1/supplier/orders",
            {"dateFrom": date_from.strftime("%Y-%m-%dT%H:%M:%S"), "flag": flag},
            on_progress=on_progress,
        )

    def get_sales(self, date_from: datetime, flag: int = 1, on_progress=None) -> list[dict]:
        return self._get(
            "/api/v1/supplier/sales",
            {"dateFrom": date_from.strftime("%Y-%m-%dT%H:%M:%S"), "flag": flag},
            on_progress=on_progress,
        )


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
