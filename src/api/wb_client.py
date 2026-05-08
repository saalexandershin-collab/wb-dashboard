import time
import requests
from datetime import datetime, date
from typing import Optional


BASE_URL = "https://statistics-api.wildberries.ru"
REQUEST_DELAY = 65  # секунд между запросами (лимит WB: 1 req/мин)


class WBApiError(Exception):
    pass


class WBClient:

    def __init__(self, token: str):
        self.token = token
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": token,
            "Content-Type": "application/json",
        })

    def _get(self, path: str, params: dict) -> list[dict]:
        url = f"{BASE_URL}{path}"
        response = self.session.get(url, params=params, timeout=60)
        if response.status_code == 401:
            raise WBApiError("Неверный токен WB API. Проверьте токен в настройках.")
        if response.status_code == 429:
            raise WBApiError("Превышен лимит запросов WB API (1 запрос в минуту).")
        if response.status_code != 200:
            raise WBApiError(
                f"Ошибка WB API {response.status_code}: {response.text[:300]}"
            )
        data = response.json()
        return data if isinstance(data, list) else []

    def get_orders(self, date_from: datetime, flag: int = 1) -> list[dict]:
        params = {
            "dateFrom": date_from.strftime("%Y-%m-%dT%H:%M:%S"),
            "flag": flag,
        }
        return self._get("/api/v1/supplier/orders", params)

    def get_sales(self, date_from: datetime, flag: int = 1) -> list[dict]:
        params = {
            "dateFrom": date_from.strftime("%Y-%m-%dT%H:%M:%S"),
            "flag": flag,
        }
        return self._get("/api/v1/supplier/sales", params)

    def get_orders_and_sales(
        self,
        date_from: datetime,
        on_progress: Optional[callable] = None,
    ) -> tuple[list[dict], list[dict]]:
        if on_progress:
            on_progress("Загружаю заказы из WB API...")
        orders = self.get_orders(date_from)

        if on_progress:
            on_progress(f"Получено заказов: {len(orders)}. Жду 65 сек (лимит API)...")
        time.sleep(REQUEST_DELAY)

        if on_progress:
            on_progress("Загружаю продажи из WB API...")
        sales = self.get_sales(date_from)

        if on_progress:
            on_progress(f"Получено продаж/возвратов: {len(sales)}.")

        return orders, sales


def parse_orders(raw: list[dict], platform: str = "wb") -> list[dict]:
    result = []
    for r in raw:
        order_date = _parse_dt(r.get("date") or r.get("dateCreated"))
        if not order_date:
            continue
        result.append({
            "platform": platform,
            "srid": r.get("srid") or r.get("odid") or str(r.get("nmId", "")) + r.get("date", ""),
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
