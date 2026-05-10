import time
import requests
from datetime import datetime
from typing import Optional

BASE_URL = "https://api-seller.ozon.ru"


class OzonApiError(Exception):
    pass


class OzonClient:

    def __init__(self, client_id: str, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Client-Id": str(client_id),
            "Api-Key": api_key,
            "Content-Type": "application/json",
        })

    def _post(self, path: str, body: dict, on_progress=None) -> dict:
        url = f"{BASE_URL}{path}"
        resp = self.session.post(url, json=body, timeout=60)
        if resp.status_code == 401:
            raise OzonApiError("Неверный Client-Id или Api-Key Ozon.")
        if resp.status_code == 429:
            retry = 30
            if on_progress:
                on_progress(f"Ozon API 429 — жду {retry} сек...")
            time.sleep(retry)
            resp = self.session.post(url, json=body, timeout=60)
        if resp.status_code != 200:
            raise OzonApiError(f"Ошибка Ozon API {resp.status_code}: {resp.text[:300]}")
        return resp.json()

    # ── Заказы FBO ────────────────────────────────────────────────────────────
    def get_postings_fbo(self, date_from: datetime, date_to: datetime,
                         on_progress=None) -> list[dict]:
        results = []
        offset = 0
        limit = 1000
        while True:
            body = {
                "dir": "asc",
                "filter": {
                    "since": date_from.strftime("%Y-%m-%dT00:00:00.000Z"),
                    "to":    date_to.strftime("%Y-%m-%dT23:59:59.000Z"),
                    "status": "",
                },
                "limit": limit,
                "offset": offset,
                "with": {"analytics_data": True, "financial_data": True},
            }
            data = self._post("/v3/posting/fbo/list", body, on_progress)
            rows = data.get("result", {}).get("postings", [])
            results.extend(rows)
            if on_progress:
                on_progress(f"  FBO offset={offset}: получено {len(rows)} (итого {len(results)})")
            if len(rows) < limit:
                break
            offset += limit
        return results

    # ── Заказы FBS ────────────────────────────────────────────────────────────
    def get_postings_fbs(self, date_from: datetime, date_to: datetime,
                         on_progress=None) -> list[dict]:
        results = []
        offset = 0
        limit = 1000
        while True:
            body = {
                "dir": "asc",
                "filter": {
                    "since": date_from.strftime("%Y-%m-%dT00:00:00.000Z"),
                    "to":    date_to.strftime("%Y-%m-%dT23:59:59.000Z"),
                    "status": "",
                },
                "limit": limit,
                "offset": offset,
                "with": {"analytics_data": True, "financial_data": True, "barcodes": False},
            }
            data = self._post("/v3/posting/fbs/list", body, on_progress)
            rows = data.get("result", {}).get("postings", [])
            results.extend(rows)
            if on_progress:
                on_progress(f"  FBS offset={offset}: получено {len(rows)} (итого {len(results)})")
            if len(rows) < limit:
                break
            offset += limit
        return results

    # ── Остатки ───────────────────────────────────────────────────────────────
    def get_stocks(self, on_progress=None) -> list[dict]:
        results = []
        offset = 0
        limit = 1000
        while True:
            body = {"limit": limit, "offset": offset, "warehouse_type": "ALL"}
            data = self._post("/v2/analytics/stock_on_warehouses", body, on_progress)
            rows = data.get("result", {}).get("rows", [])
            results.extend(rows)
            if on_progress:
                on_progress(f"  Stocks offset={offset}: получено {len(rows)}")
            if len(rows) < limit:
                break
            offset += limit
        return results

    # ── Финансовые транзакции ─────────────────────────────────────────────────
    def get_transactions(self, date_from: datetime, date_to: datetime,
                         on_progress=None) -> list[dict]:
        results = []
        page = 1
        page_size = 1000
        while True:
            body = {
                "filter": {
                    "date": {
                        "from": date_from.strftime("%Y-%m-%dT00:00:00.000Z"),
                        "to":   date_to.strftime("%Y-%m-%dT23:59:59.000Z"),
                    },
                    "operation_type": [],
                    "posting_number": "",
                    "transaction_type": "all",
                },
                "page": page,
                "page_size": page_size,
            }
            data = self._post("/v3/finance/transaction/list", body, on_progress)
            result = data.get("result", {})
            rows = result.get("operations", [])
            results.extend(rows)
            if on_progress:
                on_progress(f"  Transactions page={page}: получено {len(rows)} (итого {len(results)})")
            if len(rows) < page_size:
                break
            page += 1
        return results


# ── Парсеры ───────────────────────────────────────────────────────────────────

def parse_postings(raw: list[dict], order_type: str) -> list[dict]:
    """Разворачивает список постингов в строки (одна строка = один товар)."""
    rows = []
    for p in raw:
        posting_number = p.get("posting_number", "")
        status = p.get("status", "")
        is_cancelled = status in ("cancelled", "cancelled_seller", "cancelled_user")
        created_at = _parse_dt(p.get("created_at") or p.get("in_process_at"))
        in_process_at = _parse_dt(p.get("in_process_at"))
        shipment_date = _parse_dt(p.get("shipment_date"))

        analytics = p.get("analytics_data") or {}
        warehouse_name = analytics.get("warehouse_name") or p.get("warehouse", {}).get("name", "")
        region = analytics.get("region") or analytics.get("city") or ""

        fin_products = {}
        fin_data = p.get("financial_data") or {}
        for fp in fin_data.get("products", []):
            sku = str(fp.get("client_price") or "")
            # keyed by sku
            fin_products[str(fp.get("product_id") or "")] = fp

        for item in p.get("products", []):
            sku = str(item.get("sku") or "")
            fp = fin_products.get(sku, {})
            rows.append({
                "posting_number": posting_number,
                "order_id": str(p.get("order_id") or ""),
                "order_number": str(p.get("order_number") or ""),
                "sku": _int(sku),
                "offer_id": item.get("offer_id") or fp.get("offer_id") or "",
                "product_name": item.get("name") or "",
                "quantity": _int(item.get("quantity")) or 0,
                "price": _float(item.get("price")),
                "total_discount_value": _float(item.get("total_discount_value")),
                "commission_amount": _float(fp.get("commission_amount")),
                "payout": _float(fp.get("payout")),
                "old_price": _float(fp.get("old_price")),
                "warehouse_name": warehouse_name,
                "region": region,
                "order_type": order_type,
                "status": status,
                "is_cancelled": is_cancelled,
                "created_at": created_at,
                "in_process_at": in_process_at,
                "shipment_date": shipment_date,
            })
    return rows


def parse_stocks(raw: list[dict]) -> list[dict]:
    rows = []
    for r in raw:
        rows.append({
            "sku": _int(r.get("sku")),
            "offer_id": r.get("offer_id") or r.get("item_code") or "",
            "product_name": r.get("item_name") or "",
            "warehouse_name": r.get("warehouse_name") or "",
            "free_to_sell_amount": _int(r.get("free_to_sell_amount")) or 0,
            "promised_amount": _int(r.get("promised_amount")) or 0,
            "reserved_amount": _int(r.get("reserved_amount")) or 0,
        })
    return rows


def parse_transactions(raw: list[dict]) -> list[dict]:
    rows = []
    for op in raw:
        posting = op.get("posting") or {}
        operation_date = _parse_dt(op.get("operation_date"))
        period_from = _parse_dt(op.get("period", {}).get("begin"))
        period_to = _parse_dt(op.get("period", {}).get("end"))

        # Разбиваем сервисные комиссии по типам
        services = {s.get("name"): _float(s.get("price")) for s in (op.get("services") or [])}

        items = op.get("items") or [{}]
        for item in items:
            rows.append({
                "operation_id": str(op.get("operation_id") or ""),
                "operation_date": operation_date,
                "operation_type": op.get("operation_type") or "",
                "operation_type_name": op.get("operation_type_name") or "",
                "posting_number": posting.get("posting_number") or "",
                "order_id": str(posting.get("order_id") or ""),
                "sku": _int(item.get("sku")),
                "offer_id": item.get("offer_id") or "",
                "product_name": item.get("name") or "",
                "quantity": _int(item.get("quantity")) or 0,
                "amount": _float(op.get("amount")) or 0.0,
                "accruals_for_sale": _float(services.get("MarketplaceServiceItemFeeRevShare")),
                "sale_commission": _float(services.get("MarketplaceServiceItemReturnAfterDeliveryDesc")),
                "delivery_charge": _float(services.get("MarketplaceServiceItemDelivToCustomer")),
                "return_delivery_charge": _float(services.get("MarketplaceServiceItemReturnFlowLogistic")),
                "period_from": period_from,
                "period_to": period_to,
            })
    return rows


def _parse_dt(value) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    s = str(value)[:19].replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
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
