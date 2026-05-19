import time
import requests
from datetime import datetime
from typing import Optional

BASE_URL    = "https://statistics-api.wildberries.ru"
FINANCE_URL = "https://statistics-api.wildberries.ru"

# Лимиты WB API (1 req/min на эндпоинт, общий аккаунтный лимит):
MIN_INTERVAL         = 90   # секунд — orders / sales / stocks
MIN_INTERVAL_FINANCE = 310  # секунд — reportDetailByPeriod (5 мин с запасом)

# Fallback-файл для локальной разработки (когда нет DB rate limiter)
_FALLBACK_FILE = "/tmp/wb_last_any_request.txt"


class WBApiError(Exception):
    pass


class WBClient:

    def __init__(self, token: str, rate_limiter=None):
        """
        token        — WB Statistics API token
        rate_limiter — экземпляр DBRateLimiter (передаётся из sync-скриптов).
                       Если None — используется файловый fallback (только локально).
        """
        self.token        = token
        self.rate_limiter = rate_limiter
        self.session      = requests.Session()
        self.session.headers.update({"Authorization": token})

    # ── rate limiting ────────────────────────────────────────────────────────

    def _last_request_time_file(self) -> float:
        try:
            with open(_FALLBACK_FILE) as f:
                return float(f.read().strip())
        except Exception:
            return 0.0

    def _save_request_time_file(self):
        try:
            with open(_FALLBACK_FILE, "w") as f:
                f.write(str(time.time()))
        except Exception:
            pass

    def _wait_if_needed(self, key: str = "default", on_progress=None):
        if self.rate_limiter is not None:
            self.rate_limiter.wait_if_needed(key=key, on_progress=on_progress)
        else:
            # Fallback: файловый таймер (работает только в рамках одного runner'а)
            interval = MIN_INTERVAL_FINANCE if key == "finance" else MIN_INTERVAL
            elapsed  = time.time() - self._last_request_time_file()
            if elapsed < interval:
                wait = int(interval - elapsed + 2)
                msg  = f"Пауза {wait} сек (rate limit)..."
                print(f"⏱  {msg}") if not on_progress else on_progress(msg)
                time.sleep(wait)

    def _record_request(self, key: str = "default"):
        if self.rate_limiter is not None:
            self.rate_limiter.record_request()
        else:
            self._save_request_time_file()

    def _record_block(self, seconds: float):
        if self.rate_limiter is not None:
            self.rate_limiter.record_block(seconds)

    # ── HTTP ─────────────────────────────────────────────────────────────────

    def _get(self, path: str, params: dict, key: str = "default",
             on_progress=None) -> list[dict]:
        self._wait_if_needed(key=key, on_progress=on_progress)

        url = f"{BASE_URL}{path}"
        self._record_request(key)
        response = self.session.get(url, params=params, timeout=60)

        if response.status_code == 401:
            raise WBApiError("Неверный токен WB API. Проверьте токен в настройках.")

        if response.status_code == 429:
            # Берём реальное Retry-After БЕЗ искусственного ограничения сверху
            retry_after = int(
                response.headers.get("X-Ratelimit-Retry")
                or response.headers.get("Retry-After")
                or response.headers.get("X-Ratelimit-Reset")
                or 120
            )
            self._record_block(retry_after)
            msg = (f"WB API вернул 429 — токен заблокирован на {retry_after} сек "
                   f"(до {datetime.utcfromtimestamp(time.time() + retry_after).strftime('%H:%M:%S')} UTC). "
                   "Прерываю синхронизацию.")
            if on_progress:
                on_progress(msg)
            else:
                print(f"🚫 {msg}")
            raise WBApiError(msg)

        if response.status_code != 200:
            raise WBApiError(
                f"Ошибка WB API {response.status_code}: {response.text[:300]}"
            )

        data = response.json()
        return data if isinstance(data, list) else []

    # ── публичные методы ─────────────────────────────────────────────────────

    def get_orders(self, date_from: datetime, flag: int = 0,
                   on_progress=None) -> list[dict]:
        return self._get(
            "/api/v1/supplier/orders",
            {"dateFrom": date_from.strftime("%Y-%m-%dT%H:%M:%S"), "flag": flag},
            key="orders",
            on_progress=on_progress,
        )

    def get_sales(self, date_from: datetime, flag: int = 0,
                  on_progress=None) -> list[dict]:
        return self._get(
            "/api/v1/supplier/sales",
            {"dateFrom": date_from.strftime("%Y-%m-%dT%H:%M:%S"), "flag": flag},
            key="sales",
            on_progress=on_progress,
        )

    def get_stocks(self, date_from: datetime, on_progress=None) -> list[dict]:
        return self._get(
            "/api/v1/supplier/stocks",
            {"dateFrom": date_from.strftime("%Y-%m-%dT%H:%M:%S")},
            key="stocks",
            on_progress=on_progress,
        )

    def get_financial_report(self, date_from: datetime, date_to: datetime,
                             on_progress=None) -> list[dict]:
        """Финансовый отчёт с пагинацией по rrdid."""
        path   = "/api/v5/supplier/reportDetailByPeriod"
        params = {
            "dateFrom": date_from.strftime("%Y-%m-%d"),
            "dateTo":   date_to.strftime("%Y-%m-%d"),
            "rrdid":    0,
            "limit":    100_000,
        }
        all_rows: list[dict] = []
        page = 0

        while True:
            page += 1
            self._wait_if_needed(key="finance", on_progress=on_progress)

            url = f"{BASE_URL}{path}"
            self._record_request("finance")
            resp = self.session.get(url, params=params, timeout=120)

            if resp.status_code == 401:
                raise WBApiError("Неверный токен WB API.")

            if resp.status_code == 429:
                retry_after = int(
                    resp.headers.get("X-Ratelimit-Retry")
                    or resp.headers.get("Retry-After")
                    or resp.headers.get("X-Ratelimit-Reset")
                    or 300
                )
                self._record_block(retry_after)
                msg = (f"WB API 429 (финотчёт) — блок на {retry_after} сек. "
                       "Прерываю синхронизацию.")
                if on_progress:
                    on_progress(msg)
                else:
                    print(f"🚫 {msg}")
                raise WBApiError(msg)

            if resp.status_code != 200:
                raise WBApiError(
                    f"Ошибка WB API {resp.status_code}: {resp.text[:300]}"
                )

            rows = resp.json()
            if not rows:
                break
            all_rows.extend(rows)
            if on_progress:
                on_progress(
                    f"  Страница {page}: {len(rows)} строк (итого {len(all_rows)})"
                )
            params["rrdid"] = rows[-1].get("rrd_id", 0)
            if len(rows) < params["limit"]:
                break

        return all_rows


# ── Парсеры ──────────────────────────────────────────────────────────────────

def parse_financial_report(raw: list[dict], platform: str = "wb") -> list[dict]:
    result = []
    for r in raw:
        result.append({
            "platform":                  platform,
            "rrd_id":                    r.get("rrd_id"),
            "realizationreport_id":      r.get("realizationreport_id"),
            "date_from":                 _parse_date(r.get("date_from")),
            "date_to":                   _parse_date(r.get("date_to")),
            "create_dt":                 _parse_dt(r.get("create_dt")),
            "nm_id":                     r.get("nm_id"),
            "supplier_article":          r.get("sa_name") or r.get("supplier_article"),
            "brand_name":                r.get("brand_name"),
            "subject_name":              r.get("subject_name"),
            "doc_type_name":             r.get("doc_type_name"),
            "supplier_oper_name":        r.get("supplier_oper_name"),
            "quantity":                  _int(r.get("quantity")) or 0,
            "retail_price":              _float(r.get("retail_price")),
            "retail_price_withdisc_rub": _float(r.get("retail_price_withdisc_rub")),
            "ppvz_for_pay":              _float(r.get("ppvz_for_pay")),
            "ppvz_sales_commission":     _float(r.get("ppvz_sales_commission")),
            "delivery_rub":              _float(r.get("delivery_rub")),
            "penalty":                   _float(r.get("penalty")),
            "additional_payment":        _float(r.get("additional_payment")),
            "storage_fee":               _float(r.get("storage_fee")),
            "acquiring_fee":             _float(r.get("acquiring_fee")),
        })
    return result


def parse_stocks(raw: list[dict]) -> list[dict]:
    result = []
    for r in raw:
        result.append({
            "nm_id":              r.get("nmId"),
            "supplier_article":   r.get("supplierArticle"),
            "barcode":            r.get("barcode"),
            "brand":              r.get("brand"),
            "subject":            r.get("subject"),
            "category":           r.get("category"),
            "warehouse_name":     r.get("warehouseName"),
            "quantity":           int(r.get("quantity") or 0),
            "in_way_to_client":   int(r.get("inWayToClient") or 0),
            "in_way_from_client": int(r.get("inWayFromClient") or 0),
            "quantity_full":      int(r.get("quantityFull") or 0),
        })
    return result


def parse_orders(raw: list[dict], platform: str = "wb") -> list[dict]:
    result = []
    for r in raw:
        order_date = _parse_dt(r.get("date") or r.get("dateCreated"))
        if not order_date:
            continue
        result.append({
            "platform":         platform,
            "srid":             r.get("srid") or r.get("odid") or
                                str(r.get("nmId", "")) + str(r.get("date", "")),
            "nm_id":            r.get("nmId"),
            "supplier_article": r.get("supplierArticle"),
            "barcode":          r.get("barcode"),
            "brand":            r.get("brand"),
            "subject":          r.get("subject"),
            "category":         r.get("category"),
            "warehouse_name":   r.get("warehouseName"),
            "region_name":      r.get("regionName"),
            "country_name":     r.get("countryName"),
            "total_price":      _float(r.get("totalPrice")),
            "discount_percent": _int(r.get("discountPercent")),
            "spp":              _float(r.get("spp")),
            "finished_price":   _float(r.get("finishedPrice")),
            "price_with_disc":  _float(r.get("priceWithDisc")),
            "order_date":       order_date,
            "last_change_date": _parse_dt(r.get("lastChangeDate")),
            "is_cancel":        bool(r.get("isCancel", False)),
            "cancel_dt":        _parse_dt(r.get("cancelDt")),
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
            "platform":         platform,
            "sale_id":          sale_id,
            "nm_id":            r.get("nmId"),
            "supplier_article": r.get("supplierArticle"),
            "barcode":          r.get("barcode"),
            "brand":            r.get("brand"),
            "subject":          r.get("subject"),
            "category":         r.get("category"),
            "warehouse_name":   r.get("warehouseName"),
            "region_name":      r.get("regionName"),
            "country_name":     r.get("countryName"),
            "price_with_disc":  _float(r.get("priceWithDisc")),
            "finished_price":   _float(r.get("finishedPrice")),
            "for_pay":          _float(r.get("forPay")),
            "total_price":      _float(r.get("totalPrice")),
            "discount_percent": _int(r.get("discountPercent")),
            "spp":              _float(r.get("spp")),
            "sale_date":        sale_date,
            "last_change_date": _parse_dt(r.get("lastChangeDate")),
            "is_return":        sale_id.startswith("R"),
        })
    return result


# ── Хелперы ───────────────────────────────────────────────────────────────────

def _parse_date(value):
    if not value:
        return None
    try:
        from datetime import date
        return date.fromisoformat(str(value)[:10])
    except Exception:
        return None


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
