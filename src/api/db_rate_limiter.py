"""
DB-based rate limiter для WB API.

Хранит состояние в PostgreSQL/SQLite — единый источник истины для всех
GitHub Actions runner'ов (которые не разделяют /tmp/).

Ключи в таблице wb_api_state:
  wb_last_request_at  — unix timestamp последнего запроса к любому эндпоинту WB
  wb_blocked_until    — unix timestamp конца блокировки (0 = не заблокирован)
"""

import time
import logging
from datetime import datetime
from typing import Optional

log = logging.getLogger(__name__)

KEY_LAST_REQUEST = "wb_last_request_at"
KEY_BLOCKED      = "wb_blocked_until"

MIN_INTERVAL         = 90   # секунд — orders / sales / stocks
MIN_INTERVAL_FINANCE = 310  # секунд — reportDetailByPeriod


class DBRateLimiter:
    """
    Потокобезопасный (и cross-runner) rate limiter через UPSERT в БД.
    Принимает engine напрямую — не зависит от deprecated session.bind.
    """

    def __init__(self, engine):
        """engine — SQLAlchemy Engine (из init_db или create_engine)."""
        self._engine  = engine
        self._dialect = engine.dialect.name  # 'postgresql' | 'sqlite'

    # ── внутренние методы ────────────────────────────────────────────────────

    def _get(self, key: str) -> float:
        from sqlalchemy import text
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("SELECT value FROM wb_api_state WHERE key = :k"),
                    {"k": key},
                ).fetchone()
                return float(row[0]) if row else 0.0
        except Exception as e:
            log.warning("DBRateLimiter._get(%s) failed: %s", key, e)
            return 0.0

    def _set(self, key: str, value: float):
        from sqlalchemy import text
        now = datetime.utcnow()
        try:
            with self._engine.begin() as conn:
                if self._dialect == "postgresql":
                    conn.execute(text("""
                        INSERT INTO wb_api_state (key, value, updated_at)
                        VALUES (:k, :v, :ts)
                        ON CONFLICT (key) DO UPDATE
                            SET value = EXCLUDED.value,
                                updated_at = EXCLUDED.updated_at
                    """), {"k": key, "v": value, "ts": now})
                else:  # sqlite
                    conn.execute(text("""
                        INSERT INTO wb_api_state (key, value, updated_at)
                        VALUES (:k, :v, :ts)
                        ON CONFLICT (key) DO UPDATE
                            SET value = excluded.value,
                                updated_at = excluded.updated_at
                    """), {"k": key, "v": value, "ts": now})
        except Exception as e:
            log.warning("DBRateLimiter._set(%s, %s) failed: %s", key, value, e)

    # ── публичный API ────────────────────────────────────────────────────────

    def last_request_at(self) -> float:
        return self._get(KEY_LAST_REQUEST)

    def blocked_until(self) -> float:
        return self._get(KEY_BLOCKED)

    def is_blocked(self) -> bool:
        return time.time() < self.blocked_until()

    def record_request(self):
        """Записать время текущего запроса."""
        self._set(KEY_LAST_REQUEST, time.time())

    def record_block(self, seconds: float):
        """Записать время окончания блокировки токена."""
        until = time.time() + seconds
        self._set(KEY_BLOCKED, until)
        log.warning(
            "WB token blocked for %.0f s (until %s UTC)",
            seconds,
            datetime.utcfromtimestamp(until).strftime("%H:%M:%S"),
        )

    def clear_block(self):
        self._set(KEY_BLOCKED, 0.0)

    def check_blocked(self, on_progress=None) -> Optional[float]:
        """
        Если токен заблокирован — вывести сообщение и вернуть секунды ожидания.
        Иначе вернуть None.
        """
        until     = self.blocked_until()
        remaining = until - time.time()
        if remaining > 0:
            mins = int(remaining // 60)
            secs = int(remaining % 60)
            until_str = datetime.utcfromtimestamp(until).strftime("%H:%M:%S")
            msg = (f"WB API токен заблокирован ещё на {mins}м {secs}с "
                   f"(до {until_str} UTC). Синхронизация отменена.")
            if on_progress:
                on_progress(msg)
            else:
                print(f"🚫 {msg}")
            return remaining
        return None

    def wait_if_needed(self, key: str = "default", on_progress=None) -> float:
        """
        Подождать, если с последнего запроса прошло меньше MIN_INTERVAL.
        Возвращает фактическое время ожидания в секундах.
        """
        interval = MIN_INTERVAL_FINANCE if key == "finance" else MIN_INTERVAL
        elapsed  = time.time() - self.last_request_at()
        if elapsed >= interval:
            return 0.0

        wait = interval - elapsed + 2  # +2 сек запас
        msg  = f"Пауза {int(wait)} сек (глобальный rate limit WB API)..."
        if on_progress:
            on_progress(msg)
        else:
            print(f"⏱  {msg}")
        time.sleep(wait)
        return wait
