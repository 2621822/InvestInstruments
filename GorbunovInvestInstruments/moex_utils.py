"""Утилиты, извлечённые из старого moex.py (для тестов и переиспользования).

Содержит:
 - START_DATE
 - get_date_ranges(start, end, step)
 - resolve_start_date(base_start, args, last_date_raw)
 - RateLimiter (асинхронный ограничитель скорости)

Оставлено минимум необходимого после удаления legacy moex.py.
"""
from __future__ import annotations

import asyncio
import datetime as dt

START_DATE = dt.date(2022, 1, 1)


def get_date_ranges(start: dt.date, end: dt.date, step: int) -> list[tuple[dt.date, dt.date]]:
    ranges: list[tuple[dt.date, dt.date]] = []
    current = start
    while current <= end:
        till = min(current + dt.timedelta(days=step - 1), end)
        ranges.append((current, till))
        current = till + dt.timedelta(days=1)
    return ranges


def resolve_start_date(base_start: dt.date, args, last_date_raw: str | None) -> dt.date:
    """Логика выбора стартовой даты.

    Приоритет:
      1. args.since (YYYY-MM-DD)
      2. args.days (окно последних N дней относительно args._effective_end_date)
      3. last_date_raw + 1 день
      4. base_start
    """
    if getattr(args, "since", None):
        try:
            return dt.datetime.strptime(args.since, "%Y-%m-%d").date()
        except ValueError:
            pass
    if getattr(args, "days", None) is not None:
        try:
            window_days = int(args.days)
            if window_days > 0:
                return args._effective_end_date - dt.timedelta(days=window_days - 1)
        except Exception:  # noqa: BLE001
            pass
    if last_date_raw:
        for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
            try:
                parsed = dt.datetime.strptime(last_date_raw, fmt).date()
                return parsed + dt.timedelta(days=1)
            except ValueError:
                continue
    return base_start


class RateLimiter:
    """Простейший token-bucket ограничитель для асинхронного кода."""

    def __init__(self, rate: float, burst: int | None = None):
        if rate <= 0:
            raise ValueError("rate должен быть > 0")
        self.rate = rate
        self.capacity = burst if burst and burst > 0 else max(1, int(rate))
        self.tokens = self.capacity
        self.updated = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self.updated
            if elapsed > 0:
                refill = elapsed * self.rate
                if refill > 0:
                    self.tokens = min(self.capacity, self.tokens + refill)
                    self.updated = now
            if self.tokens >= 1:
                self.tokens -= 1
                return
            needed = 1 - self.tokens
            wait_time = needed / self.rate
        await asyncio.sleep(wait_time)
        await self.acquire()
