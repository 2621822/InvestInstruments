import asyncio
import datetime as dt
import pytest

from moex import get_date_ranges, resolve_start_date, RateLimiter, START_DATE

class DummyArgs:
    def __init__(self, to_date=None, since=None, days=None):
        self.to_date = to_date
        self.since = since
        self.days = days
        # internal attribute used by resolve_start_date
        self._effective_end_date = dt.date.today() if to_date is None else dt.datetime.strptime(to_date, "%Y-%m-%d").date()


def test_get_date_ranges_basic():
    start = dt.date(2024, 1, 1)
    end = dt.date(2024, 1, 10)
    ranges = get_date_ranges(start, end, step=3)
    assert ranges == [
        (dt.date(2024, 1, 1), dt.date(2024, 1, 3)),
        (dt.date(2024, 1, 4), dt.date(2024, 1, 6)),
        (dt.date(2024, 1, 7), dt.date(2024, 1, 9)),
        (dt.date(2024, 1, 10), dt.date(2024, 1, 10)),
    ]


def test_resolve_start_date_since_overrides():
    args = DummyArgs(to_date="2024-02-10", since="2024-02-01", days=5)
    d = resolve_start_date(START_DATE, args, last_date_raw="2023-12-31")
    assert d == dt.date(2024, 2, 1)


def test_resolve_start_date_days_window():
    args = DummyArgs(to_date="2024-02-10", since=None, days=5)
    d = resolve_start_date(START_DATE, args, last_date_raw="2024-01-20")
    # days=5 => start = end - 5 + 1 = 2024-02-10 -4 = 2024-02-06
    assert d == dt.date(2024, 2, 6)


def test_resolve_start_date_incremental():
    args = DummyArgs(to_date="2024-02-10", since=None, days=None)
    d = resolve_start_date(START_DATE, args, last_date_raw="2024-02-05")
    assert d == dt.date(2024, 2, 6)


def test_resolve_start_date_fallback():
    args = DummyArgs(to_date="2024-02-10", since=None, days=None)
    d = resolve_start_date(START_DATE, args, last_date_raw=None)
    assert d == START_DATE

@pytest.mark.asyncio
async def test_rate_limiter_basic():
    rl = RateLimiter(rate=5)  # 5 rps
    t0 = asyncio.get_event_loop().time()
    # Acquire 5 tokens quickly
    for _ in range(5):
        await rl.acquire()
    t1 = asyncio.get_event_loop().time()
    # Should be near-immediate (<0.3s to be safe on CI)
    assert (t1 - t0) < 0.3
    # Next acquire should incur wait ~0.2s (1/5)
    await rl.acquire()
    t2 = asyncio.get_event_loop().time()
    assert (t2 - t1) >= 0.15
