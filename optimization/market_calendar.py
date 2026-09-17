from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .models import SessionWindow


EASTERN = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def is_nyse_session(session: date) -> bool:
    return session.weekday() < 5 and session not in nyse_holidays(session.year)


def is_nyse_early_close(session: date) -> bool:
    return session in nyse_early_closes(session.year)


def nyse_session_window(session: date) -> SessionWindow:
    if not is_nyse_session(session):
        raise ValueError(f"{session.isoformat()} is not a NYSE regular session.")
    end = time(13, 0) if is_nyse_early_close(session) else time(16, 0)
    return SessionWindow(
        session_date=session,
        start_utc=datetime.combine(session, time(9, 30), tzinfo=EASTERN).astimezone(UTC),
        end_utc=datetime.combine(session, end, tzinfo=EASTERN).astimezone(UTC),
    )


def latest_completed_nyse_sessions(*, now: datetime, count: int) -> tuple[date, ...]:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware.")
    current = now.astimezone(EASTERN).date()
    sessions: list[date] = []
    while len(sessions) < count:
        if is_nyse_session(current):
            window = nyse_session_window(current)
            if now.astimezone(UTC) >= window.end_utc:
                sessions.append(current)
        current -= timedelta(days=1)
    return tuple(reversed(sessions))


def preceding_nyse_sessions(before: date, *, count: int) -> tuple[date, ...]:
    current = before - timedelta(days=1)
    sessions: list[date] = []
    while len(sessions) < count:
        if is_nyse_session(current):
            sessions.append(current)
        current -= timedelta(days=1)
    return tuple(reversed(sessions))


def nyse_holidays(year: int) -> set[date]:
    holidays = {
        _observed(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _good_friday(year),
        _last_weekday(year, 5, 0),
        _observed(date(year, 6, 19)),
        _observed(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed(date(year, 12, 25)),
    }
    return {item for item in holidays if item.year == year}


def nyse_early_closes(year: int) -> set[date]:
    candidates = {
        date(year, 7, 3),
        _nth_weekday(year, 11, 3, 4) + timedelta(days=1),
        date(year, 12, 24),
    }
    return {item for item in candidates if item.year == year and is_nyse_session(item)}


def _observed(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nth_weekday(year: int, month: int, weekday: int, nth: int) -> date:
    current = date(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)
    return current + timedelta(days=7 * (nth - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    current = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    while current.weekday() != weekday:
        current -= timedelta(days=1)
    return current


def _good_friday(year: int) -> date:
    # Meeus/Jones/Butcher Gregorian Easter algorithm.
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day) - timedelta(days=2)
