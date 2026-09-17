from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterator
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")
DEFAULT_TICK_ROOT = Path("/root/trading-server/artifacts/live/ticks")
DEFAULT_LOCK_PATH = Path("/tmp/trading_tick_retention.lock")
SUPPORTED_CALENDAR_START_YEAR = 1990
SUPPORTED_CALENDAR_END_YEAR = 2030
MARKET_CLOSE = time(16, 0)
DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class TickRetentionError(RuntimeError):
    pass


class TradingCalendarError(TickRetentionError):
    pass


@dataclass(frozen=True)
class TickFile:
    path: str
    session_date: str
    size: int


@dataclass(frozen=True)
class SkippedPath:
    path: str
    reason: str


@dataclass(frozen=True)
class RetentionError:
    path: str
    error: str


def _nth_weekday(year: int, month: int, weekday: int, nth: int) -> date:
    current = date(year, month, 1)
    offset = (weekday - current.weekday()) % 7
    return current + timedelta(days=offset + (nth - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        current = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        current = date(year, month + 1, 1) - timedelta(days=1)
    return current - timedelta(days=(current.weekday() - weekday) % 7)


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    actual = date(year, month, day)
    if actual.weekday() == 5:
        return actual - timedelta(days=1)
    if actual.weekday() == 6:
        return actual + timedelta(days=1)
    return actual


def _easter_date(year: int) -> date:
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
    return date(year, month, day)


def _validate_supported_year(value: date) -> None:
    if not (SUPPORTED_CALENDAR_START_YEAR <= value.year <= SUPPORTED_CALENDAR_END_YEAR):
        raise TradingCalendarError(
            f"NYSE calendar is supported only for {SUPPORTED_CALENDAR_START_YEAR}-"
            f"{SUPPORTED_CALENDAR_END_YEAR}; got {value.isoformat()}"
        )


def nyse_holidays_for_year(year: int) -> set[date]:
    if not (SUPPORTED_CALENDAR_START_YEAR <= year <= SUPPORTED_CALENDAR_END_YEAR):
        raise TradingCalendarError(
            f"NYSE calendar is supported only for {SUPPORTED_CALENDAR_START_YEAR}-{SUPPORTED_CALENDAR_END_YEAR}; got {year}"
        )

    holidays: set[date] = set()
    for holiday_year in (year - 1, year, year + 1):
        fixed_holidays = [
            _observed_fixed_holiday(holiday_year, 1, 1),
            _observed_fixed_holiday(holiday_year, 7, 4),
            _observed_fixed_holiday(holiday_year, 12, 25),
        ]
        if holiday_year >= 2022:
            fixed_holidays.append(_observed_fixed_holiday(holiday_year, 6, 19))
        for observed in fixed_holidays:
            if observed.year == year:
                holidays.add(observed)

    holidays.update(
        {
            _nth_weekday(year, 1, 0, 3),  # Martin Luther King Jr. Day
            _nth_weekday(year, 2, 0, 3),  # Washington's Birthday
            _easter_date(year) - timedelta(days=2),  # Good Friday
            _last_weekday(year, 5, 0),  # Memorial Day
            _nth_weekday(year, 9, 0, 1),  # Labor Day
            _nth_weekday(year, 11, 3, 4),  # Thanksgiving
        }
    )

    holidays.update(
        {
            date(2001, 9, 11),
            date(2001, 9, 12),
            date(2001, 9, 13),
            date(2001, 9, 14),
            date(2004, 6, 11),
            date(2007, 1, 2),
            date(2012, 10, 29),
            date(2012, 10, 30),
            date(2018, 12, 5),
            date(2025, 1, 9),
        }
    )
    return {holiday for holiday in holidays if holiday.year == year}


def is_nyse_session(value: date) -> bool:
    _validate_supported_year(value)
    return value.weekday() < 5 and value not in nyse_holidays_for_year(value.year)


def normalize_as_of(as_of: datetime | str | None) -> datetime:
    if as_of is None:
        return datetime.now(tz=EASTERN)
    if isinstance(as_of, str):
        text = as_of.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    else:
        parsed = as_of
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=EASTERN)
    return parsed.astimezone(EASTERN)


def retained_nyse_sessions(retention_trading_days: int, as_of: datetime | str | None = None) -> tuple[list[date], date | None]:
    if retention_trading_days < 1:
        raise TickRetentionError("retention_trading_days must be >= 1")

    local_as_of = normalize_as_of(as_of)
    cursor = local_as_of.date()
    current_session = cursor if is_nyse_session(cursor) else None

    if current_session is None:
        cursor -= timedelta(days=1)

    retained: list[date] = []
    while len(retained) < retention_trading_days:
        _validate_supported_year(cursor)
        if is_nyse_session(cursor):
            retained.append(cursor)
        cursor -= timedelta(days=1)

    retained.reverse()
    return retained, current_session


@contextlib.contextmanager
def _exclusive_lock(lock_path: Path) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise TickRetentionError(f"another tick retention job is already running: {lock_path}") from exc
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _path_inside_root(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _parse_session_from_relative_path(relative: Path) -> date | None:
    parts = relative.parts
    if len(parts) != 2:
        return None
    if not DATE_DIR_RE.match(parts[0]):
        return None
    if relative.suffix != ".jsonl":
        return None
    try:
        return date.fromisoformat(parts[0])
    except ValueError:
        return None


def _inventory_tick_files(root: Path) -> tuple[list[TickFile], list[SkippedPath], list[RetentionError]]:
    files: list[TickFile] = []
    skipped: list[SkippedPath] = []
    errors: list[RetentionError] = []
    stack = [root]

    while stack:
        directory = stack.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    path = Path(entry.path)
                    display_path = str(path)
                    try:
                        relative = path.relative_to(root)
                    except ValueError:
                        skipped.append(SkippedPath(display_path, "outside_tick_root"))
                        continue

                    try:
                        if entry.is_symlink():
                            skipped.append(SkippedPath(display_path, "symlink"))
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(path)
                            continue
                        if not entry.is_file(follow_symlinks=False):
                            skipped.append(SkippedPath(display_path, "not_regular_file"))
                            continue
                        session = _parse_session_from_relative_path(relative)
                        if session is None:
                            skipped.append(SkippedPath(display_path, "unrecognized_tick_path"))
                            continue
                        try:
                            if not is_nyse_session(session):
                                skipped.append(SkippedPath(display_path, "not_nyse_session"))
                                continue
                        except TradingCalendarError as exc:
                            errors.append(RetentionError(display_path, str(exc)))
                            continue
                        stat_result = entry.stat(follow_symlinks=False)
                        files.append(TickFile(display_path, session.isoformat(), int(stat_result.st_size)))
                    except FileNotFoundError:
                        skipped.append(SkippedPath(display_path, "already_missing"))
                    except OSError as exc:
                        errors.append(RetentionError(display_path, str(exc)))
        except FileNotFoundError:
            errors.append(RetentionError(str(directory), "directory disappeared during inventory"))
        except OSError as exc:
            errors.append(RetentionError(str(directory), str(exc)))

    files.sort(key=lambda item: (item.session_date, item.path))
    skipped.sort(key=lambda item: item.path)
    errors.sort(key=lambda item: item.path)
    return files, skipped, errors


def _json_log_sink(message: dict[str, object]) -> None:
    print(json.dumps(message, sort_keys=True), file=sys.stderr, flush=True)


def purge_tick_data(
    retention_trading_days: int = 15,
    dry_run: bool = False,
    *,
    tick_root: str | Path = DEFAULT_TICK_ROOT,
    as_of: datetime | str | None = None,
    lock_path: str | Path = DEFAULT_LOCK_PATH,
    log_sink: Callable[[dict[str, object]], None] | None = None,
) -> dict[str, object]:
    root = Path(tick_root)
    if not root.is_absolute():
        raise TickRetentionError("tick_root must be an absolute path")
    root_resolved = root.resolve(strict=True)
    if not root_resolved.is_dir():
        raise TickRetentionError(f"tick_root is not a directory: {root_resolved}")

    sink = log_sink or _json_log_sink
    local_as_of = normalize_as_of(as_of)
    retained_dates, current_session = retained_nyse_sessions(retention_trading_days, local_as_of)
    retained_sessions = [session.isoformat() for session in retained_dates]
    retained_session_set = set(retained_sessions)
    cutoff_session = retained_sessions[0]

    with _exclusive_lock(Path(lock_path)):
        inventory, skipped, errors = _inventory_tick_files(root_resolved)
        recognized_sessions = sorted({item.session_date for item in inventory})
        selected: list[TickFile] = []
        deleted: list[TickFile] = []
        selection_suppressed_reason = None

        if len(recognized_sessions) < retention_trading_days:
            selection_suppressed_reason = "fewer_than_retention_sessions"
        elif errors:
            selection_suppressed_reason = "inventory_errors"
        else:
            for item in inventory:
                if item.session_date == (current_session.isoformat() if current_session else None):
                    continue
                if item.session_date in retained_session_set:
                    continue
                if item.session_date < cutoff_session:
                    selected.append(item)

        bytes_selected = sum(item.size for item in selected)
        bytes_reclaimed = 0
        delete_errors: list[RetentionError] = []
        delete_skipped: list[SkippedPath] = []

        if not dry_run:
            for item in selected:
                path = Path(item.path)
                try:
                    stat_result = path.lstat()
                except FileNotFoundError:
                    delete_skipped.append(SkippedPath(item.path, "already_missing"))
                    continue
                if not path.is_file() or path.is_symlink():
                    delete_skipped.append(SkippedPath(item.path, "not_regular_file_at_delete_time"))
                    continue
                try:
                    resolved_path = path.resolve(strict=True)
                except FileNotFoundError:
                    delete_skipped.append(SkippedPath(item.path, "already_missing"))
                    continue
                if not _path_inside_root(resolved_path, root_resolved):
                    delete_errors.append(RetentionError(item.path, "resolved path escaped tick root"))
                    continue
                size = int(stat_result.st_size)
                path.unlink()
                deleted_item = TickFile(item.path, item.session_date, size)
                deleted.append(deleted_item)
                bytes_reclaimed += size
                sink(
                    {
                        "event": "tick_file_deleted",
                        "path": item.path,
                        "session_date": item.session_date,
                        "size": size,
                    }
                )

        all_skipped = [*skipped, *delete_skipped]
        all_errors = [*errors, *delete_errors]
        return {
            "tick_root": str(root_resolved),
            "dry_run": dry_run,
            "as_of": local_as_of.isoformat(),
            "retention_trading_days": retention_trading_days,
            "cutoff_session": cutoff_session,
            "retained_sessions": retained_sessions,
            "current_session": current_session.isoformat() if current_session else None,
            "recognized_sessions": recognized_sessions,
            "selection_suppressed_reason": selection_suppressed_reason,
            "files_selected": [asdict(item) for item in selected],
            "files_deleted": [asdict(item) for item in deleted],
            "bytes_selected": bytes_selected,
            "bytes_reclaimed": bytes_reclaimed,
            "skipped": [asdict(item) for item in all_skipped],
            "errors": [asdict(item) for item in all_errors],
        }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Purge live tick archives using NYSE trading-session retention.")
    parser.add_argument("--tick-root", default=str(DEFAULT_TICK_ROOT))
    parser.add_argument("--retention-trading-days", type=int, default=15)
    parser.add_argument("--as-of", help="ISO timestamp for deterministic runs. Defaults to now in America/New_York.")
    parser.add_argument("--lock-path", default=str(DEFAULT_LOCK_PATH))
    parser.add_argument("--execute", action="store_true", help="Delete selected files. Omit for dry-run mode.")
    parser.add_argument("--json", action="store_true", help="Print the full report as JSON.")
    return parser.parse_args()


def _print_text_report(report: dict[str, object]) -> None:
    print(f"tick_root: {report['tick_root']}")
    print(f"dry_run: {report['dry_run']}")
    print(f"as_of: {report['as_of']}")
    print(f"cutoff_session: {report['cutoff_session']}")
    print("retained_sessions:")
    for session in report["retained_sessions"]:
        print(f"  {session}")
    print(f"files_selected: {len(report['files_selected'])}")
    print(f"bytes_selected: {report['bytes_selected']}")
    print(f"files_deleted: {len(report['files_deleted'])}")
    print(f"bytes_reclaimed: {report['bytes_reclaimed']}")
    if report.get("selection_suppressed_reason"):
        print(f"selection_suppressed_reason: {report['selection_suppressed_reason']}")
    if report["files_selected"]:
        print("selected_paths:")
        for item in report["files_selected"]:
            print(f"  {item['session_date']} {item['size']} {item['path']}")
    if report["skipped"]:
        print("skipped:")
        for item in report["skipped"]:
            print(f"  {item['reason']} {item['path']}")
    if report["errors"]:
        print("errors:")
        for item in report["errors"]:
            print(f"  {item['error']} {item['path']}")


def main() -> int:
    args = _parse_args()
    try:
        report = purge_tick_data(
            retention_trading_days=args.retention_trading_days,
            dry_run=not args.execute,
            tick_root=args.tick_root,
            as_of=args.as_of,
            lock_path=args.lock_path,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        _print_text_report(report)
    return 1 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
