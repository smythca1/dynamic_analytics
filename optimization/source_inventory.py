from __future__ import annotations

import json
import gzip
import shutil
import subprocess
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Callable

from replay.historical_source import file_sha256

from .models import SourceFileInfo


DEFAULT_REMOTE_ROOT = "/root/trading-server/artifacts/live/ticks"


@dataclass(frozen=True)
class StageRequest:
    host: str
    symbols: tuple[str, ...]
    sessions: tuple[date, ...]
    output_root: str = "artifacts/bronze/market"
    remote_root: str = DEFAULT_REMOTE_ROOT
    dry_run: bool = False


@dataclass(frozen=True)
class StageResult:
    files: tuple[SourceFileInfo, ...]
    commands: tuple[list[str], ...]
    copied: int
    reused: int
    skipped: int


def remote_source_path(remote_root: str, symbol: str, session: date) -> str:
    return f"{remote_root.rstrip('/')}/{session.isoformat()}/{symbol.upper()}.jsonl"


def local_source_path(output_root: str | Path, symbol: str, session: date) -> Path:
    return Path(output_root) / symbol.upper() / session.isoformat() / "sip_trades.jsonl"


def resolve_local_source_path(output_root: str | Path, symbol: str, session: date) -> Path:
    plain = local_source_path(output_root, symbol, session)
    compressed = plain.with_suffix(plain.suffix + ".gz")
    if plain.exists() or not compressed.exists():
        return plain
    return compressed


def stage_sources(
    request: StageRequest,
    *,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> StageResult:
    files: list[SourceFileInfo] = []
    commands: list[list[str]] = []
    copied = reused = skipped = 0
    for symbol in request.symbols:
        for session in request.sessions:
            remote = remote_source_path(request.remote_root, symbol, session)
            local = local_source_path(request.output_root, symbol, session)
            command = ["scp", f"{request.host}:{remote}", str(local)]
            commands.append(command)
            if request.dry_run:
                files.append(SourceFileInfo(symbol.upper(), session, str(local), local.exists(), "raw_sip_trades", warnings=("dry_run",)))
                skipped += 1
                continue
            local.parent.mkdir(parents=True, exist_ok=True)
            tmp = local.with_name(local.name + ".tmp")
            if tmp.exists():
                tmp.unlink()
            if local.exists():
                info = inspect_source_file(local, symbol=symbol, session=session)
                metadata = _metadata_path(local)
                if metadata.exists():
                    payload = json.loads(metadata.read_text(encoding="utf-8"))
                    if payload.get("source_hash") == info.sha256:
                        files.append(info)
                        reused += 1
                        continue
                raise FileExistsError(f"Existing local source differs or lacks matching metadata: {local}")
            try:
                runner(["scp", f"{request.host}:{remote}", str(tmp)], check=True)
                tmp.replace(local)
                info = inspect_source_file(local, symbol=symbol, session=session)
                write_source_metadata(info, remote_source_path=remote)
                files.append(info)
                copied += 1
            except Exception:
                if tmp.exists():
                    tmp.unlink()
                raise
    return StageResult(tuple(files), tuple(commands), copied=copied, reused=reused, skipped=skipped)


def inspect_source_file(path: str | Path, *, symbol: str, session: date) -> SourceFileInfo:
    file_path = Path(path)
    if not file_path.exists():
        return SourceFileInfo(symbol.upper(), session, str(file_path), False, "raw_sip_trades", warnings=("missing",))
    digest = file_sha256(file_path)
    row_count = 0
    earliest: str | None = None
    latest: str | None = None
    feed: str | None = None
    with _open_text(file_path) as handle:
        for line in handle:
            if not line.strip():
                continue
            row_count += 1
            payload = json.loads(line)
            timestamp = payload.get("timestamp") or payload.get("t") or payload.get("sip_timestamp")
            if timestamp:
                earliest = timestamp if earliest is None or timestamp < earliest else earliest
                latest = timestamp if latest is None or timestamp > latest else latest
            feed = feed or payload.get("feed")
    warnings = tuple() if feed in {None, "sip"} else (f"unexpected_feed:{feed}",)
    return SourceFileInfo(symbol.upper(), session, str(file_path), True, "raw_sip_trades", digest, row_count, earliest, latest, warnings)


def write_source_metadata(info: SourceFileInfo, *, remote_source_path: str) -> Path:
    path = _metadata_path(info.path)
    payload = {
        "symbol": info.symbol,
        "date": info.session_date.isoformat(),
        "feed": "sip",
        "remote_source_path": remote_source_path,
        "local_path": info.path,
        "row_count": info.row_count,
        "actual_earliest_timestamp": info.earliest_timestamp,
        "actual_latest_timestamp": info.latest_timestamp,
        "source_hash": info.sha256,
        "copy_timestamp": datetime.now(timezone.utc).isoformat(),
        "completion_status": "complete" if info.exists and info.row_count else "incomplete",
        "warnings": list(info.warnings),
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _metadata_path(path: str | Path) -> Path:
    return Path(path).with_suffix(".metadata.json")


def _open_text(path: Path):
    if path.suffixes[-2:] == [".jsonl", ".gz"]:
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")
