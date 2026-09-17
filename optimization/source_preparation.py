from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import date
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any

from replay.historical_source import stream_historical_source
from replay.models import ReplayConfig
from silver.market import CanonicalObservation, CanonicalOneSecondSipSampler, market_stream_contract
from silver.market.one_second_sampler import SAMPLER_VERSION
from silver.market.trade_filter import TRADE_FILTER_VERSION

from .models import PREPARATION_VERSION, PreparedSource, SessionWindow, SourceFileInfo
from .market_calendar import nyse_session_window
from .source_inventory import inspect_source_file, resolve_local_source_path


FILTER_VERSION = TRADE_FILTER_VERSION


def prepare_source(
    *,
    symbol: str,
    trader_id: str,
    session_dates: tuple[date, ...],
    source_root: str,
    output_root: str,
    run_id: str,
    session_windows: tuple[SessionWindow, ...] | None = None,
) -> PreparedSource:
    windows = session_windows or tuple(nyse_session_window(session) for session in session_dates)
    files = tuple(inspect_source_file(resolve_local_source_path(source_root, symbol, session), symbol=symbol, session=session) for session in session_dates)
    missing = [item.path for item in files if not item.exists]
    if missing:
        raise FileNotFoundError("Missing source files: " + ", ".join(missing))
    preparation_id = _preparation_id(symbol=symbol, session_dates=session_dates, files=files, windows=windows)
    out_dir = Path(output_root) / preparation_id
    observations_path = out_dir / "observations_1s.jsonl"
    manifest_path = out_dir / "manifest.json"
    if manifest_path.exists() and observations_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("preparation_id") == preparation_id:
            contract = dict(manifest.get("market_stream_contract") or {})
            return PreparedSource(
                preparation_id=preparation_id,
                symbol=symbol.upper(),
                session_dates=session_dates,
                source_files=files,
                source_combined_hash=manifest["combined_source_hash"],
                observation_count=int(manifest["observation_count"]),
                output_directory=str(out_dir),
                observations_path=str(observations_path),
                diagnostics_summary=dict(manifest.get("diagnostics_summary", {})),
                data_quality_status=str(manifest.get("data_quality_status", "UNKNOWN")),
                reused=True,
                market_stream_contract=contract,
                approved_canonical_path=bool(manifest.get("approved_canonical_path", False)),
                promotable=bool(manifest.get("promotable", False)),
            )
    out_dir.mkdir(parents=True, exist_ok=True)
    observations: list[CanonicalObservation] = []
    diagnostics_summary: dict[str, int] = {}
    sampler = CanonicalOneSecondSipSampler(run_id=run_id)
    for window, info in zip(windows, files, strict=True):
        config = ReplayConfig(
            run_id=run_id,
            trader_id=trader_id,
            symbol=symbol,
            start=window.start_utc,
            end=window.end_utc,
            timezone="UTC",
            session_mode="regular",
            strategy_config={"symbol": symbol, "brick_size": 1, "entry_bricks": 1, "qty": 1},
            initial_capital="10000",
            brick_mode="BRICK_DOLLARS",
            brick_size_dollars="1",
            source_data_path=info.path,
        )
        for item in stream_historical_source(config):
            if item.diagnostic is not None:
                diagnostics_summary[item.diagnostic.diagnostic_type] = diagnostics_summary.get(item.diagnostic.diagnostic_type, 0) + 1
                continue
            if item.trade is None:
                continue
            result = sampler.process_trade(item.trade)
            for diagnostic in result.diagnostics:
                diagnostics_summary[diagnostic.diagnostic_type] = diagnostics_summary.get(diagnostic.diagnostic_type, 0) + 1
            observations.extend(result.observations)
    observations.sort(key=lambda item: (item.second_bucket, item.emitted_sequence, item.observation_id))
    _write_jsonl(observations_path, observations)
    pending = sampler.pending_diagnostics()
    if pending:
        diagnostics_summary["pending_final_sampler_trade"] = diagnostics_summary.get("pending_final_sampler_trade", 0) + len(pending)
    combined_hash = _combined_hash(files)
    stream_contract = market_stream_contract(symbol=symbol, session_timezone="America/New_York")
    data_quality = "OK" if not diagnostics_summary else "WARNINGS"
    session_summary = [
        {
            "session_date": window.session_date.isoformat(),
            "start_utc": window.start_utc.isoformat(),
            "end_utc": window.end_utc.isoformat(),
        }
        for window in windows
    ]
    manifest = {
        "preparation_id": preparation_id,
        "preparation_version": PREPARATION_VERSION,
        "symbol": symbol.upper(),
        "session_dates": [item.isoformat() for item in session_dates],
        "source_files": [asdict(item) for item in files],
        "source_hashes": [item.sha256 for item in files],
        "combined_source_hash": combined_hash,
        "canonical_filter_version": FILTER_VERSION,
        "canonical_sampler_version": SAMPLER_VERSION,
        "market_stream_contract": asdict(stream_contract),
        "market_stream_fingerprint": stream_contract.fingerprint,
        "final_flush_policy": "NO_FLUSH",
        "session_policy": "explicit_regular_utc",
        "session_summary": session_summary,
        "observation_count": len(observations),
        "diagnostics_summary": diagnostics_summary,
        "data_quality_status": data_quality,
        "approved_canonical_path": True,
        "promotable": data_quality in {"OK", "WARNINGS"},
    }
    (out_dir / "manifest.json").write_text(json.dumps(_jsonable(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out_dir / "diagnostics_summary.json").write_text(json.dumps(diagnostics_summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out_dir / "session_summary.json").write_text(json.dumps(session_summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return PreparedSource(
        preparation_id,
        symbol.upper(),
        session_dates,
        files,
        combined_hash,
        len(observations),
        str(out_dir),
        str(observations_path),
        diagnostics_summary,
        data_quality,
        market_stream_contract=asdict(stream_contract),
        approved_canonical_path=True,
        promotable=data_quality in {"OK", "WARNINGS"},
    )


def load_prepared_observations(path: str | Path) -> tuple[CanonicalObservation, ...]:
    rows: list[CanonicalObservation] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            rows.append(CanonicalObservation(**json.loads(line)))
    return tuple(rows)


def _preparation_id(*, symbol: str, session_dates: tuple[date, ...], files: tuple[SourceFileInfo, ...], windows: tuple[SessionWindow, ...]) -> str:
    payload = {
        "version": PREPARATION_VERSION,
        "symbol": symbol.upper(),
        "session_dates": [item.isoformat() for item in session_dates],
        "source_hashes": [item.sha256 for item in files],
        "windows": [(item.start_utc.isoformat(), item.end_utc.isoformat()) for item in windows],
        "filter": FILTER_VERSION,
        "sampler": SAMPLER_VERSION,
        "final_flush_policy": "NO_FLUSH",
    }
    return "prep_" + sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:24]


def _combined_hash(files: tuple[SourceFileInfo, ...]) -> str:
    return sha256("|".join(str(item.sha256) for item in files).encode("utf-8")).hexdigest()


def _write_jsonl(path: Path, rows: list[Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_jsonable(row), sort_keys=True) + "\n")


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {key: _jsonable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value
