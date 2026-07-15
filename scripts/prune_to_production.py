from __future__ import annotations

import argparse
import shutil
from pathlib import Path


ROOT_KEEP_FILES = {
    ".env",
    ".env.example",
    "Makefile",
    "PRODUCTION_MANIFEST.md",
    "VERSION_CONTRACT.md",
    "alpaca_trader.py",
    "deploy-trading",
    "execution_engine.py",
    "fleet_runner.py",
    "mission-control.service",
    "monitoring_app.py",
    "requirements.txt",
    "renko_builder.py",
    "strategy_engine.py",
    "trading-fleet-live.service",
    "trading_events.py",
    "validated_renko_backtest.cpp",
}

CONFIG_KEEP_FILES = {
    "configs/traders.live.example.json",
}

SCRIPT_KEEP_FILES = {
    "scripts/check_syntax.py",
    "scripts/manual_flatten_profiles.py",
    "scripts/prune_to_production.py",
}

TEST_KEEP_FILES = {
    "tests/test_alpaca_trader.py",
    "tests/test_execution_engine.py",
    "tests/test_renko_anchor.py",
    "tests/test_strategy_engine.py",
}

ROOT_KEEP_DIRS = {
    ".venv",
    ".vscode",
    "artifacts",
    "configs",
    "scripts",
    "tests",
}

ROOT_QUARANTINE_DIRS = {
    "__pycache__",
    "data",
    "strategies",
}


def unique_target(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 10_000):
        candidate = path.with_name(f"{path.name}.{index}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Could not find unused quarantine target for {path}")


def collect_moves(root: Path, legacy_dir: Path) -> list[tuple[Path, Path]]:
    moves: list[tuple[Path, Path]] = []
    keep_rel = ROOT_KEEP_FILES | CONFIG_KEEP_FILES | SCRIPT_KEEP_FILES | TEST_KEEP_FILES

    for path in sorted(root.iterdir(), key=lambda item: item.name):
        if path == legacy_dir or path.name == legacy_dir.name:
            continue
        rel = path.relative_to(root).as_posix()
        if path.is_file():
            if rel not in keep_rel:
                moves.append((path, legacy_dir / rel))
            continue
        if not path.is_dir():
            continue
        if path.name in ROOT_QUARANTINE_DIRS:
            moves.append((path, legacy_dir / rel))
            continue
        if path.name not in ROOT_KEEP_DIRS:
            moves.append((path, legacy_dir / rel))

    for parent, allowed in (
        (root / "configs", CONFIG_KEEP_FILES),
        (root / "scripts", SCRIPT_KEEP_FILES),
        (root / "tests", TEST_KEEP_FILES),
    ):
        if not parent.exists():
            continue
        for path in sorted(parent.iterdir(), key=lambda item: item.name):
            rel = path.relative_to(root).as_posix()
            if rel not in allowed:
                moves.append((path, legacy_dir / rel))

    return moves


def apply_moves(moves: list[tuple[Path, Path]]) -> None:
    for source, target in moves:
        target = unique_target(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source), str(target))


def recreate_production_dirs(root: Path) -> None:
    (root / "artifacts" / "live").mkdir(parents=True, exist_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Move non-production files into a reversible legacy quarantine."
    )
    parser.add_argument("--apply", action="store_true", help="Move files instead of only printing the plan.")
    parser.add_argument("--legacy-dir", default="_legacy_quarantine")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    legacy_dir = root / args.legacy_dir
    moves = collect_moves(root, legacy_dir)

    print(f"Production prune root: {root}")
    print(f"Legacy quarantine: {legacy_dir}")
    print(f"Mode: {'apply' if args.apply else 'dry-run'}")
    print(f"Planned moves: {len(moves)}")
    for source, target in moves:
        print(f"{source.relative_to(root)} -> {target.relative_to(root)}")

    if not args.apply:
        print("Dry run only. Re-run with --apply to quarantine these paths.")
        return

    apply_moves(moves)
    recreate_production_dirs(root)
    print("Quarantine complete. Run: make compile && make test")


if __name__ == "__main__":
    main()
