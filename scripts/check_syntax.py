from __future__ import annotations

from pathlib import Path


EXCLUDED_DIRS = {
    ".venv",
    "_legacy_quarantine",
    "__pycache__",
}


def iter_python_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*.py"):
        if any(part in EXCLUDED_DIRS for part in path.relative_to(root).parts):
            continue
        files.append(path)
    return sorted(files)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    for path in iter_python_files(root):
        source = path.read_text(encoding="utf-8")
        compile(source, str(path), "exec")
    print(f"Syntax OK: {len(iter_python_files(root))} Python files")


if __name__ == "__main__":
    main()
