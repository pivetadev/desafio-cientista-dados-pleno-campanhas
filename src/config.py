from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    repo_root: Path
    data_raw: Path
    data_processed: Path


def get_paths(repo_root: str | Path | None = None) -> Paths:
    root = Path(repo_root) if repo_root is not None else Path(__file__).resolve().parents[1]
    return Paths(
        repo_root=root,
        data_raw=root / "data" / "raw",
        data_processed=root / "data" / "processed",
    )

