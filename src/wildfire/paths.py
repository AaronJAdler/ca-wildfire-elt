"""Filesystem locations, overridable so tests and CI never touch local data."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Paths:
    data_dir: Path
    replay_dir: Path = PROJECT_ROOT / "data" / "replay"

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze" / "incidents"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine" / "incidents"

    @property
    def warehouse(self) -> Path:
        return self.data_dir / "warehouse.duckdb"


def default_paths() -> Paths:
    default = PROJECT_ROOT / "data" / "local"
    return Paths(data_dir=Path(os.environ.get("WILDFIRE_DATA_DIR", default)))
