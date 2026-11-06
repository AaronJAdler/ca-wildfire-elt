"""End-to-end checks on a small real sample. scripts/prove_idempotency.py runs the full season."""

from __future__ import annotations

import datetime as dt
import subprocess
import sys
from pathlib import Path

import pytest

from wildfire.cli import extract, replay, transform
from wildfire.paths import PROJECT_ROOT, Paths

sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
from fingerprint import fingerprint  # noqa: E402

pytestmark = pytest.mark.integration

START = dt.date(2021, 7, 15)
END = dt.date(2021, 7, 18)
FIRST_ATTRIBUTE_CHANGE = dt.date(2021, 7, 17)


@pytest.fixture(scope="module")
def built(tmp_path_factory: pytest.TempPathFactory) -> Paths:
    paths = Paths(data_dir=tmp_path_factory.mktemp("warehouse"))
    replay(START, END, paths)
    return paths


def test_rerunning_a_past_day_changes_nothing(built: Paths) -> None:
    before = fingerprint(built.warehouse)
    day = START + dt.timedelta(days=1)
    extract(day, built)
    transform(day, built)
    assert fingerprint(built.warehouse) == before


def test_skipping_a_day_fails_the_build(tmp_path: Path) -> None:
    paths = Paths(data_dir=tmp_path)
    extract(FIRST_ATTRIBUTE_CHANGE - dt.timedelta(days=1), paths)
    extract(FIRST_ATTRIBUTE_CHANGE, paths)
    with pytest.raises(subprocess.CalledProcessError):
        transform(FIRST_ATTRIBUTE_CHANGE, paths)


def test_a_day_with_no_snapshot_is_reported(tmp_path: Path) -> None:
    assert extract(dt.date(2021, 10, 5), Paths(data_dir=tmp_path)) == 99
