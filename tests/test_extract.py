"""Failure-mode tests for the ingestion layer. Each one names a way pipelines really break."""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

import duckdb
import pytest

from wildfire.extract import QualityGateError, Snapshot, load_day
from wildfire.paths import Paths

DAY = dt.date(2021, 8, 20)


def incident(unique_id: str = "a", **overrides: Any) -> dict[str, Any]:
    base = {
        "UniqueId": unique_id,
        "Name": "Test Fire",
        "Started": "2021-08-01T00:00:00Z",
        "Updated": "2021-08-20T00:00:00Z",
        "Counties": ["Plumas"],
        "AcresBurned": 100,
        "PercentContained": 10,
    }
    return {**base, **overrides}


class StubSource:
    def __init__(self, incidents: list[dict[str, Any]] | None) -> None:
        self.incidents = incidents

    def fetch(self, day: dt.date) -> Snapshot | None:
        if self.incidents is None:
            return None
        return Snapshot(day, "abc123", "2021-08-20T23:00:00+00:00", 1.0, self.incidents)


@pytest.fixture
def paths(tmp_path: Path) -> Paths:
    return Paths(data_dir=tmp_path)


def partition(paths: Paths) -> Path:
    return paths.bronze_dir / f"dt={DAY.isoformat()}"


def bronze_rows(paths: Paths) -> list[tuple[Any, ...]]:
    query = "select unique_id, raw_json from read_parquet(?) order by unique_id"
    return duckdb.sql(query, params=[str(partition(paths) / "data.parquet")]).fetchall()


def test_rerun_replaces_the_day_without_duplicates(paths: Paths) -> None:
    first = load_day(DAY, StubSource([incident("a"), incident("b")]), paths)
    before = bronze_rows(paths)
    second = load_day(DAY, StubSource([incident("a"), incident("b")]), paths)
    assert first.content_sha256 == second.content_sha256
    assert bronze_rows(paths) == before
    assert len(before) == 2


def test_output_does_not_depend_on_feed_order(paths: Paths) -> None:
    first = load_day(DAY, StubSource([incident("a"), incident("b")]), paths)
    second = load_day(DAY, StubSource([incident("b"), incident("a")]), paths)
    assert first.content_sha256 == second.content_sha256


def test_missing_snapshot_is_reported_and_writes_nothing(paths: Paths) -> None:
    result = load_day(DAY, StubSource(None), paths)
    assert result.status == "no_snapshot"
    assert not partition(paths).exists()


def test_empty_feed_cannot_overwrite_good_data(paths: Paths) -> None:
    load_day(DAY, StubSource([incident("a")]), paths)
    before = bronze_rows(paths)
    with pytest.raises(QualityGateError):
        load_day(DAY, StubSource([]), paths)
    assert bronze_rows(paths) == before


def test_mostly_invalid_feed_is_blocked(paths: Paths) -> None:
    bad = [{"UniqueId": str(i)} for i in range(3)]
    with pytest.raises(QualityGateError):
        load_day(DAY, StubSource([incident("a"), *bad]), paths)
    assert not (partition(paths) / "data.parquet").exists()


def test_few_invalid_records_are_quarantined(paths: Paths) -> None:
    good = [incident(str(i)) for i in range(9)]
    result = load_day(DAY, StubSource([*good, {"UniqueId": "bad"}]), paths)
    assert (result.rows, result.rejected) == (9, 1)
    quarantine = paths.quarantine_dir / f"dt={DAY.isoformat()}.json"
    assert json.loads(quarantine.read_text())[0]["record"]["UniqueId"] == "bad"


def test_clean_rerun_clears_stale_quarantine(paths: Paths) -> None:
    good = [incident(str(i)) for i in range(9)]
    load_day(DAY, StubSource([*good, {"UniqueId": "bad"}]), paths)
    load_day(DAY, StubSource(good), paths)
    assert not (paths.quarantine_dir / f"dt={DAY.isoformat()}.json").exists()


def test_duplicate_ids_in_a_snapshot_keep_the_first(paths: Paths) -> None:
    good = [incident(str(i)) for i in range(9)]
    result = load_day(DAY, StubSource([*good, incident("0", Name="Duplicate")]), paths)
    assert (result.rows, result.rejected) == (9, 1)
    assert all("Duplicate" not in raw for _, raw in bronze_rows(paths))


def test_new_source_field_is_kept_and_reported(paths: Paths) -> None:
    load_day(DAY, StubSource([incident("a", NewField="surprise")]), paths)
    manifest = json.loads((partition(paths) / "_manifest.json").read_text())
    assert manifest["fields_not_in_contract"] == ["NewField"]
    assert "surprise" in bronze_rows(paths)[0][1]


def test_crash_during_publish_leaves_previous_partition_readable(
    paths: Paths, monkeypatch: pytest.MonkeyPatch
) -> None:
    load_day(DAY, StubSource([incident("a")]), paths)
    before = bronze_rows(paths)

    def crash(source: Any, destination: Any) -> None:
        raise OSError("simulated crash before the atomic rename")

    monkeypatch.setattr(os, "replace", crash)
    with pytest.raises(OSError, match="simulated crash"):
        load_day(DAY, StubSource([incident("a"), incident("b")]), paths)
    monkeypatch.undo()

    assert bronze_rows(paths) == before
    load_day(DAY, StubSource([incident("a"), incident("b")]), paths)
    assert len(bronze_rows(paths)) == 2
