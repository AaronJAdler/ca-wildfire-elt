"""Load one logical day of the source feed into the bronze layer.

Guarantees:
- Idempotent: a day's output depends only on the logical date, so a rerun replaces it with
  identical content.
- Atomic: readers see the previous partition or the complete new one, never a partial file.
- Immutable history: raw records are kept as JSON, so new logic can replay old days.
"""

from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import duckdb
from pydantic import BaseModel, ConfigDict, ValidationError

from wildfire.paths import Paths

log = logging.getLogger(__name__)

MAX_REJECTED_SHARE = 0.20


class QualityGateError(RuntimeError):
    """The day's data is unsafe to publish, so its partition is left unchanged."""


class IncidentRecord(BaseModel):
    """The fields downstream models depend on. Unknown fields are allowed and reported."""

    model_config = ConfigDict(extra="allow", strict=False)

    UniqueId: str
    Name: str
    Started: str
    Updated: str
    Counties: list[str]
    AcresBurned: float | None
    PercentContained: float | None


@dataclass(frozen=True)
class Snapshot:
    day: dt.date
    source_commit: str
    source_committed_at: str
    feed_all_acres: float | None
    incidents: list[dict[str, Any]]


class Source(Protocol):
    def fetch(self, day: dt.date) -> Snapshot | None:
        """Return the day's snapshot, or None if the source published nothing that day."""


class ReplaySource:
    """Replays recorded daily feed snapshots as if they were arriving live."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def fetch(self, day: dt.date) -> Snapshot | None:
        path = self.directory / f"{day.isoformat()}.json.gz"
        if not path.exists():
            return None
        document = json.loads(gzip.decompress(path.read_bytes()))
        return Snapshot(
            day=day,
            source_commit=document["source_commit"],
            source_committed_at=document["source_committed_at"],
            feed_all_acres=document.get("feed_all_acres"),
            incidents=document["incidents"],
        )


@dataclass(frozen=True)
class LoadResult:
    day: dt.date
    status: str  # "loaded" or "no_snapshot"
    rows: int = 0
    rejected: int = 0
    content_sha256: str | None = None


def _validate(
    snapshot: Snapshot,
) -> tuple[list[tuple[str, dict[str, Any]]], list[dict[str, Any]], set[str]]:
    accepted: dict[str, dict[str, Any]] = {}
    rejected: list[dict[str, Any]] = []
    new_fields: set[str] = set()
    for raw in snapshot.incidents:
        try:
            record = IncidentRecord.model_validate(raw)
        except ValidationError as error:
            rejected.append({"record": raw, "reason": error.errors(include_url=False)})
            continue
        if record.UniqueId in accepted:
            rejected.append({"record": raw, "reason": "duplicate UniqueId within snapshot"})
            continue
        accepted[record.UniqueId] = raw
        new_fields.update(record.model_extra or {})
    # Sorted output makes the partition's content independent of feed ordering.
    return sorted(accepted.items()), rejected, new_fields


def _content_hash(rows: list[tuple[str, dict[str, Any]]]) -> str:
    digest = hashlib.sha256()
    for unique_id, raw in rows:
        digest.update(unique_id.encode())
        digest.update(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode())
    return digest.hexdigest()


def _publish_atomically(directory: Path, rows_path: Path, manifest: dict[str, Any]) -> None:
    manifest_tmp = directory / "_manifest.json.tmp"
    manifest_tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    os.replace(rows_path, directory / "data.parquet")
    os.replace(manifest_tmp, directory / "_manifest.json")


def load_day(day: dt.date, source: Source, paths: Paths) -> LoadResult:
    snapshot = source.fetch(day)
    if snapshot is None:
        log.warning("source published no snapshot for %s", day)
        return LoadResult(day=day, status="no_snapshot")

    rows, rejected, new_fields = _validate(snapshot)
    total = len(rows) + len(rejected)
    if not rows:
        raise QualityGateError(f"{day}: snapshot has no valid incidents")
    if len(rejected) / total > MAX_REJECTED_SHARE:
        raise QualityGateError(f"{day}: {len(rejected)} of {total} records failed validation")

    directory = paths.bronze_dir / f"dt={day.isoformat()}"
    directory.mkdir(parents=True, exist_ok=True)
    content_sha = _content_hash(rows)
    tmp = directory / ".data.parquet.tmp"
    with duckdb.connect() as connection:
        connection.execute(
            """
            create table incidents (
                snapshot_date date, unique_id varchar, raw_json varchar,
                source_commit varchar, source_committed_at timestamptz
            )
            """
        )
        connection.executemany(
            "insert into incidents values (?, ?, ?, ?, ?)",
            [
                (
                    day,
                    unique_id,
                    json.dumps(raw, sort_keys=True, separators=(",", ":")),
                    snapshot.source_commit,
                    snapshot.source_committed_at,
                )
                for unique_id, raw in rows
            ],
        )
        connection.execute(
            "copy (select * from incidents order by unique_id) to ? "
            "(format parquet, compression zstd)",
            [str(tmp)],
        )

    _publish_atomically(
        directory,
        tmp,
        {
            "snapshot_date": day.isoformat(),
            "rows": len(rows),
            "rejected": len(rejected),
            "content_sha256": content_sha,
            "source_commit": snapshot.source_commit,
            "feed_all_acres": snapshot.feed_all_acres,
            "fields_not_in_contract": sorted(new_fields),
        },
    )

    quarantine = paths.quarantine_dir / f"dt={day.isoformat()}.json"
    if rejected:
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        quarantine.write_text(json.dumps(rejected, indent=2, default=str))
        log.warning("%s: quarantined %d records at %s", day, len(rejected), quarantine)
    else:
        quarantine.unlink(missing_ok=True)

    log.info("%s: loaded %d rows", day, len(rows))
    return LoadResult(
        day=day, status="loaded", rows=len(rows), rejected=len(rejected), content_sha256=content_sha
    )
