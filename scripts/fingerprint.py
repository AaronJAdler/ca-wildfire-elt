"""Print a row count and content hash for every warehouse table, so two runs can be compared."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb

TABLES = [
    "main.dim_county",
    "main.dim_date",
    "main.dim_incident",
    "main.bridge_incident_county",
    "main.fct_incident_daily",
    "main.mart_incident_season",
    "main.mart_statewide_daily",
    "snapshots.snap_incident",
]


def fingerprint(warehouse: Path) -> dict[str, dict[str, object]]:
    results: dict[str, dict[str, object]] = {}
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        for table in TABLES:
            # Table names come from the constant above, never from input.
            query = (
                "select count(*), md5(coalesce(string_agg(t::varchar, '|' order by t::varchar), '')) "  # noqa: S608, E501
                f"from {table} t"
            )
            row = connection.execute(query).fetchone()
            assert row is not None  # noqa: S101
            results[table] = {"rows": row[0], "md5": row[1]}
    return results


if __name__ == "__main__":
    print(json.dumps(fingerprint(Path(sys.argv[1])), indent=2))
