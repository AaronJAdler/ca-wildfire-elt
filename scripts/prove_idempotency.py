"""Run the pipeline's failure scenarios and record what happened in docs/proof/idempotency.json.

Each scenario states a claim and tries to break it. The last two confirm the tests can really fail,
because a test that cannot fail proves nothing.

    uv run python scripts/prove_idempotency.py
    uv run python scripts/prove_idempotency.py --end 2021-08-31   # faster, partial season
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import duckdb
from fingerprint import fingerprint

from wildfire.cli import extract, replay, run_dbt, transform
from wildfire.paths import PROJECT_ROOT, Paths

START = dt.date(2021, 7, 15)


def first_real_attribute_change(paths: Paths) -> dt.date:
    """Return the first date, after an incident's first appearance, that a version began."""
    with duckdb.connect(str(paths.warehouse), read_only=True) as connection:
        row = connection.execute(
            """
            with versions as (
                select
                    valid_from,
                    row_number() over (partition by unique_id order by valid_from) as version_number
                from main.dim_incident
            )
            select cast(min(valid_from) as date) from versions where version_number > 1
            """
        ).fetchone()
    assert row is not None and row[0] is not None  # noqa: S101
    return row[0]


def build_fails(day: dt.date, paths: Paths) -> bool:
    try:
        transform(day, paths)
    except subprocess.CalledProcessError:
        return True
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--end", type=dt.date.fromisoformat, default=dt.date(2021, 10, 15))
    args = parser.parse_args()
    results: dict[str, object] = {"window": [START.isoformat(), args.end.isoformat()]}
    claims: dict[str, bool] = {}

    with tempfile.TemporaryDirectory() as temporary:
        paths = Paths(data_dir=Path(temporary) / "main")
        started = time.perf_counter()
        replay(START, args.end, paths)
        results["replay_seconds"] = round(time.perf_counter() - started)
        baseline = fingerprint(paths.warehouse)
        results["baseline"] = baseline
        change_day = first_real_attribute_change(paths)
        results["first_attribute_change"] = change_day.isoformat()

        # A rerun from before a real attribute change is the hardest case for the SCD2 snapshot.
        before_change = change_day - dt.timedelta(days=1)
        for day in (before_change, change_day, args.end):
            extract(day, paths)
            transform(day, paths)
            claims[f"rerun_{day.isoformat()}_changes_nothing"] = (
                fingerprint(paths.warehouse) == baseline
            )

        shutil.rmtree(paths.bronze_dir / f"dt={before_change.isoformat()}")
        extract(before_change, paths)
        transform(before_change, paths)
        claims["lost_bronze_partition_can_be_restored"] = fingerprint(paths.warehouse) == baseline

        run_dbt(paths, "build", "--select", "fct_incident_daily+", "--full-refresh", "--quiet")
        claims["full_refresh_matches_day_by_day_build"] = fingerprint(paths.warehouse) == baseline

        # Negative check: skip the day before an attribute change, and the build must fail.
        skipped = Paths(data_dir=Path(temporary) / "skipped_day")
        extract(before_change, skipped)
        extract(change_day, skipped)
        claims["skipping_a_day_fails_the_build"] = build_fails(change_day, skipped)

        # Negative check: a fan-out in the fact must fail the build.
        with duckdb.connect(str(paths.warehouse)) as connection:
            connection.execute(
                """
                insert into main.dim_incident
                select md5(incident_version_sk || 'duplicate'), * exclude (incident_version_sk)
                from main.dim_incident
                """
            )
        try:
            run_dbt(paths, "test", "--select", "dim_incident", "--quiet")
            claims["duplicated_dimension_rows_fail_the_tests"] = False
        except subprocess.CalledProcessError:
            claims["duplicated_dimension_rows_fail_the_tests"] = True

    results["claims"] = claims
    output = PROJECT_ROOT / "docs" / "proof"
    output.mkdir(parents=True, exist_ok=True)
    text = json.dumps(results, indent=2, default=str) + "\n"
    (output / "idempotency.json").write_text(text)
    print(text)
    if not all(claims.values()):
        raise SystemExit("At least one claim failed. See docs/proof/idempotency.json.")


if __name__ == "__main__":
    main()
