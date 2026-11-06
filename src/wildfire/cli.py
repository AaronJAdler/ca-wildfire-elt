"""Command-line entry points. The same commands run locally, in CI, and from Airflow."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import shutil
import subprocess
import sys

from wildfire.extract import ReplaySource, load_day
from wildfire.paths import PROJECT_ROOT, Paths, default_paths

# Airflow's BashOperator marks a task skipped on this exit code.
EXIT_NO_SNAPSHOT = 99
DBT_DIR = PROJECT_ROOT / "dbt"


def run_dbt(paths: Paths, *arguments: str) -> None:
    paths.data_dir.mkdir(parents=True, exist_ok=True)
    executable = shutil.which("dbt")
    if executable is None:
        sys.exit("dbt was not found. Run this command with: uv run wildfire ...")
    environment = {
        **os.environ,
        "WILDFIRE_DATA_DIR": paths.data_dir.as_posix(),
        # Keeps dbt's state with its warehouse, so separate warehouses can build at the same time.
        "DBT_TARGET_PATH": (paths.data_dir / "dbt_target").as_posix(),
        "DBT_LOG_PATH": (paths.data_dir / "dbt_logs").as_posix(),
    }
    command = [
        executable,
        *arguments,
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
    ]
    subprocess.run(command, check=True, env=environment)


def extract(day: dt.date, paths: Paths) -> int:
    result = load_day(day, ReplaySource(paths.replay_dir), paths)
    print(json.dumps({**result.__dict__, "day": day.isoformat()}))
    return EXIT_NO_SNAPSHOT if result.status == "no_snapshot" else 0


def transform(day: dt.date, paths: Paths) -> None:
    """Build everything for one logical date, in dependency order, stopping on a failed test."""
    run_dbt(paths, "build", "--vars", json.dumps({"run_date": day.isoformat()}), "--quiet")


def replay(start: dt.date, end: dt.date, paths: Paths, run_transform: bool = True) -> None:
    """Replay a date range in order, exactly as the daily schedule would have run it."""
    day = start
    while day <= end:
        if extract(day, paths) == EXIT_NO_SNAPSHOT:
            print(f"{day}: no snapshot, skipping transform")
        elif run_transform:
            transform(day, paths)
        day += dt.timedelta(days=1)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(prog="wildfire", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    for name in ("extract", "transform"):
        command = commands.add_parser(name)
        command.add_argument("--date", type=dt.date.fromisoformat, required=True)

    command = commands.add_parser("replay", help="run a date range in order")
    command.add_argument("--start", type=dt.date.fromisoformat, default=dt.date(2021, 7, 15))
    command.add_argument("--end", type=dt.date.fromisoformat, default=dt.date(2021, 10, 15))

    args = parser.parse_args()
    paths = default_paths()

    if args.command == "extract":
        sys.exit(extract(args.date, paths))
    elif args.command == "transform":
        transform(args.date, paths)
    elif args.command == "replay":
        replay(args.start, args.end, paths)


if __name__ == "__main__":
    main()
