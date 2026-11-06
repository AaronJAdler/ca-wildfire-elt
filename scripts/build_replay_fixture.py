"""Rebuild data/replay/ from a clone of github.com/simonw/ca-fires-history.

That repository recorded CAL FIRE's incident feed as Git commits. For each UTC day in the
window, this script keeps the day's last commit, which is the state of the feed at day end.

Long narrative HTML is removed to keep the fixture small. Every other field stays as published.

    git clone --filter=blob:none --no-checkout https://github.com/simonw/ca-fires-history
    python scripts/build_replay_fixture.py ca-fires-history
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import subprocess
from pathlib import Path

DROPPED_FIELDS = {
    "ConditionStatement",
    "ControlStatement",
    "SearchDescription",
    "SearchKeywords",
    "ExternalIncidentLink",
}
DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "data" / "replay"


def git(repo: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, check=True)
    return result.stdout.decode()


def last_commit_per_day(repo: Path) -> dict[str, tuple[str, str]]:
    latest: dict[str, tuple[str, str]] = {}
    log = git(repo, "log", "--format=%H|%cI", "--", "incidents.json")
    for line in log.splitlines():  # newest first, so the first commit seen for a day wins
        commit, committed_at = line.split("|")
        moment = dt.datetime.fromisoformat(committed_at).astimezone(dt.UTC)
        latest.setdefault(moment.date().isoformat(), (commit, moment.isoformat()))
    return latest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", type=Path)
    parser.add_argument("--start", default="2021-07-15")
    parser.add_argument("--end", default="2021-10-15")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    written = 0
    for day, (commit, committed_at) in sorted(last_commit_per_day(args.repo).items()):
        if not args.start <= day <= args.end:
            continue
        feed = json.loads(git(args.repo, "show", f"{commit}:incidents.json"))
        incidents = [
            {key: value for key, value in incident.items() if key not in DROPPED_FIELDS}
            for incident in feed["Incidents"]
        ]
        document = {
            "source_commit": commit,
            "source_committed_at": committed_at,
            "feed_all_acres": feed.get("AllAcres"),
            "incidents": incidents,
        }
        body = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
        # mtime=0 makes the archive byte-identical on every rebuild.
        (args.output / f"{day}.json.gz").write_bytes(gzip.compress(body, 9, mtime=0))
        written += 1
    print(f"Wrote {written} daily snapshots to {args.output}")


if __name__ == "__main__":
    main()
