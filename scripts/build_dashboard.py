"""Build docs/dashboard.html from the warehouse marts. The output has no server or CDN dependency.

uv run python scripts/build_dashboard.py [path/to/warehouse.duckdb]
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = Path(__file__).with_name("dashboard_template.html")
OUTPUT = ROOT / "docs" / "dashboard.html"


def rows(connection: duckdb.DuckDBPyConnection, query: str) -> list[dict[str, object]]:
    result = connection.execute(query)
    columns = [column[0] for column in result.description]
    return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def load(warehouse: Path) -> dict[str, object]:
    with duckdb.connect(str(warehouse), read_only=True) as connection:
        top_fires = rows(
            connection,
            """
            select unique_id, incident_name, county_names, peak_acres, latest_percent_contained,
                   strftime(first_seen_date, '%Y-%m-%d') as first_seen_date, days_in_feed,
                   peak_daily_growth_acres,
                   strftime(peak_growth_date, '%Y-%m-%d') as peak_growth_date,
                   days_from_start_to_50_percent_contained,
                   share_of_days_carried_forward, peak_structures_destroyed
            from main.mart_incident_season
            where peak_acres is not null
            order by peak_acres desc
            limit 10
            """,
        )
        series_ids = ", ".join(f"'{fire['unique_id']}'" for fire in top_fires[:4])
        return {
            "topFires": top_fires,
            "fireDaily": rows(
                connection,
                f"""
                select unique_id, strftime(snapshot_date, '%Y-%m-%d') as date,
                       acres_burned as acres, percent_contained as contained,
                       acres_is_carried_forward as acres_carried,
                       containment_is_carried_forward as contained_carried
                from main.fct_incident_daily
                where unique_id in ({series_ids})
                order by snapshot_date
                """,  # noqa: S608
            ),
            "statewide": rows(
                connection,
                """
                select strftime(calendar_date, '%Y-%m-%d') as date, has_feed_snapshot,
                       incidents_in_feed, tracked_acres_to_date, fires_with_known_acreage,
                       acres_added
                from main.mart_statewide_daily
                order by calendar_date
                """,
            ),
            "quality": rows(
                connection,
                """
                select
                    count(*) as fact_rows,
                    count(distinct unique_id) as incidents,
                    count(distinct snapshot_date) as snapshot_days,
                    sum(case when acres_reported is null then 1 else 0 end) as null_acres,
                    sum(case when acres_is_carried_forward then 1 else 0 end) as carried_acres,
                    sum(case when acres_burned is null then 1 else 0 end) as unknown_acres,
                    (select count(*) from main.dim_date where not has_feed_snapshot)
                        as missing_days,
                    (select count(distinct unique_id) from main.fct_incident_daily
                     where acres_burned is not null) as incidents_with_acres,
                    (select count(*) from main.dim_incident) as dim_versions
                from main.fct_incident_daily
                """,
            )[0],
        }


def main() -> None:
    default = (
        Path(os.environ.get("WILDFIRE_DATA_DIR", ROOT / "data" / "local")) / "warehouse.duckdb"
    )
    warehouse = Path(sys.argv[1]) if len(sys.argv) > 1 else default
    data = json.dumps(load(warehouse), default=float, separators=(",", ":"))
    # Keeps a closing script tag in source data from ending the embedded JSON block.
    safe_data = data.replace("</", "<\\/")
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    template = TEMPLATE.read_text(encoding="utf-8")
    OUTPUT.write_text(template.replace("__DATA__", safe_data), encoding="utf-8")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
