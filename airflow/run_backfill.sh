#!/usr/bin/env bash
# Backfills the season through a real Airflow scheduler, then prints each run's state.
# Linux, macOS, or WSL2. Airflow does not run natively on Windows.
#
#   airflow/run_backfill.sh [from-date] [to-date]
#
# Airflow gets its own virtual environment because its pins conflict with dbt's. Tasks call the
# project's environment through `uv run`, so the two never share dependencies.
set -euo pipefail

FROM_DATE="${1:-2021-07-15}"
TO_DATE="${2:-2021-10-15}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="${WILDFIRE_AIRFLOW_WORK_DIR:-$ROOT/data/local/airflow}"
AIRFLOW_VERSION=3.3.2
PYTHON_VERSION=3.13

export WILDFIRE_DATA_DIR="${WILDFIRE_DATA_DIR:-$ROOT/data/local}"
export AIRFLOW_HOME="$WORK_DIR/home"
export AIRFLOW__CORE__DAGS_FOLDER="$ROOT/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__PARALLELISM=4
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False

mkdir -p "$WORK_DIR"
if [ ! -x "$WORK_DIR/venv/bin/airflow" ]; then
  uv venv --python "$PYTHON_VERSION" "$WORK_DIR/venv"
  uv pip install --python "$WORK_DIR/venv/bin/python" \
    "apache-airflow==$AIRFLOW_VERSION" apache-airflow-providers-standard \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt"
fi
# `airflow standalone` starts its components by name, so its venv must be on the PATH.
export PATH="$WORK_DIR/venv/bin:$PATH"
AIRFLOW=airflow

# A new process group lets cleanup stop the scheduler, API server, and all their workers together.
setsid "$AIRFLOW" standalone >"$WORK_DIR/standalone.log" 2>&1 &
SERVER_PID=$!
trap 'kill -- "-$SERVER_PID" 2>/dev/null || true' EXIT

SECONDS=0
until grep -q "Airflow is ready" "$WORK_DIR/standalone.log" \
    && "$AIRFLOW" dags list 2>/dev/null | grep -q wildfire_daily; do
  kill -0 "$SERVER_PID" || { echo "Airflow stopped. See $WORK_DIR/standalone.log"; exit 1; }
  [ "$SECONDS" -lt 300 ] || { echo "The DAG never appeared. See $WORK_DIR/standalone.log"; exit 1; }
  sleep 5
done

# SQLite allows one writer, so creating the backfill can briefly collide with the scheduler.
created=false
for attempt in 1 2 3 4 5; do
  if "$AIRFLOW" backfill create --dag-id wildfire_daily \
      --from-date "$FROM_DATE" --to-date "$TO_DATE" --max-active-runs 1 \
      --reprocess-behavior failed; then
    created=true
    break
  fi
  echo "Creating the backfill failed on attempt $attempt. Retrying."
  sleep 15
done
"$created" || { echo "Could not create the backfill."; exit 1; }

while "$AIRFLOW" dags list-runs wildfire_daily --output json \
    | grep -qE '"state": "(queued|running)"'; do
  sleep 15
done

"$AIRFLOW" dags list-runs wildfire_daily --output json >"$WORK_DIR/runs.json"
echo "Run states:"
grep -oE '"state": "[a-z_]+"' "$WORK_DIR/runs.json" | sort | uniq -c
! grep -q '"state": "failed"' "$WORK_DIR/runs.json"
