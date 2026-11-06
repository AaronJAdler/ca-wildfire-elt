#!/usr/bin/env bash
# Slim CI: builds the base commit on sample data, then builds and tests only what changed.
#
#   scripts/ci_changed_models.sh <base-commit>
#
# Run it from a clean checkout. It adds a temporary worktree for the base commit and removes it.
set -euo pipefail

BASE_SHA="$1"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SAMPLE_START=2021-07-15
SAMPLE_END="${CI_SAMPLE_END:-2021-07-20}"
export WILDFIRE_DATA_DIR="${WILDFIRE_DATA_DIR:-$ROOT/.ci-data}"
BASE_DIR="$(mktemp -d)"
trap 'git -C "$ROOT" worktree remove --force "$BASE_DIR/worktree" 2>/dev/null || true; rm -rf "$BASE_DIR"' EXIT

git -C "$ROOT" worktree add --quiet --detach "$BASE_DIR/worktree" "$BASE_SHA"

# The base commit's own code builds the warehouse the pull request will be compared with.
(cd "$BASE_DIR/worktree" && uv sync --frozen --quiet \
  && uv run --frozen wildfire replay --start "$SAMPLE_START" --end "$SAMPLE_END")

# Its manifest tells dbt what "modified" means. It is produced with the data directory above.
BASE_MANIFEST_DIR="$WILDFIRE_DATA_DIR/dbt_target"

cd "$ROOT"
uv run --frozen dbt build \
  --select state:modified+ \
  --state "$BASE_MANIFEST_DIR" \
  --defer \
  --vars "{\"run_date\": \"$SAMPLE_END\"}" \
  --target-path "$BASE_DIR/pr_target" \
  --project-dir dbt --profiles-dir dbt
