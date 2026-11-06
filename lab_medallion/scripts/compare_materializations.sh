#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

backup=$(mktemp)
cp dbt_project.yml "$backup"

trap 'cp "$backup" dbt_project.yml; rm -f "$backup"' EXIT

cat > materialization_results.md <<'MD'
# Exercise 5 - Materialization Experiment

Comparison of dbt view and table materializations
using DuckDB and S3 Bronze data.

## Measurements

| Materialization | Build time (s) | Database size (bytes) |
|---|---:|---:|
MD

for mode in view table; do
    sed -i -E \
      "s/(\+materialized: )(view|table)/\1${mode}/" \
      dbt_project.yml

    rm -f lab_medallion.duckdb

    uv run dbt seed \
      --select states zip_state_reference > /dev/null

    start=$(date +%s)

    if ! uv run dbt build --select silver \
      > "/tmp/dbt_${mode}_build.log" 2>&1; then
        cat "/tmp/dbt_${mode}_build.log"
        exit 1
    fi

    duration=$(($(date +%s) - start))
    size=$(stat -c %s lab_medallion.duckdb)

    echo "| $mode | $duration | $size |" \
      >> materialization_results.md
done

cat >> materialization_results.md <<'MD'

## Analysis

### View
- Stores the SQL definition.
- Executes the transformation when queried.
- Reflects changes in the underlying CSV on future queries.
- Does not store the transformed result as a table.

### Table
- Stores the transformed data in DuckDB.
- Executes the transformation during dbt build.
- Must be rebuilt to reflect changes to the source CSV.
- Requires storage for the materialized result.

## Conclusion

Views are useful when current source data is needed
and queries are relatively inexpensive.

Tables can improve repeated query performance,
but require storage and a refresh strategy.

Build timings are measurements from one run and
include dbt execution overhead. They are not
isolated query-performance benchmarks.

The dbt project configuration is restored to
view materialization after this experiment.
MD

echo "Experiment completed."
