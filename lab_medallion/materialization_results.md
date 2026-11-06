# Exercise 5 - Materialization Experiment

Comparison of dbt view and table materializations
using DuckDB and S3 Bronze data.

## Measurements

| Materialization | Build time (s) | Database size (bytes) |
|---|---:|---:|
| view | 12 | 798720 |
| table | 10 | 1323008 |

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
