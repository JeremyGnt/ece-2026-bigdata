# PySpark Silver - Iceberg and Exercises

## Exercise 1 - Birthdate validation
- Invalid birthdates: 5
- DataFrame and Spark SQL results are identical.
- Invalid birthdates are replaced with NULL.
- Same validation rule as the dbt lab.

## Exercise 2 - ZIP extraction timings
- regexp_extract: 0.826 seconds
- python_udf: 2.659 seconds
- pandas_udf: 1.543 seconds

## Exercise 3 - Bronze caching
- users (50 rows): uncached=0.436s, first cached=0.677s, reused=0.252s
- orders (2829 rows): uncached=0.362s, first cached=0.594s, reused=0.388s
- Cached partitions can be reused by later Spark actions.

## Exercise 4 - Iceberg snapshots
- Orders before append: 2829
- Orders after append: 2830
- Historical orders: 2829
- Iceberg snapshots: 4
- Parquet count before append: 2829
- Parquet count after append: 2830
- Iceberg supports querying the previous snapshot.
- A plain Parquet directory has no built-in time travel.

### Iceberg snapshot history
- Snapshot 8058452145057502167: overwrite (2026-11-06 10:10:00.504000)
- Snapshot 3536285090955177950: append (2026-11-06 10:10:03.053000)
- Snapshot 2218576078450301949: overwrite (2026-11-06 10:16:12.146000)
- Snapshot 4445558926149202586: append (2026-11-06 10:16:15.335000)

## Comparison with dbt
- Spark orders: 2829
- Spark total quantity: 8615
- dbt orders: 2829
- dbt total quantity: 8615
- Aggregates match: True

### Technical differences
- Spark uses string IDs; DuckDB supports UUID.
- Spark uses DataFrame dependencies; dbt uses source() and ref().
- Spark Parquet writes partitioned files.
- DuckDB views store SQL rather than transformed rows.
- dbt provides built-in generic data tests.
- Equivalent Spark assertions must be written explicitly.
- Build times depend on engine, hardware, caching and I/O.

### Recommended use cases
- Daily 10 GB: DuckDB/dbt can be sufficient on one machine.
- Daily 10 TB: distributed Spark is generally more suitable.
- SQL-only analysts: dbt provides an accessible workflow.
