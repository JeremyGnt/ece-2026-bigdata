# PySpark Silver Lab

## Objective
Transform Bronze CSV datasets into a cleaned Silver layer
using Spark DataFrames and Spark SQL.

## Implementation
The silver.py script:
- Connects Spark to Onyxia S3 storage.
- Reads Bronze CSV files with multiline support.
- Cleans and standardizes user attributes.
- Extracts street, city, state and ZIP code.
- Converts order columns to appropriate types.
- Deduplicates orders with a window function.
- Implements the equivalent transformation in Spark SQL.
- Validates DataFrame and SQL equivalence.
- Writes Silver data as Parquet to S3.
- Reads Parquet files back to verify row counts.

## DataFrame vs Spark SQL
DataFrames provide a programmatic transformation pipeline.
Spark SQL expresses transformations declaratively.

Both use Spark's Catalyst optimizer.

## Parquet
Spark writes directories containing partitioned Parquet files.
This supports distributed processing and parallel reads.

## Results
See results.md for execution results.
