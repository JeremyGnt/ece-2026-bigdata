# PySpark Silver - Results

## Bronze
- Users: 50
- Orders: 2829

## Silver
- Users: 50
- Orders after deduplication: 2829

## Validation
- DataFrame API and Spark SQL produce identical orders.
- No records lost during Parquet export.
- Duplicated orders removed by row_number().
- Addresses parsed into street, city, state and ZIP code.

## Storage
- s3://user-j-gonnet-ece/silver/spark/users.parquet/
- s3://user-j-gonnet-ece/silver/spark/orders.parquet/

## Conclusion
Spark DataFrames provide typed transformations,
deduplication and distributed Parquet storage.
The SQL implementation produces equivalent results.
