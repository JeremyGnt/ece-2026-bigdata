# Lab - Introduction to Spark RDD API

## Objectives
Explore partitions, lazy evaluation, RDD transformations,
aggregations, deduplication, joins and caching.

## Main operations
- RDD creation with textFile()
- repartition() and partition count
- Lazy evaluation and lineage
- Handling multiline CSV records
- Manual order parsing
- reduceByKey vs groupByKey
- Manual order deduplication
- Joining orders with users

## Exercise 1
aggregateByKey calculates order count and total quantity.
Average quantity = total quantity / order count.

## Exercise 2
Inject a fake user ID and use leftOuterJoin to detect
orders without matching users.

## Exercise 3
Compare reduceByKey with DataFrame groupBy().sum().
RDD lineage and DataFrame physical plans are recorded.

## Exercise 4
Compare count() and aggregation execution times
with and without caching.

## Observations
- RDD transformations are lazy.
- Actions trigger execution.
- reduceByKey performs local aggregation before shuffling.
- groupByKey moves individual values across partitions.
- Joins may require expensive shuffles.
- Caching helps when reusing the same RDD.
- textFile cannot reconstruct multiline CSV records.

## Files
- lab_rdd.py: implementation and exercises
- results.md: execution results and measurements
