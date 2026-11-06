# PySpark RDD Lab - Results

## RDD creation
- Users partitions: 2
- Orders partitions: 2
- Repartitioned orders: 8
- Physical user lines: 101
- Valid users: 50
- Orders: 2829

## Transformations
- Deduplicated orders: 2829
- Joined records: 2829
- reduceByKey and groupByKey: identical results

## Exercise 1 - aggregateByKey
- Users aggregated: 50
- Average quantities sample: [('f143262f-dc5c-4eed-8da0-365bf89897b9', 3.0), ('969b6662-0562-4059-968c-c69b1064005c', 3.3333333333333335), ('ce9e1a11-fcbb-4e59-bbdd-cf7c9c96e9ec', 2.976470588235294), ('5563f616-00e8-4ece-8b49-452d46d483f3', 2.7142857142857144), ('f94d6204-6808-493f-9fed-2c43e256a6dc', 3.5714285714285716)]

## Exercise 2 - leftOuterJoin
- Orders with missing users after injecting fake user: 1

## Exercise 3 - DataFrame API
- DataFrame aggregation matches RDD aggregation: True

### RDD lineage
~~~text
(2) PythonRDD[61] at RDD at PythonRDD.scala:58 []
 |  MapPartitionsRDD[60] at mapPartitions at PythonRDD.scala:170 []
 |  ShuffledRDD[59] at partitionBy at NativeMethodAccessorImpl.java:0 []
 +-(2) PairwiseRDD[58] at reduceByKey at /home/onyxia/work/ece-2026-bigdata/lab_pyspark_rdd/lab_rdd.py:89 []
    |  PythonRDD[57] at reduceByKey at /home/onyxia/work/ece-2026-bigdata/lab_pyspark_rdd/lab_rdd.py:89 []
    |  PythonRDD[5] at RDD at PythonRDD.scala:58 []
    |      CachedPartitions: 2; MemorySize: 168.0 KiB; DiskSize: 0.0 B
    |  /home/onyxia/work/ece-2026-bigdata/orders.csv MapPartitionsRDD[3] at textFile at NativeMethodAccessorImpl.java:0 []
    |  /home/onyxia/work/ece-2026-bigdata/orders.csv HadoopRDD[2] at textFile at NativeMethodAccessorImpl.java:0 []
~~~

### DataFrame explain
~~~text
== Physical Plan ==
AdaptiveSparkPlan (10)
+- == Final Plan ==
   ResultQueryStage (7)
   +- * HashAggregate (6)
      +- AQEShuffleRead (5)
         +- ShuffleQueryStage (4), Statistics(sizeInBytes=3.2 KiB, rowCount=51)
            +- Exchange (3)
               +- * HashAggregate (2)
                  +- * Scan ExistingRDD (1)
+- == Initial Plan ==
   HashAggregate (9)
   +- Exchange (8)
      +- HashAggregate (2)
         +- Scan ExistingRDD (1)


(1) Scan ExistingRDD [codegen id : 1]
Output [2]: [user_id#0, quantity#1L]
Arguments: [user_id#0, quantity#1L], MapPartitionsRDD[51] at applySchemaToPythonRDD at NativeMethodAccessorImpl.java:0, ExistingRDD, UnknownPartitioning(0)

(2) HashAggregate [codegen id : 1]
Input [2]: [user_id#0, quantity#1L]
Keys [1]: [user_id#0]
Functions [1]: [partial_sum(quantity#1L)]
Aggregate Attributes [1]: [sum#6L]
Results [2]: [user_id#0, sum#7L]

(3) Exchange
Input [2]: [user_id#0, sum#7L]
Arguments: hashpartitioning(user_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=20]

(4) ShuffleQueryStage
Output [2]: [user_id#0, sum#7L]
Arguments: 0

(5) AQEShuffleRead
Input [2]: [user_id#0, sum#7L]
Arguments: coalesced

(6) HashAggregate [codegen id : 2]
Input [2]: [user_id#0, sum#7L]
Keys [1]: [user_id#0]
Functions [1]: [sum(quantity#1L)]
Aggregate Attributes [1]: [sum(quantity#1L)#4L]
Results [2]: [user_id#0, sum(quantity#1L)#4L AS sum(quantity)#5L]

(7) ResultQueryStage
Output [2]: [user_id#0, sum(quantity)#5L]
Arguments: 1

(8) Exchange
Input [2]: [user_id#0, sum#7L]
Arguments: hashpartitioning(user_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=11]

(9) HashAggregate
Input [2]: [user_id#0, sum#7L]
Keys [1]: [user_id#0]
Functions [1]: [sum(quantity#1L)]
Aggregate Attributes [1]: [sum(quantity#1L)#4L]
Results [2]: [user_id#0, sum(quantity#1L)#4L AS sum(quantity)#5L]

(10) AdaptiveSparkPlan
Output [2]: [user_id#0, sum(quantity)#5L]
Arguments: isFinalPlan=true



~~~

## Exercise 4 - Cache comparison

| Operation | Seconds |
|---|---:|
| Count without cache | 0.2066 |
| Aggregation without cache | 0.4719 |
| First count with cache | 0.2125 |
| Second count with cache | 0.1119 |
| Aggregation with cache | 0.3776 |

The first cached action computes and stores partitions.
Later actions can reuse cached partitions.
Timings depend on execution conditions.
