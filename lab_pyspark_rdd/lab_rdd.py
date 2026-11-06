import re
import io
import time
from pathlib import Path
from operator import add
from contextlib import redirect_stdout
from pyspark.sql import SparkSession

root = Path(__file__).resolve().parent.parent
spark = SparkSession.builder.master("local[*]").appName("lab-rdd").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

users_lines = sc.textFile(str(root / "users.csv"))
orders_lines = sc.textFile(str(root / "orders.csv"))
header = orders_lines.first()

orders_data = orders_lines.filter(
    lambda line: line != header and line.strip() != ""
)

users_records = users_lines.filter(
    lambda line: re.match(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        line
    ) is not None
)

def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return dict(order_id=order_id, user_id=user_id, date=date,
                quantity=int(quantity), product=product.strip().lower())

orders_rdd = orders_data.map(parse_order).cache()
n_orders = orders_rdd.count()
n_users = users_records.count()

pairs = orders_rdd.map(lambda o: (o["user_id"], o["quantity"]))

reduced = dict(pairs.reduceByKey(add).collect())
grouped = dict(pairs.groupByKey().mapValues(sum).collect())
assert reduced == grouped

stats = dict(pairs.aggregateByKey(
    (0, 0),
    lambda a, q: (a[0] + 1, a[1] + q),
    lambda a, b: (a[0] + b[0], a[1] + b[1])
).collect())

averages = {u: total / count for u, (count, total) in stats.items()}
assert {u: total for u, (count, total) in stats.items()} == reduced

dedup = (orders_rdd
    .map(lambda o: (o["order_id"], o))
    .reduceByKey(lambda a, b: a if a["date"] > b["date"] else b)
    .values())

users_kv = users_records.map(
    lambda line: (line.split(",")[0], line.split(",")[1])
)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
joined_count = orders_kv.join(users_kv).count()

fake = dict(orders_rdd.first())
fake["user_id"] = "00000000-0000-0000-0000-000000000000"
fake["order_id"] = "fake-order-rdd"

augmented = orders_rdd.union(sc.parallelize([fake]))
missing = (augmented.map(lambda o: (o["user_id"], o))
    .leftOuterJoin(users_kv)
    .filter(lambda row: row[1][1] is None).count())

assert missing == 1

df = spark.createDataFrame(
    orders_rdd.map(lambda o: (o["user_id"], o["quantity"])),
    ["user_id", "quantity"]
)

df_result = df.groupBy("user_id").sum("quantity")
df_totals = {r[0]: r[1] for r in df_result.collect()}
assert df_totals == reduced

buffer = io.StringIO()
with redirect_stdout(buffer):
    df_result.explain(mode="formatted")

df_plan = buffer.getvalue()
rdd_plan = pairs.reduceByKey(add).toDebugString().decode()

def timed(f):
    start = time.perf_counter()
    f()
    return round(time.perf_counter() - start, 4)

orders_rdd.unpersist(blocking=True)

uncached = orders_data.map(parse_order)
time_uncached = timed(uncached.count)
time_uncached_agg = timed(
    lambda: uncached.map(
        lambda o: (o["user_id"], o["quantity"])
    ).reduceByKey(add).count()
)

cached = orders_data.map(parse_order).cache()
time_cached_first = timed(cached.count)
time_cached_second = timed(cached.count)
time_cached_agg = timed(
    lambda: cached.map(
        lambda o: (o["user_id"], o["quantity"])
    ).reduceByKey(add).count()
)

report = f"""# PySpark RDD Lab - Results

## RDD creation
- Users partitions: {users_lines.getNumPartitions()}
- Orders partitions: {orders_lines.getNumPartitions()}
- Repartitioned orders: {orders_lines.repartition(8).getNumPartitions()}
- Physical user lines: {users_lines.count()}
- Valid users: {n_users}
- Orders: {n_orders}

## Transformations
- Deduplicated orders: {dedup.count()}
- Joined records: {joined_count}
- reduceByKey and groupByKey: identical results

## Exercise 1 - aggregateByKey
- Users aggregated: {len(stats)}
- Average quantities sample: {list(averages.items())[:5]}

## Exercise 2 - leftOuterJoin
- Orders with missing users after injecting fake user: {missing}

## Exercise 3 - DataFrame API
- DataFrame aggregation matches RDD aggregation: True

### RDD lineage
~~~text
{rdd_plan}
~~~

### DataFrame explain
~~~text
{df_plan}
~~~

## Exercise 4 - Cache comparison

| Operation | Seconds |
|---|---:|
| Count without cache | {time_uncached} |
| Aggregation without cache | {time_uncached_agg} |
| First count with cache | {time_cached_first} |
| Second count with cache | {time_cached_second} |
| Aggregation with cache | {time_cached_agg} |

The first cached action computes and stores partitions.
Later actions can reuse cached partitions.
Timings depend on execution conditions.
"""

output = root / "lab_pyspark_rdd" / "results.md"
output.write_text(report, encoding="utf-8")

cached.unpersist()
spark.stop()
print("Lab completed successfully.")
