import os
import json
import subprocess
import time
import uuid
import re
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

BUCKET = os.environ["LAB_BUCKET_NAME"]
PROFILE = os.environ.get("AWS_PROFILE", "default")

creds = json.loads(subprocess.check_output([
    "aws", "configure", "export-credentials",
    "--profile", PROFILE, "--format", "process"
], text=True))

endpoint = os.environ.get(
    "AWS_S3_ENDPOINT", "s3.seaweedfs.adm.adaltas.cloud"
)
if not endpoint.startswith("http"):
    endpoint = "https://" + endpoint

spark = (
    SparkSession.builder
    .appName("lab-silver-iceberg-exercises")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.access.key", creds["AccessKeyId"])
    .config("spark.hadoop.fs.s3a.secret.key", creds["SecretAccessKey"])
    .config("spark.hadoop.fs.s3a.session.token", creds.get("SessionToken", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
        if creds.get("SessionToken")
        else "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config(
        "spark.sql.catalog.lab.warehouse",
        f"s3a://{BUCKET}/warehouse"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

BASE = f"s3a://{BUCKET}"
users = spark.read.parquet(f"{BASE}/silver/spark/users.parquet")
orders = spark.read.parquet(f"{BASE}/silver/spark/orders.parquet")
report = ["# PySpark Silver - Iceberg and Exercises"]

# Exercise 1: birthdate validation with DataFrames.
first_orders = orders.groupBy("user_id").agg(
    F.min(F.to_date("ordered_at")).alias("first_order_date")
)

joined = users.join(first_orders, "user_id", "left")

valid = (
    F.when(F.col("birthdate").isNull(), F.lit(False))
    .when(F.col("first_order_date").isNull(), F.lit(True))
    .otherwise(F.col("birthdate") <= F.col("first_order_date"))
)

users_checked = (
    joined.withColumn("birthdate_is_valid", valid)
    .withColumn(
        "birthdate",
        F.when(F.col("birthdate_is_valid"), F.col("birthdate"))
         .otherwise(F.lit(None).cast("date"))
    )
    .drop("first_order_date")
)

# Same validation using Spark SQL.
users.createOrReplaceTempView("silver_users")
orders.createOrReplaceTempView("silver_orders")

users_sql = spark.sql("""
WITH first_orders AS (
    SELECT user_id, min(to_date(ordered_at)) AS first_order_date
    FROM silver_orders GROUP BY user_id
),
checked AS (
    SELECT u.*, CASE
        WHEN u.birthdate IS NULL THEN FALSE
        WHEN o.first_order_date IS NULL THEN TRUE
        ELSE u.birthdate <= o.first_order_date
    END AS birthdate_is_valid
    FROM silver_users u
    LEFT JOIN first_orders o ON u.user_id = o.user_id
)
SELECT user_id, username, name, sex, email,
       CASE WHEN birthdate_is_valid THEN birthdate
            ELSE CAST(NULL AS DATE) END AS birthdate,
       street, city, state, zip_code, birthdate_is_valid
FROM checked
""")

cols = sorted(users_checked.columns)
assert users_checked.select(*cols).exceptAll(
    users_sql.select(*cols)
).count() == 0

assert users_sql.select(*cols).exceptAll(
    users_checked.select(*cols)
).count() == 0

invalid_birthdates = users_checked.filter(
    ~F.col("birthdate_is_valid")
).count()

report += [
    "",
    "## Exercise 1 - Birthdate validation",
    f"- Invalid birthdates: {invalid_birthdates}",
    "- DataFrame and Spark SQL results are identical.",
    "- Invalid birthdates are replaced with NULL.",
    "- Same validation rule as the dbt lab."
]

from pyspark.sql.functions import udf, pandas_udf

users_bronze = spark.read.csv(
    f"{BASE}/bronze/users.csv",
    header=True, inferSchema=True, multiLine=True
)
orders_bronze = spark.read.csv(
    f"{BASE}/bronze/orders.csv",
    header=True, inferSchema=True, multiLine=True
)

def elapsed(action):
    start = time.perf_counter()
    result = action()
    return round(time.perf_counter() - start, 3), result

# Exercise 3: cache both Bronze DataFrames.
cache_stats = []
for name, df in [
    ("users", users_bronze),
    ("orders", orders_bronze)
]:
    uncached_time, n = elapsed(df.count)
    df.cache()
    first_time, _ = elapsed(df.count)
    second_time, _ = elapsed(df.count)
    cache_stats.append((name, n, uncached_time,
                        first_time, second_time))

# Exercise 2: ZIP extraction using three approaches.
def extract_zip(address):
    m = re.search(r"([A-Z]{2}) ([0-9]{5})$", address or "")
    return m.group(2) if m else ""

python_zip = udf(extract_zip, "string")

@pandas_udf("string")
def pandas_zip(addresses):
    parts = addresses.str.extract(
        r"([A-Z]{2}) ([0-9]{5})$", expand=True
    )
    return parts[1].fillna("")

regex_zip = F.regexp_extract(
    "address", r"([A-Z]{2}) ([0-9]{5})$", 2
)

expressions = [
    ("regexp_extract", regex_zip),
    ("python_udf", python_zip("address")),
    ("pandas_udf", pandas_zip("address"))
]

results = []
for name, expr in expressions:
    duration, total = elapsed(
        lambda e=expr: users_bronze.select(
            e.alias("zip")
        ).agg(F.sum(F.length("zip"))).first()[0]
    )
    results.append((name, duration, total))

assert len({value for _, _, value in results}) == 1

report += ["", "## Exercise 2 - ZIP extraction timings"]
for name, duration, _ in results:
    report.append(f"- {name}: {duration} seconds")

report += ["", "## Exercise 3 - Bronze caching"]
for name, n, no_cache, first, second in cache_stats:
    report.append(
        f"- {name} ({n} rows): uncached={no_cache}s, "
        f"first cached={first}s, reused={second}s"
    )
report.append(
    "- Cached partitions can be reused by later Spark actions."
)

# Iceberg tables
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")

users_checked.coalesce(1).writeTo(
    "lab.silver.users"
).createOrReplace()

orders.coalesce(1).writeTo(
    "lab.silver.orders"
).createOrReplace()

before_count = spark.table("lab.silver.orders").count()

before_snapshot = spark.sql("""
    SELECT snapshot_id
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at DESC
    LIMIT 1
""").first()[0]

# Exercise 4: append one unique order.
new_order = orders.limit(1).withColumn(
    "order_id", F.lit(str(uuid.uuid4()))
)

new_order.writeTo("lab.silver.orders").append()

after_count = spark.table("lab.silver.orders").count()

historical_count = spark.sql(f"""
    SELECT count(*) AS n
    FROM lab.silver.orders VERSION AS OF {int(before_snapshot)}
""").first()[0]

assert after_count == before_count + 1
assert historical_count == before_count

snapshots = spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at
""").collect()

# Parquet has no built-in snapshot history.
parquet_path = f"{BASE}/silver/spark/orders_ex4.parquet"

orders.coalesce(1).write.mode("overwrite").parquet(parquet_path)
parquet_before = spark.read.parquet(parquet_path).count()

new_order.coalesce(1).write.mode("append").parquet(parquet_path)
parquet_after = spark.read.parquet(parquet_path).count()

assert parquet_after == parquet_before + 1

report += [
    "",
    "## Exercise 4 - Iceberg snapshots",
    f"- Orders before append: {before_count}",
    f"- Orders after append: {after_count}",
    f"- Historical orders: {historical_count}",
    f"- Iceberg snapshots: {len(snapshots)}",
    f"- Parquet count before append: {parquet_before}",
    f"- Parquet count after append: {parquet_after}",
    "- Iceberg supports querying the previous snapshot.",
    "- A plain Parquet directory has no built-in time travel.",
    "",
    "### Iceberg snapshot history"
]

for row in snapshots:
    report.append(
        f"- Snapshot {row.snapshot_id}: "
        f"{row.operation} ({row.committed_at})"
    )

# Compare with the previous dbt / DuckDB lab.
dbt_path = (
    Path(__file__).resolve().parent.parent
    / "lab_medallion/lab_medallion.duckdb"
)

spark_count = orders.count()
spark_quantity = orders.agg(F.sum("quantity")).first()[0]

report += [
    "",
    "## Comparison with dbt",
    f"- Spark orders: {spark_count}",
    f"- Spark total quantity: {spark_quantity}"
]

if dbt_path.exists():
    try:
        import duckdb
        con = duckdb.connect(str(dbt_path), read_only=True)
        dbt_count, dbt_quantity = con.execute("""
            SELECT count(*), sum(quantity)
            FROM main_silver.stg_orders
        """).fetchone()
        con.close()
        report += [
            f"- dbt orders: {dbt_count}",
            f"- dbt total quantity: {dbt_quantity}",
            f"- Aggregates match: "
            f"{(spark_count, spark_quantity) == (dbt_count, dbt_quantity)}"
        ]
    except Exception:
        report.append("- dbt runtime comparison unavailable.")
else:
    report.append("- Local dbt database unavailable.")

report += [
    "",
    "### Technical differences",
    "- Spark uses string IDs; DuckDB supports UUID.",
    "- Spark uses DataFrame dependencies; dbt uses source() and ref().",
    "- Spark Parquet writes partitioned files.",
    "- DuckDB views store SQL rather than transformed rows.",
    "- dbt provides built-in generic data tests.",
    "- Equivalent Spark assertions must be written explicitly.",
    "- Build times depend on engine, hardware, caching and I/O.",
    "",
    "### Recommended use cases",
    "- Daily 10 GB: DuckDB/dbt can be sufficient on one machine.",
    "- Daily 10 TB: distributed Spark is generally more suitable.",
    "- SQL-only analysts: dbt provides an accessible workflow."
]

output = Path(__file__).resolve().parent / "iceberg_results.md"
output.write_text("\n".join(report) + "\n", encoding="utf-8")

users_bronze.unpersist()
orders_bronze.unpersist()

print("Iceberg exercises completed successfully.")
spark.stop()
