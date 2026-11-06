import os
import configparser
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

BUCKET = os.environ.get("LAB_BUCKET_NAME", "user-j-gonnet-ece")
PROFILE = os.environ.get("AWS_PROFILE", "default")

# Read temporary AWS credentials from local profile.
# Never store credentials in the Git repository.
config = configparser.ConfigParser()
config.read(Path.home() / ".aws" / "credentials")
if config.has_section(PROFILE):
    credentials = config[PROFILE]
else:
    import json
    import subprocess
    data = json.loads(subprocess.check_output([
        "aws", "configure", "export-credentials",
        "--profile", PROFILE, "--format", "process"
    ], text=True))
    credentials = {
        "aws_access_key_id": data["AccessKeyId"],
        "aws_secret_access_key": data["SecretAccessKey"],
        "aws_session_token": data.get("SessionToken", "")
    }

endpoint = os.environ.get(
    "AWS_S3_ENDPOINT",
    "s3.seaweedfs.adm.adaltas.cloud"
)
if not endpoint.startswith("http"):
    endpoint = "https://" + endpoint

provider = (
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    if credentials.get("aws_session_token")
    else "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

builder = (
    SparkSession.builder
    .appName("lab-pyspark-silver")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
    .config("spark.hadoop.fs.s3a.access.key",
            credentials["aws_access_key_id"])
    .config("spark.hadoop.fs.s3a.secret.key",
            credentials["aws_secret_access_key"])
    .config("spark.hadoop.fs.s3a.session.token",
            credentials.get("aws_session_token", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", provider)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.shuffle.partitions", "2")
)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Bronze: read CSV directly from S3.
users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv",
    header=True,
    inferSchema=True,
    samplingRatio=0.01,
    multiLine=True
)

orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv",
    header=True,
    inferSchema=True,
    samplingRatio=0.01,
    multiLine=True
)

# Silver users: standardization and address parsing.
users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines")
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
)

stg_users = users_typed.select(
    "user_id", "username", "name", "sex",
    "email", "birthdate", "street",
    F.regexp_extract(
        "address_line_2", r"^(.+), [A-Z]{2} [0-9]{5}$", 1
    ).alias("city"),
    F.regexp_extract(
        "address_line_2", r"([A-Z]{2}) ([0-9]{5})$", 1
    ).alias("state"),
    F.regexp_extract(
        "address_line_2", r"([A-Z]{2}) ([0-9]{5})$", 2
    ).alias("zip_code")
)

stg_users = stg_users.withColumn(
    "city",
    F.when(F.col("city") == "", None).otherwise(F.col("city"))
)

# Silver orders: type conversion and deduplication.
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product")
)

# Inject three duplicates to demonstrate deduplication.
orders_with_duplicates = orders_typed.unionByName(
    orders_typed.limit(3)
)

window = Window.partitionBy("order_id").orderBy(
    F.col("ordered_at").desc()
)

stg_orders = (
    orders_with_duplicates
    .withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# Equivalent deduplication with Spark SQL.
orders_with_duplicates.createOrReplaceTempView("orders_typed")

stg_orders_sql = spark.sql("""
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *,
            row_number() OVER (
                PARTITION BY order_id
                ORDER BY ordered_at DESC
            ) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
""")

# Check equivalence between DataFrame API and SQL.
columns = sorted(stg_orders.columns)
df_result = stg_orders.select(*columns)
sql_result = stg_orders_sql.select(*columns)

assert df_result.exceptAll(sql_result).count() == 0
assert sql_result.exceptAll(df_result).count() == 0

# Write silver datasets to S3 as Parquet.
stg_users.write.mode("overwrite").parquet(
    f"s3a://{BUCKET}/silver/spark/users.parquet"
)

stg_orders.write.mode("overwrite").parquet(
    f"s3a://{BUCKET}/silver/spark/orders.parquet"
)

# Read back and verify.
users_silver = spark.read.parquet(
    f"s3a://{BUCKET}/silver/spark/users.parquet"
)

orders_silver = spark.read.parquet(
    f"s3a://{BUCKET}/silver/spark/orders.parquet"
)

users_count = users_bronze.count()
orders_count = orders_bronze.count()
silver_users_count = users_silver.count()
silver_orders_count = orders_silver.count()

assert users_count == silver_users_count
assert orders_count == silver_orders_count

# Generate a report for GitHub.
report = f"""# PySpark Silver - Results

## Bronze
- Users: {users_count}
- Orders: {orders_count}

## Silver
- Users: {silver_users_count}
- Orders after deduplication: {silver_orders_count}

## Validation
- DataFrame API and Spark SQL produce identical orders.
- No records lost during Parquet export.
- Duplicated orders removed by row_number().
- Addresses parsed into street, city, state and ZIP code.

## Storage
- s3://{BUCKET}/silver/spark/users.parquet/
- s3://{BUCKET}/silver/spark/orders.parquet/

## Conclusion
Spark DataFrames provide typed transformations,
deduplication and distributed Parquet storage.
The SQL implementation produces equivalent results.
"""

output = Path(__file__).resolve().parent / "results.md"
output.write_text(report, encoding="utf-8")

print("Spark Silver completed successfully.")
spark.stop()

