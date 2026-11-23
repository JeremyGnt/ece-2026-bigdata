import os
import json
import subprocess
from pyspark.sql import SparkSession

creds = json.loads(subprocess.check_output([
    "aws", "configure", "export-credentials",
    "--profile", "default", "--format", "process"
], text=True))

spark = (
    SparkSession.builder
    .appName("trino-lab3-comparison")
    .master("local[2]")
    .config("spark.hadoop.fs.s3a.endpoint",
            "https://s3.seaweedfs.adm.adaltas.cloud")
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.access.key", creds["AccessKeyId"])
    .config("spark.hadoop.fs.s3a.secret.key", creds["SecretAccessKey"])
    .config("spark.hadoop.fs.s3a.session.token", creds.get("SessionToken", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.polaris",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri",
            os.environ["POLARIS_URI"] + "/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse",
            os.environ["POLARIS_CATALOG"])
    .config("spark.sql.catalog.polaris.credential",
            os.environ["POLARIS_DEV_CREDENTIAL"])
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Recalculate with Spark from Silver
spark_result = spark.sql("""
    SELECT
        u.state,
        date_trunc('month', o.ordered_at) AS month,
        count(*) AS orders,
        sum(o.quantity) AS units,
        count(DISTINCT o.user_id) AS buyers
    FROM polaris.silver.orders o
    JOIN polaris.silver.users u ON o.user_id = u.user_id
    GROUP BY u.state, date_trunc('month', o.ordered_at)
""")

# Read Gold created by Trino
trino_result = spark.table(
    "polaris.gold.orders_by_state_month"
)

spark_count = spark_result.count()
trino_count = trino_result.count()

differences = (
    spark_result.exceptAll(trino_result).count()
    + trino_result.exceptAll(spark_result).count()
)

print("Spark rows:", spark_count)
print("Trino Gold rows:", trino_count)
print("Differences:", differences)

assert spark_count == trino_count
assert differences == 0

print("Exercise 5 completed successfully.")
spark.stop()
