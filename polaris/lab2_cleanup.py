import os
import json
import subprocess
from pathlib import Path

from pyspark.sql import SparkSession

BUCKET = os.environ["LAB_BUCKET_NAME"]
PROFILE = os.environ.get("AWS_PROFILE", "default")
URI = os.environ["POLARIS_URI"]
CATALOG = os.environ["POLARIS_CATALOG"]
DEV = os.environ["POLARIS_DEV_CREDENTIAL"]

creds = json.loads(subprocess.check_output([
    "aws", "configure", "export-credentials",
    "--profile", PROFILE, "--format", "process"
], text=True))

endpoint = "https://s3.seaweedfs.adm.adaltas.cloud"
report = ["# Polaris - Lab 2 Results", ""]

spark = (
    SparkSession.builder
    .appName("polaris-lab2-cleanup")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.hadoop.fs.s3a.endpoint", endpoint)
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
    .config("spark.sql.catalog.lab",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse",
            f"s3a://{BUCKET}/warehouse")
    .config("spark.sql.catalog.polaris",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri",
            f"{URI}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", CATALOG)
    .config("spark.sql.catalog.polaris.credential", DEV)
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# Keep orders for the upcoming Trino lab.
orders_before = spark.table("polaris.silver.orders").count()

spark.sql(
    "DROP TABLE IF EXISTS polaris.gold.access_probe"
)

spark.sql(
    "DROP NAMESPACE IF EXISTS polaris.gold"
)

spark.sql(
    "DROP TABLE IF EXISTS polaris.silver.users"
)

orders_after = spark.table("polaris.silver.orders").count()
assert orders_after == orders_before

tables = [
    r.tableName for r in
    spark.sql("SHOW TABLES IN polaris.silver").collect()
]

assert "orders" in tables
assert "users" not in tables

lines = [
    "# Polaris - Cleanup Results",
    "",
    "- gold.access_probe dropped.",
    "- gold namespace dropped.",
    "- polaris.silver.users dropped.",
    "- polaris.silver.orders preserved.",
    f"- Orders remaining: {orders_after}",
    "- Catalog is ready for the Trino lab."
]

output = Path(__file__).parent / "lab2_cleanup_results.md"
output.write_text("\n".join(lines) + "\n", encoding="utf-8")

print("Polaris cleanup completed successfully.")
spark.stop()
