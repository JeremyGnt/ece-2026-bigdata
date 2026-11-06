import os
import json
import subprocess
from pathlib import Path

from pyspark.sql import SparkSession

BUCKET = os.environ["LAB_BUCKET_NAME"]
PROFILE = os.environ.get("AWS_PROFILE", "default")
URI = os.environ["POLARIS_URI"]
CATALOG = os.environ["POLARIS_CATALOG"]
DEV = os.environ["POLARIS_SILVER_READER_CREDENTIAL"]

creds = json.loads(subprocess.check_output([
    "aws", "configure", "export-credentials",
    "--profile", PROFILE, "--format", "process"
], text=True))

endpoint = "https://s3.seaweedfs.adm.adaltas.cloud"
report = ["# Polaris - Lab 2 Results", ""]

spark = (
    SparkSession.builder
    .appName("polaris-silver-reader")
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


orders_count = spark.sql("""
    SELECT COUNT(*) AS n FROM polaris.silver.orders
""").first()[0]

assert orders_count == 2830

print("SILVER READ: ALLOWED")
print("Orders:", orders_count)

try:
    spark.sql("""
        SELECT COUNT(*) FROM polaris.gold.access_probe
    """).collect()
except Exception as exc:
    message = str(exc).lower()
    expected = (
        "forbidden", "403", "unauthorized",
        "permission", "not authorized",
        "nosuchtable", "nosuchnamespace",
        "not found", "does not exist"
    )
    if not any(word in message for word in expected):
        raise

    print("GOLD READ: DENIED")
    gold_denied = True
else:
    gold_denied = False
    raise AssertionError("Unexpected access to gold!")

report = [
    "# Polaris - Exercise 2 RBAC Results",
    "",
    "## silver_reader",
    "- Principal: silver_tester",
    "- Principal role: silver_reader_role",
    "- Catalog role: silver_reader",
    "- Scope: namespace silver",
    "",
    "## Access tests",
    f"- silver.orders read: allowed ({orders_count} orders)",
    f"- gold.access_probe read: {'denied' if gold_denied else 'allowed'}",
    "",
    "## Conclusion",
    "- Namespace-scoped Polaris permissions restrict catalog access.",
    "- Polaris authorization does not replace S3 storage permissions."
]

output = Path(__file__).parent / "lab2_ex2_results.md"
output.write_text("\n".join(report) + "\n", encoding="utf-8")

print("Polaris exercise 2 completed successfully.")
spark.stop()
