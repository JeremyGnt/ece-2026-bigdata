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
    .appName("polaris-lab2-ex1")
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


metadata_path = (
    f"s3://{BUCKET}/warehouse/"
    "silver/polaris_users/metadata/"
)

def metadata_files():
    listing = subprocess.check_output([
        "aws", "s3", "--profile", PROFILE,
        "ls", metadata_path, "--recursive"
    ], text=True)
    return {
        line.split()[-1]
        for line in listing.splitlines()
        if line.endswith(".metadata.json")
    }

before_files = metadata_files()

snapshots_before = spark.sql("""
    SELECT snapshot_id
    FROM polaris.silver.users.snapshots
""").count()

columns_before = spark.table("polaris.silver.users").columns

metadata_path = (
    f"s3://{BUCKET}/warehouse/"
    "silver/polaris_users/metadata/"
)

def metadata_files():
    listing = subprocess.check_output([
        "aws", "s3", "--profile", PROFILE,
        "ls", metadata_path, "--recursive"
    ], text=True)
    return {
        line.split()[-1]
        for line in listing.splitlines()
        if line.endswith(".metadata.json")
    }

before_files = metadata_files()

snapshots_before = spark.sql("""
    SELECT snapshot_id
    FROM polaris.silver.users.snapshots
""").count()

columns_before = spark.table("polaris.silver.users").columns

if "country" in columns_before:
    spark.sql("""
        ALTER TABLE polaris.silver.users
        DROP COLUMN country
    """)
    operation = "DROP COLUMN country executed"
else:
    operation = "Column country already absent"

columns_after = spark.table("polaris.silver.users").columns

snapshots_after = spark.sql("""
    SELECT snapshot_id
    FROM polaris.silver.users.snapshots
""").count()

after_files = metadata_files()

assert "country" not in columns_after
assert snapshots_before == snapshots_after

if "country" in columns_before:
    assert after_files - before_files

report = [
    "# Polaris - Exercise 1",
    "",
    f"- Operation: {operation}",
    f"- Snapshots before: {snapshots_before}",
    f"- Snapshots after: {snapshots_after}",
    f"- New metadata files: {len(after_files - before_files)}",
    "- Country column is absent.",
    "- Schema changes modify Iceberg metadata.",
    "- No new data snapshot was created."
]

output = Path(__file__).parent / "lab2_ex1_results.md"
output.write_text("\n".join(report) + "\n", encoding="utf-8")

print("Polaris exercise 1 completed successfully.")
spark.stop()
