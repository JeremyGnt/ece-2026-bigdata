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
    .appName("polaris-lab2-dev")
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

lab_ns = [r[0] for r in spark.sql(
    "SHOW NAMESPACES IN lab"
).collect()]

polaris_ns = [r[0] for r in spark.sql(
    "SHOW NAMESPACES IN polaris"
).collect()]

report += [
    "## Catalog connections",
    f"- Hadoop catalog namespaces: {lab_ns}",
    f"- Polaris catalog namespaces: {polaris_ns}",
    "- lab uses the Hadoop catalog on S3.",
    "- polaris uses the Polaris REST API with dev credentials.",
    ""
]

metadata_dir = (
    f"s3://{BUCKET}/warehouse/silver/orders/metadata/"
)

version = subprocess.check_output([
    "aws", "s3", "--profile", PROFILE, "cp",
    metadata_dir + "version-hint.text", "-"
], text=True).strip()

metadata_file = (
    f"s3a://{BUCKET}/warehouse/silver/orders/"
    f"metadata/v{int(version)}.metadata.json"
)

def order_files():
    output = subprocess.check_output([
        "aws", "s3", "--profile", PROFILE, "ls",
        f"s3://{BUCKET}/warehouse/silver/orders/",
        "--recursive"
    ], text=True)
    return len([line for line in output.splitlines() if line.strip()])

files_before = order_files()

spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")

tables = [
    r.tableName for r in spark.sql(
        "SHOW TABLES IN polaris.silver"
    ).collect()
]

if "orders" not in tables:
    spark.sql(f"""
        CALL polaris.system.register_table(
            table => 'silver.orders',
            metadata_file => '{metadata_file}'
        )
    """).show()
    registration = "New registration"
else:
    registration = "Already registered"

files_after = order_files()

hadoop_count = spark.table("lab.silver.orders").count()
polaris_count = spark.table("polaris.silver.orders").count()

assert hadoop_count == polaris_count
assert files_before == files_after

report += [
    "## Register an existing Iceberg table",
    f"- Metadata version: v{version}",
    f"- Registration: {registration}",
    f"- Hadoop orders: {hadoop_count}",
    f"- Polaris orders: {polaris_count}",
    f"- Storage objects before: {files_before}",
    f"- Storage objects after: {files_after}",
    "- Registration did not copy data files.",
    ""
]

tables = [
    r.tableName for r in spark.sql(
        "SHOW TABLES IN polaris.silver"
    ).collect()
]

if "users" not in tables:
    spark.sql(f"""
        CREATE TABLE polaris.silver.users
        USING iceberg
        LOCATION 's3://{BUCKET}/warehouse/silver/polaris_users'
        AS SELECT * FROM lab.silver.users
    """)

users_count = spark.table("polaris.silver.users").count()
source_count = spark.table("lab.silver.users").count()

assert users_count == source_count

report += [
    "## Create table through Polaris",
    f"- Source users: {source_count}",
    f"- Polaris users: {users_count}",
    f"- Location: s3://{BUCKET}/warehouse/silver/polaris_users",
    ""
]

# Schema change as dev.
snapshots_before = spark.sql(
    "SELECT * FROM polaris.silver.users.snapshots"
).count()

if "country" not in spark.table("polaris.silver.users").columns:
    spark.sql(
        "ALTER TABLE polaris.silver.users ADD COLUMN country STRING"
    )

snapshots_after = spark.sql(
    "SELECT * FROM polaris.silver.users.snapshots"
).count()

assert "country" in spark.table("polaris.silver.users").columns

report += [
    "## Schema change as dev",
    "- Added country STRING to polaris.silver.users.",
    f"- Snapshots before: {snapshots_before}",
    f"- Snapshots after: {snapshots_after}",
    "- Schema changes update table metadata.",
    ""
]

output = Path(__file__).parent / "lab2_results.md"
output.write_text("\n".join(report) + "\n", encoding="utf-8")

print("Polaris dev phase completed successfully.")
spark.stop()
