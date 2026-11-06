import os
import json
import subprocess
from pathlib import Path

from pyspark.sql import SparkSession

BUCKET = os.environ["LAB_BUCKET_NAME"]
PROFILE = os.environ.get("AWS_PROFILE", "default")
URI = os.environ["POLARIS_URI"]
CATALOG = os.environ["POLARIS_CATALOG"]
DEV = os.environ["POLARIS_ANALYST_CREDENTIAL"]

creds = json.loads(subprocess.check_output([
    "aws", "configure", "export-credentials",
    "--profile", PROFILE, "--format", "process"
], text=True))

endpoint = "https://s3.seaweedfs.adm.adaltas.cloud"
report = ["# Polaris - Lab 2 Results", ""]

spark = (
    SparkSession.builder
    .appName("polaris-lab2-analyst")
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


# Analyst: read access
tables = [
    r.tableName for r in
    spark.sql("SHOW TABLES IN polaris.silver").collect()
]

assert "orders" in tables
assert "users" in tables

orders_count = spark.table("polaris.silver.orders").count()

products = spark.sql("""
    SELECT product, COUNT(*) AS total
    FROM polaris.silver.orders
    GROUP BY product
    ORDER BY total DESC
""").collect()

snapshots_before = {
    r.snapshot_id for r in spark.sql("""
        SELECT snapshot_id
        FROM polaris.silver.orders.snapshots
    """).collect()
}

report += [
    "## Analyst read permissions",
    f"- Visible tables: {tables}",
    f"- Orders read: {orders_count}",
    f"- Product groups: {len(products)}",
    "- SELECT and SHOW TABLES succeeded.",
    ""
]

# Analyst: write operations must be denied.
checks = [
    ("INSERT", """
        INSERT INTO polaris.silver.orders
        SELECT * FROM polaris.silver.orders LIMIT 1
    """),
    ("ALTER TABLE", """
        ALTER TABLE polaris.silver.users
        ADD COLUMN phone STRING
    """)
]

for name, query in checks:
    try:
        spark.sql(query).collect()
    except Exception as exc:
        message = str(exc).lower()
        denied = any(word in message for word in (
            "forbidden", "403", "accessdenied",
            "not authorized", "permission denied"
        ))
        if not denied:
            raise
        report.append(f"- {name}: denied by Polaris")
    else:
        raise AssertionError(
            f"{name} was unexpectedly allowed!"
        )

snapshots_after = {
    r.snapshot_id for r in spark.sql("""
        SELECT snapshot_id
        FROM polaris.silver.orders.snapshots
    """).collect()
}

assert snapshots_before == snapshots_after
assert "phone" not in spark.table(
    "polaris.silver.users"
).columns

report += [
    "",
    "## Security validation",
    "- INSERT correctly forbidden.",
    "- ALTER TABLE correctly forbidden.",
    "- No additional order snapshot created.",
    "- The analyst has read-only catalog access.",
    ""
]

from pathlib import Path

output = Path(__file__).parent / "lab2_analyst_results.md"
output.write_text("\n".join(report) + "\n", encoding="utf-8")

print("Polaris analyst tests completed successfully.")
spark.stop()
