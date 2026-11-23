import pandas as pd
import trino

conn = trino.dbapi.connect(
    host="trino",
    port=8080,
    user="user-j-gonnet-ece",
    catalog="lakehouse",
    http_scheme="http"
)

def sql(query):
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)

print("Trino version:")
print(sql("SELECT version()"))

print("\nSilver tables:")
print(sql("SHOW TABLES FROM lakehouse.silver"))

print("\nOrders:")
print(sql("SELECT count(*) AS orders FROM lakehouse.silver.orders"))

print("\nUsers:")
print(sql("SELECT count(*) AS users FROM lakehouse.silver.users"))

print("\nOrphan orders:")
print(sql("""
    SELECT count(*) AS orphan_orders
    FROM lakehouse.silver.orders o
    LEFT JOIN lakehouse.silver.users u ON o.user_id = u.user_id
    WHERE u.user_id IS NULL
"""))

sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold")

# Orders by state and month
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.orders_by_state_month AS
    SELECT
        u.state,
        date_trunc('month', o.ordered_at) AS month,
        count(*) AS orders,
        sum(o.quantity) AS units,
        count(DISTINCT o.user_id) AS buyers
    FROM lakehouse.silver.orders o
    JOIN lakehouse.silver.users u ON u.user_id = o.user_id
    GROUP BY 1, 2
""")

# Best-selling products
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.top_products AS
    SELECT
        product,
        count(*) AS orders,
        sum(quantity) AS units,
        rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
    FROM lakehouse.silver.orders
    GROUP BY product
""")

# User activity
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.user_activity AS
    SELECT
        u.user_id,
        u.state,
        date_diff('year', u.birthdate, current_date) AS age,
        count(o.order_id) AS orders,
        coalesce(sum(o.quantity), 0) AS units,
        min(o.ordered_at) AS first_order_at,
        max(o.ordered_at) AS last_order_at
    FROM lakehouse.silver.users u
    LEFT JOIN lakehouse.silver.orders o ON o.user_id = u.user_id
    GROUP BY u.user_id, u.state, u.birthdate
""")

print("\nGold tables:")
print(sql("SHOW TABLES FROM lakehouse.gold"))

print("\nTop products:")
print(sql("SELECT * FROM lakehouse.gold.top_products ORDER BY units_rank LIMIT 10"))

# Validate gold tables
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
product_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.top_products").iloc[0, 0]

silver_users = sql("SELECT count(*) AS v FROM lakehouse.silver.users").iloc[0, 0]
gold_users = sql("SELECT count(*) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_orders = sql("SELECT sum(orders) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]

assert silver_units == gold_units == product_units
assert silver_users == gold_users
assert silver_orders == gold_orders

print("\nAll gold validations passed.")

# Iceberg metadata
print("\nSilver snapshots:")
print(sql("""
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.silver."orders$snapshots"
    ORDER BY committed_at
"""))

print("\nGold data files:")
print(sql("""
    SELECT file_path, record_count
    FROM lakehouse.gold."top_products$files"
"""))

# Query execution plan
query = """
    SELECT u.state, count(*) AS orders
    FROM lakehouse.silver.orders o
    JOIN lakehouse.silver.users u ON u.user_id = o.user_id
    GROUP BY u.state
"""

plan = sql("EXPLAIN (TYPE DISTRIBUTED) " + query)
stats = sql("EXPLAIN ANALYZE " + query)

print("\nQuery plan and execution analysis completed.")

# Refresh top_products
sql("DELETE FROM lakehouse.gold.top_products")

sql("""
    INSERT INTO lakehouse.gold.top_products
    SELECT
        product,
        count(*),
        sum(quantity),
        rank() OVER (ORDER BY sum(quantity) DESC)
    FROM lakehouse.silver.orders
    GROUP BY product
""")

# Check snapshots after refresh
snapshots = sql("""
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."top_products$snapshots"
    ORDER BY committed_at
""")

print("\nTop products snapshots:")
print(snapshots)

# Time travel
first = snapshots.iloc[0]["snapshot_id"]

old_count = sql(f"""
    SELECT count(*) AS products
    FROM lakehouse.gold.top_products
    FOR VERSION AS OF {first}
""").iloc[0, 0]

current_count = sql("""
    SELECT count(*) AS products
    FROM lakehouse.gold.top_products
""").iloc[0, 0]

print("\nTime travel:")
print("First snapshot:", old_count)
print("Current version:", current_count)

assert old_count == current_count
print("\nLab 2 completed successfully.")
