import trino

conn = trino.dbapi.connect(
    host="trino", port=8080,
    user="user-j-gonnet-ece",
    catalog="lakehouse"
)

def sql(query):
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()

# Exercise 3 - Add one order
order_id = "trino-lab3-test-order"

if sql(f"SELECT count(*) FROM lakehouse.silver.orders WHERE order_id = '{order_id}'")[0][0] == 0:
    sql(f"""
        INSERT INTO lakehouse.silver.orders
        (order_id, user_id, ordered_at, quantity, product)
        SELECT
            '{order_id}', o.user_id, o.ordered_at, 1, o.product
        FROM lakehouse.silver.orders o
        JOIN lakehouse.silver.users u ON u.user_id = o.user_id
        WHERE o.product IS NOT NULL
        LIMIT 1
    """)

# Save previous gold snapshot
before = sql("""
    SELECT snapshot_id
    FROM lakehouse.gold."orders_by_state_month$snapshots"
    ORDER BY committed_at DESC LIMIT 1
""")[0][0]

# Refresh gold with MERGE
sql("""
    MERGE INTO lakehouse.gold.orders_by_state_month t
    USING (
        SELECT
            u.state,
            date_trunc('month', o.ordered_at) AS month,
            count(*) AS orders,
            sum(o.quantity) AS units,
            count(DISTINCT o.user_id) AS buyers
        FROM lakehouse.silver.orders o
        JOIN lakehouse.silver.users u ON u.user_id = o.user_id
        GROUP BY 1, 2
    ) s
    ON t.state IS NOT DISTINCT FROM s.state AND t.month = s.month
    WHEN MATCHED THEN UPDATE SET
        orders = s.orders,
        units = s.units,
        buyers = s.buyers
    WHEN NOT MATCHED THEN INSERT
        (state, month, orders, units, buyers)
        VALUES (s.state, s.month, s.orders, s.units, s.buyers)
""")

old_count = sql(f"""
    SELECT sum(orders)
    FROM lakehouse.gold.orders_by_state_month
    FOR VERSION AS OF {before}
""")[0][0]

new_count = sql("""
    SELECT sum(orders) FROM lakehouse.gold.orders_by_state_month
""")[0][0]

silver_count = sql("""
    SELECT count(*) FROM lakehouse.silver.orders
""")[0][0]

assert new_count == silver_count

print("Exercise 3 - MERGE and time travel")
print("Previous orders:", old_count)
print("Current orders:", new_count)
print("Silver orders:", silver_count)

# Exercise 4 - Optimize top_products
files_before = sql("""
    SELECT count(*) FROM lakehouse.gold."top_products$files"
""")[0][0]

snapshots_before = sql("""
    SELECT count(*) FROM lakehouse.gold."top_products$snapshots"
""")[0][0]

sql("ALTER TABLE lakehouse.gold.top_products EXECUTE optimize")

files_after = sql("""
    SELECT count(*) FROM lakehouse.gold."top_products$files"
""")[0][0]

snapshots_after = sql("""
    SELECT count(*) FROM lakehouse.gold."top_products$snapshots"
""")[0][0]

print("\nExercise 4 - OPTIMIZE")
print("Files:", files_before, "->", files_after)
print("Snapshots:", snapshots_before, "->", snapshots_after)
print("\nExercises 3 and 4 completed.")

conn.close()
