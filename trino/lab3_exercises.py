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


# Exercise 1 - Partitioned gold table
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.orders_by_state_month_part
    WITH (partitioning = ARRAY['month(month)'])
    AS SELECT * FROM lakehouse.gold.orders_by_state_month
""")

print("\nExercise 1 - Partitioned table")
print("Rows:", sql("SELECT count(*) FROM lakehouse.gold.orders_by_state_month_part"))
print("Files:", sql("""
    SELECT file_path, record_count
    FROM lakehouse.gold."orders_by_state_month_part$files"
"""))

# Exercise 2 - Birth cohorts
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.cohorts AS
    SELECT
        CAST(floor(year(u.birthdate) / 10) * 10 AS integer) AS birth_decade,
        count(DISTINCT u.user_id) AS users,
        count(o.order_id) AS orders,
        avg(CAST(o.quantity AS double)) AS avg_units_per_order
    FROM lakehouse.silver.users u
    LEFT JOIN lakehouse.silver.orders o ON u.user_id = o.user_id
    GROUP BY 1
""")

print("\nExercise 2 - Cohorts")
for row in sql("SELECT * FROM lakehouse.gold.cohorts ORDER BY birth_decade"):
    print(row)

print("\nExercises 1 and 2 completed.")
