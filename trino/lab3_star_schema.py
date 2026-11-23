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
    return cur.fetchall()

# Dimensions
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_user AS
    SELECT
        row_number() OVER (ORDER BY user_id) AS user_key,
        user_id, username, name, sex, birthdate, city, state, zip_code
    FROM lakehouse.silver.users
""")

sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_product AS
    SELECT row_number() OVER (ORDER BY product) AS product_key, product
    FROM (SELECT DISTINCT product FROM lakehouse.silver.orders)
""")

first_day, last_day = sql("""
    SELECT CAST(min(ordered_at) AS date),
           CAST(max(ordered_at) AS date)
    FROM lakehouse.silver.orders
""")[0]

sql(f"""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_date AS
    SELECT
        CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
        d AS full_date,
        year(d) AS year,
        quarter(d) AS quarter,
        month(d) AS month,
        date_format(CAST(d AS timestamp), '%M') AS month_name,
        day_of_week(d) AS day_of_week,
        day_of_week(d) IN (6, 7) AS is_weekend
    FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}',
                         INTERVAL '1' DAY)) AS t(d)
""")

# Fact table
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_orders AS
    SELECT
        o.order_id,
        u.user_key,
        p.product_key,
        CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key,
        o.ordered_at,
        o.quantity
    FROM lakehouse.silver.orders o
    LEFT JOIN lakehouse.gold.dim_user u ON u.user_id = o.user_id
    LEFT JOIN lakehouse.gold.dim_product p ON p.product = o.product
""")

print("Star schema tables:")
for table in ["dim_user", "dim_product", "dim_date", "fact_orders"]:
    count = sql(f"SELECT count(*) FROM lakehouse.gold.{table}")[0][0]
    print(f"{table}: {count}")

# Validation
silver_orders, silver_units = sql("""
    SELECT count(*), sum(quantity) FROM lakehouse.silver.orders
""")[0]

fact_orders, fact_units = sql("""
    SELECT count(*), sum(quantity) FROM lakehouse.gold.fact_orders
""")[0]

assert silver_orders == fact_orders
assert silver_units == fact_units

unmatched = sql("""
    SELECT count(*)
    FROM lakehouse.gold.fact_orders f
    LEFT JOIN lakehouse.gold.dim_user u ON u.user_key = f.user_key
    LEFT JOIN lakehouse.gold.dim_product p ON p.product_key = f.product_key
    LEFT JOIN lakehouse.gold.dim_date d ON d.date_key = f.date_key
    WHERE u.user_key IS NULL
       OR p.product_key IS NULL
       OR d.date_key IS NULL
""")[0][0]

assert unmatched == 0
print("All star schema validations passed.")

# Query the star
results = sql("""
    SELECT d.year, u.state, p.product,
           count(*) AS orders,
           sum(f.quantity) AS units
    FROM lakehouse.gold.fact_orders f
    JOIN lakehouse.gold.dim_date d ON d.date_key = f.date_key
    JOIN lakehouse.gold.dim_user u ON u.user_key = f.user_key
    JOIN lakehouse.gold.dim_product p ON p.product_key = f.product_key
    GROUP BY d.year, u.state, p.product
    ORDER BY units DESC
    LIMIT 10
""")

print("Top 10 star schema results:")
for row in results:
    print(row)

conn.close()
