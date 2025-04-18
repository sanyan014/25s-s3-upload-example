import psycopg2

# === RDS Connection Configuration ===
DB_HOST = "sleep-data-db.cur84skoqqm0.us-east-1.rds.amazonaws.com"
DB_NAME = "sleepdb"
DB_USER = "postgres"
DB_PASSWORD = "strongpassword!"

# === Connect to RDS ===
conn = psycopg2.connect(
    host="sleep-data-db.cur84skoqqm0.us-east-1.rds.amazonaws.com",
    dbname="sleepdb",
    user="postgres",
    password="strongpassword!"
)

cur = conn.cursor()
print(f"✅ Connected to sleepdb on {conn.dsn}")

# List tables in the current schema
cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
""")
tables = cur.fetchall()

if not tables:
    print("⚠️ No tables found in the public schema.")
else:
    print("📋 Tables found:")
    for t in tables:
        print(f" - {t[0]}")

# Try to fetch data from sleep_stats if it exists
if ('sleep_stats',) in tables:
    cur.execute("SELECT * FROM sleep_stats ORDER BY timestamp DESC LIMIT 10;")
    rows = cur.fetchall()
    print("\n📊 Latest rows from sleep_stats:")
    for row in rows:
        print(row)

cur.close()
conn.close()