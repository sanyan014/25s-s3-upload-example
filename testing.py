import psycopg2

# ✅ RDS credentials
conn = psycopg2.connect(
    host="sleep-data-db.cur84skoqqm0.us-east-1.rds.amazonaws.com",
    dbname="sleepdb",
    user="postgres",
    password="strongpassword!"
)

cur = conn.cursor()

# Add unique constraint
cur.execute("""
    ALTER TABLE sleep_stats
    ADD CONSTRAINT unique_filename UNIQUE (filename);
""")
conn.commit()
cur.close()
conn.close()
print("✅ Unique constraint added on filename.")