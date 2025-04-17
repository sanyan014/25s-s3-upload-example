import psycopg2

# Fill these in with your actual values
conn = psycopg2.connect(
    host="sleep-prod-db.cur84skoqqm0.us-east-1.rds.amazonaws.com",
    database="postgres",
    user="postgres",
    password="strongpassword!"
)

cur = conn.cursor()

# Fetch the latest 10 entries from sleep_stats table
cur.execute("""
    SELECT id, avg_sleep_hours, avg_productivity, correlation, timestamp 
    FROM sleep_stats 
    ORDER BY timestamp DESC 
    LIMIT 10
""")

rows = cur.fetchall()

print("\nLatest sleep analysis records:")
print("ID | Avg Sleep | Avg Productivity | Correlation | Timestamp")

for row in rows:
    print(f"{row[0]} | {row[1]:.2f} | {row[2]:.2f} | {row[3]:.2f} | {row[4]}")

cur.close()
conn.close()
