import psycopg2

# Fill these in with your actual values
conn = psycopg2.connect(
    host="sleep-prod-db.cur84skoqqm0.us-east-1.rds.amazonaws.com",
    database="postgres",
    user="postgres",
    password="strongpassword!"
)

cur = conn.cursor()

# Adjust table name if needed
cur.execute("SELECT * FROM sleep_stats ORDER BY timestamp DESC LIMIT 10;")
rows = cur.fetchall()

for row in rows:
    print(row)

cur.close()
conn.close()
