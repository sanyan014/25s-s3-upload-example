import psycopg2

conn = psycopg2.connect(
    host="sleep-prod-db.cur84skoqqm0.us-east-1.rds.amazonaws.com",
    user="postgres",
    password="strongpassword!",
    database="sleepdb"
)

cur = conn.cursor()

# List all tables
cur.execute("""SELECT table_name FROM information_schema.tables 
               WHERE table_schema = 'public'""")
tables = cur.fetchall()

print("Tables in sleepdb:")
for table in tables:
    print(table[0])

cur.close()
conn.close()