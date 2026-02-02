import pandas as pd
import psycopg2
import time

time.sleep(5)  # wait for Postgres

conn = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname="postgres",
    user="postgres",
    password="postgres"
)

df = pd.read_csv("/app/data/users.csv")

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INT,
        name TEXT,
        age INT
    )
""")

for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO users VALUES (%s, %s, %s)",
        (row.id, row.name, row.age)
    )

conn.commit()
cur.close()
conn.close()

print("Data ingestion completed successfully")
