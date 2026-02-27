from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()

dsn = os.getenv("POSTGRES_DSN")
print("Using DSN:", dsn)

try:
    conn = psycopg2.connect(dsn)
    print("CONNECTED SUCCESSFULLY!")
    conn.close()
except Exception as e:
    print("CONNECTION FAILED:")
    print(e)
