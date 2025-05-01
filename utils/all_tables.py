import psycopg2

# Connection details for 'usa' database
host = "82.112.254.77"
user = "supernova"
password = "TRAwir515025"
database = "usa"

try:
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        dbname=database
    )

    cur = conn.cursor()

    # List all tables in all schemas (excluding system schemas)
    cur.execute("""
        SELECT schemaname, tablename 
        FROM pg_catalog.pg_tables 
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema');
    """)

    tables = cur.fetchall()
    print(f"Tables in database '{database}':")
    for schema, table in tables:
        print(f"- {schema}.{table}")

    cur.close()
    conn.close()

except Exception as e:
    print("Connection failed:", e)
