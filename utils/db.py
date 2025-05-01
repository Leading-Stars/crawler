import psycopg2

# Connection details
host = "82.112.254.77"
user = "supernova"
password = "TRAwir515025"
database = "postgres"  # Connect to a default DB to access others

try:
    # Connect to the PostgreSQL server
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        dbname=database
    )

    # Open a cursor to perform database operations
    cur = conn.cursor()

    # List all databases (excluding templates)
    cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")

    databases = cur.fetchall()
    print("Databases:")
    for db in databases:
        print("-", db[0])

    # Clean up
    cur.close()
    conn.close()

except Exception as e:
    print("Connection failed:", e)
