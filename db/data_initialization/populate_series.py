import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
def populate_series(conn: connection):
    cur = conn.cursor()

    market = {
    "name": "BTC/USD",
    "active": True
    # description and volume are missing
    }

    columns = ["name", "description", "active", "volume"]
    values = [market.get(col) for col in columns]  # missing keys return None -> NULL

    cur.execute("""
        INSERT INTO markets (name, description, active, volume)
        VALUES (%s, %s, %s, %s)
    """, values)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    db_name = "polypunt_test"
    port = "5432"
    conn = psycopg2.connect(database=db_name, port=port)
    populate_series(conn=conn)