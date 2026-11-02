import os

import psycopg2


def get_conn():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "postgres"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "frauddb"),
        user=os.getenv("PGUSER", "fraud"),
        password=os.getenv("PGPASSWORD", "fraud"),
    )


def execute(sql, params=None, fetch=False):
    """Run one statement. Returns rows if fetch=True, else the affected row count."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchall() if fetch else cur.rowcount
        conn.commit()
        return result
    finally:
        conn.close()