import os
from contextlib import contextmanager

from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor

GOLD_SCHEMA = os.getenv("GOLD_SCHEMA", "gold")

_pool = None


def init_pool():
    global _pool
    _pool = SimpleConnectionPool(
        1, 10,
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "retailrocket"),
        user=os.getenv("POSTGRES_USER", "retail"),
        password=os.getenv("POSTGRES_PASSWORD", "retail"),
    )


def close_pool():
    if _pool:
        _pool.closeall()


@contextmanager
def get_cursor():
    conn = _pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        conn.commit()
    finally:
        _pool.putconn(conn)
