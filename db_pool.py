from psycopg2 import pool
from db_config import get_db_config

# _pool is "private" (underscore prefix = convention for internal use)
# It's None until initialize_pool() is called
_pool = None


def initialize_pool(minconn=2, maxconn=10):
    """
    Call this ONCE when your app starts.
    Opens minconn connections immediately, can scale up to maxconn.
    """
    global _pool
    if _pool is None:
        _pool = pool.SimpleConnectionPool(minconn, maxconn, **get_db_config())
        print(f"Connection pool created (min={minconn}, max={maxconn})")


def get_conn():
    """Borrow a connection from the pool."""
    if _pool is None:
        initialize_pool()
    return _pool.getconn()


def put_conn(conn):
    """Return a connection to the pool so others can use it."""
    if _pool is not None:
        _pool.putconn(conn)


def close_pool():
    """
    Shut down the pool cleanly.
    Call this when your script finishes — closes all open connections.
    """
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
        print("Connection pool closed.")