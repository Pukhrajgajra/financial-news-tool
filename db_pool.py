"""
db_pool.py — Shared connection pool.

CHANGE: replaced print() with log.info() / log.error()
"""

from psycopg2 import pool
from db_config import get_db_config
from logger import get_logger

log = get_logger(__name__)  # logger named "db_pool"

_pool = None


def initialize_pool(minconn=2, maxconn=10):
    global _pool
    if _pool is None:
        _pool = pool.SimpleConnectionPool(minconn, maxconn, **get_db_config())
        log.info(f"Connection pool created (min={minconn}, max={maxconn})")


def get_conn():
    if _pool is None:
        initialize_pool()
    return _pool.getconn()


def put_conn(conn):
    if _pool is not None:
        _pool.putconn(conn)


def close_pool():
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None
        log.info("Connection pool closed.")