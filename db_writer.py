import psycopg2
from datetime import datetime
from db_config import get_db_config  # <-- NEW: import central config


def get_connection():
    # **get_db_config() unpacks the dict as keyword args
    # same as writing: psycopg2.connect(dbname=..., user=..., host=..., ...)
    return psycopg2.connect(**get_db_config())


def save_article(article):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO articles (title, url, summary, full_text, source, published_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING;
        """, (
            article["title"],
            article["url"],
            article.get("summary", ""),
            article.get("full_text", ""),
            article.get("source", ""),
            article.get("published_at", str(datetime.now()))
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"DB error: {e}")