from datetime import datetime
from db_pool import get_conn, put_conn
from logger import get_logger

log = get_logger(__name__)  # logger named "db_writer"


def save_article(article):
    conn = get_conn()
    try:
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
        log.debug(f"Saved: {article['title'][:60]}")  # debug = only in log file
    except Exception as e:
        conn.rollback()
        log.error(f"DB error saving article: {e}")
    finally:
        put_conn(conn)