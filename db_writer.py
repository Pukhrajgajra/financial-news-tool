
from datetime import datetime
from db_pool import get_conn, put_conn  # NEW: use pool instead of direct connect


def save_article(article):
    conn = get_conn()       # borrow from pool
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
    except Exception as e:
        conn.rollback()     # NEW: undo any partial changes if something fails
        print(f"DB error: {e}")
    finally:
        put_conn(conn)      # ALWAYS return to pool, whether success or error