import psycopg2
from datetime import datetime

def get_connection():
    return psycopg2.connect(
        dbname="financial_news",
        user="pukhrajgajra",
        host="localhost",
        port="5432"
    )

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