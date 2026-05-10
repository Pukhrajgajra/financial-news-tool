"""
nlp_processor.py — Sentiment analysis and entity extraction.

CHANGE: replaced all print() with appropriate log levels
"""

import spacy
from textblob import TextBlob
from db_pool import get_conn, put_conn
from logger import get_logger

log = get_logger(__name__)  # logger named "nlp_processor"

nlp = spacy.load("en_core_web_sm")


def analyze_sentiment(text):
    blob = TextBlob(text)
    score = blob.sentiment.polarity
    if score > 0.1:
        label = "positive"
    elif score < -0.1:
        label = "negative"
    else:
        label = "neutral"
    return score, label


def extract_entities(text):
    doc = nlp(text[:10000])
    entities = []
    for ent in doc.ents:
        if ent.label_ in ["ORG", "PERSON", "GPE", "MONEY"]:
            entities.append((ent.text, ent.label_))
    return list(set(entities))


def process_all_articles():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT a.id, a.title, a.summary, a.full_text
        FROM articles a
        LEFT JOIN sentiment_scores s ON a.id = s.article_id
        WHERE s.id IS NULL;
    """)
    articles = cur.fetchall()
    log.info(f"Found {len(articles)} unprocessed articles")

    for article_id, title, summary, full_text in articles:
        text = full_text if full_text and len(full_text) > 100 else (title + " " + (summary or ""))

        score, label = analyze_sentiment(text)
        cur.execute("""
            INSERT INTO sentiment_scores (article_id, score, label)
            VALUES (%s, %s, %s);
        """, (article_id, score, label))

        entities = extract_entities(text)
        for entity, entity_type in entities:
            cur.execute("""
                INSERT INTO named_entities (article_id, entity, entity_type)
                VALUES (%s, %s, %s);
            """, (article_id, entity, entity_type))

        # INFO = shows in console. Use debug for per-article noise if you prefer.
        log.info(f"[{label:8}] {title[:60]}")

    conn.commit()
    cur.close()
    put_conn(conn)
    log.info("NLP processing complete!")


if __name__ == "__main__":
    from logger import setup_logging
    setup_logging()
    process_all_articles()