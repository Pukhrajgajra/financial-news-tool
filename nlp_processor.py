import psycopg2
from textblob import TextBlob
import spacy

nlp = spacy.load("en_core_web_sm")

def get_connection():
    return psycopg2.connect(
        dbname="financial_news",
        user="pukhrajgajra",
        host="localhost",
        port="5432"
    )

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
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT a.id, a.title, a.summary, a.full_text
        FROM articles a
        LEFT JOIN sentiment_scores s ON a.id = s.article_id
        WHERE s.id IS NULL;
    """)
    articles = cur.fetchall()
    print(f"Found {len(articles)} unprocessed articles")

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

        print(f"  [{label:8}] {title[:50]}")

    conn.commit()
    cur.close()
    conn.close()
    print("\nNLP processing complete!")

if __name__ == "__main__":
    process_all_articles()