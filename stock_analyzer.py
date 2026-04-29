import yfinance as yf
import psycopg2
import pandas as pd
from scipy import stats
from datetime import datetime, timedelta, date
import time

TICKERS = ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL", "AMZN", "META", "JPM", "NFLX", "AMD"]

def get_connection():
    return psycopg2.connect(dbname="financial_news", user="pukhrajgajra", host="localhost", port="5432")

def fetch_and_store_prices(ticker, days_back=60):
    try:
        end = datetime.today()
        start = end - timedelta(days=days_back)
        stock = yf.Ticker(ticker)
        df = stock.history(start=start, end=end)
        if df.empty:
            print(f"  No data for {ticker}")
            return 0
        conn = get_connection()
        cur = conn.cursor()
        saved = 0
        prev_close = None
        for idx, row in df.iterrows():
            price_date = idx.date()
            close = round(float(row["Close"]), 4)
            pct_change = round(((close - prev_close) / prev_close * 100), 4) if prev_close else None
            prev_close = close
            cur.execute("""
                INSERT INTO stock_prices (ticker, price_date, open_price, close_price, high_price, low_price, volume, pct_change)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, price_date) DO UPDATE SET close_price = EXCLUDED.close_price, pct_change = EXCLUDED.pct_change;
            """, (ticker, price_date, round(float(row["Open"]), 4), close, round(float(row["High"]), 4), round(float(row["Low"]), 4), int(row["Volume"]), pct_change))
            saved += 1
        conn.commit()
        cur.close()
        conn.close()
        print(f"  {ticker}: {saved} days of prices stored")
        return saved
    except Exception as e:
        print(f"  Error fetching {ticker}: {e}")
        return 0

def parse_date(published_at):
    clean = str(published_at).strip()
    formats = [
        "%a, %d %b %Y %H:%M:%S GMT",
        "%a, %d %b %Y %H:%M:%S +0000",
        "%a, %d %b %Y %H:%M:%S -0400",
        "%a, %d %b %Y %H:%M:%S -0500",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(clean[:35].strip(), fmt).date()
        except Exception:
            continue
    try:
        return datetime.strptime(clean[:25].strip(), "%a, %d %b %Y %H:%M:%S").date()
    except Exception:
        return date.today()

def build_correlation_table():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM sentiment_correlations;")
    cur.execute("""
        SELECT a.id, a.tickers, a.published_at, s.score, s.label
        FROM articles a
        JOIN sentiment_scores s ON a.id = s.article_id
        WHERE a.tickers IS NOT NULL AND a.tickers != 'GENERAL' AND a.published_at IS NOT NULL;
    """)
    articles = cur.fetchall()
    print(f"\nProcessing {len(articles)} articles with tickers...")
    matched = 0
    for article_id, tickers_str, published_at, score, label in articles:
        try:
            pub_date = parse_date(published_at)
            for ticker in tickers_str.split(","):
                ticker = ticker.strip()
                if not ticker or ticker == "GENERAL":
                    continue
                cur.execute("SELECT close_price, price_date FROM stock_prices WHERE ticker = %s AND price_date <= %s ORDER BY price_date DESC LIMIT 1;", (ticker, pub_date))
                day_row = cur.fetchone()
                cur.execute("SELECT close_price, price_date FROM stock_prices WHERE ticker = %s AND price_date > %s ORDER BY price_date ASC LIMIT 1;", (ticker, pub_date))
                next_row = cur.fetchone()
                if not next_row:
                    cur.execute("SELECT close_price, price_date FROM stock_prices WHERE ticker = %s ORDER BY price_date DESC LIMIT 2;", (ticker,))
                    rows = cur.fetchall()
                    if len(rows) == 2:
                        next_row = rows[0]
                        day_row = rows[1]
                if day_row and next_row:
                    close = day_row[0]
                    next_close = next_row[0]
                    actual_date = day_row[1]
                    pct_change = round(((next_close - close) / close) * 100, 4)
                    cur.execute("""
                        INSERT INTO sentiment_correlations (ticker, article_id, sentiment_score, sentiment_label, price_date, close_price, next_day_close, next_day_pct_change)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """, (ticker, article_id, score, label, actual_date, close, next_close, pct_change))
                    matched += 1
        except Exception:
            continue
    conn.commit()
    cur.close()
    conn.close()
    print(f"Matched {matched} article-price pairs")

def calculate_correlations():
    conn = get_connection()
    cur = conn.cursor()
    print("\n" + "="*60)
    print("SENTIMENT vs NEXT-DAY PRICE CHANGE CORRELATION")
    print("="*60)
    print(f"{'Ticker':<8} {'Corr':>8} {'P-value':>10} {'N':>5}  Interpretation")
    print("-"*60)
    cur.execute("SELECT DISTINCT ticker FROM sentiment_correlations;")
    tickers = [row[0] for row in cur.fetchall()]
    results = []
    for ticker in sorted(tickers):
        cur.execute("SELECT sentiment_score, next_day_pct_change FROM sentiment_correlations WHERE ticker = %s AND next_day_pct_change IS NOT NULL;", (ticker,))
        rows = cur.fetchall()
        if len(rows) < 3:
            print(f"{ticker:<8} {'--':>8} {'--':>10} {len(rows):>5}  not enough data")
            continue
        scores = [r[0] for r in rows]
        changes = [r[1] for r in rows]
        corr, pvalue = stats.pearsonr(scores, changes)
        sig = "SIGNIFICANT" if pvalue < 0.05 else "marginal" if pvalue < 0.1 else "not significant"
        direction = "positive" if corr > 0 else "negative"
        strength = "strong" if abs(corr) > 0.5 else "moderate" if abs(corr) > 0.3 else "weak"
        print(f"{ticker:<8} {corr:>8.4f} {pvalue:>10.4f} {len(rows):>5}  {strength} {direction} ({sig})")
        results.append((ticker, corr, pvalue, len(rows)))
    print("="*60)
    print("Corr > 0: positive news tends to precede price increases")
    print("Corr < 0: positive news tends to precede price decreases")
    print("P-value < 0.05: statistically significant")
    cur.close()
    conn.close()
    return results

def export_for_tableau():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT sc.ticker, sc.sentiment_score, sc.sentiment_label, sc.price_date,
               sc.close_price, sc.next_day_close, sc.next_day_pct_change, a.title, a.source
        FROM sentiment_correlations sc
        JOIN articles a ON sc.article_id = a.id
        ORDER BY sc.ticker, sc.price_date;
    """)
    rows = cur.fetchall()
    cols = ['ticker','sentiment_score','sentiment_label','price_date','close_price','next_day_close','next_day_pct_change','title','source']
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv("correlation_dashboard.csv", index=False)
    cur.close()
    conn.close()
    print(f"\nExported {len(df)} rows to correlation_dashboard.csv")

def run_full_analysis():
    print("Step 1: Fetching stock prices from Yahoo Finance...")
    for ticker in TICKERS:
        fetch_and_store_prices(ticker, days_back=60)
        time.sleep(0.5)
    print("\nStep 2: Building sentiment-price correlation table...")
    build_correlation_table()
    print("\nStep 3: Calculating correlations...")
    calculate_correlations()
    print("\nStep 4: Exporting for Tableau...")
    export_for_tableau()
    print("\nDone! Open correlation_dashboard.csv in Tableau.")

if __name__ == "__main__":
    run_full_analysis()