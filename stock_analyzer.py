from logger import get_logger, setup_logging
log = get_logger(__name__)

import yfinance as yf
from db_pool import get_conn, put_conn
import pandas as pd
from scipy import stats
from datetime import datetime, timedelta, date
import time

TICKERS = ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL", "AMZN", "META", "JPM", "NFLX", "AMD"]


def fetch_and_store_prices(ticker, days_back=60):
    try:
        end = datetime.today()
        start = end - timedelta(days=days_back)
        stock = yf.Ticker(ticker)
        df = stock.history(start=start, end=end)
        if df.empty:
            log.warning(f"No data for {ticker}")
            return 0
        conn = get_conn()
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
        put_conn(conn)
        log.info(f"{ticker}: {saved} days of prices stored")
        return saved
    except Exception as e:
        log.error(f"Error fetching {ticker}: {e}")
        return 0


def parse_date(published_at):
    """
    Parse an article timestamp to a calendar date.

    Returns None when the value can't be parsed. The caller skips those
    articles rather than guessing — stamping an unparseable date as "today"
    (the old behaviour) silently pollutes the analysis with wrong dates.
    """
    if published_at is None:
        return None
    # If the DB column is already a real timestamp/date, use it directly.
    if isinstance(published_at, datetime):
        return published_at.date()
    if isinstance(published_at, date):
        return published_at

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
        return None  # was date.today() — now we skip instead of faking


def build_correlation_table():
    """
    Match each article to a GENUINE before/after pair of trading days and
    store the next-day price move.

    An article is only matched when both prices really exist:
      - the last close on or before the article date (the "before" price), and
      - the first close strictly after it (the "next day" reaction).

    Articles that can't be matched to real prices are skipped and counted,
    never fabricated. This replaces the old fallback that assigned the most
    recent two trading days to any unmatched article (which made dozens of
    articles share one identical price move).
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM sentiment_correlations;")
    cur.execute("""
        SELECT a.id, a.tickers, a.published_at, s.score, s.label
        FROM articles a
        JOIN sentiment_scores s ON a.id = s.article_id
        WHERE a.tickers IS NOT NULL AND a.tickers != 'GENERAL' AND a.published_at IS NOT NULL;
    """)
    articles = cur.fetchall()
    log.info(f"Processing {len(articles)} articles with tickers...")

    matched = 0
    skipped_no_date = 0       # couldn't parse the article date
    skipped_before_history = 0  # article older than our price history
    skipped_too_recent = 0    # article newer than our latest price (no next day yet)

    for article_id, tickers_str, published_at, score, label in articles:
        pub_date = parse_date(published_at)
        if pub_date is None:
            skipped_no_date += 1
            continue

        for ticker in tickers_str.split(","):
            ticker = ticker.strip()
            if not ticker or ticker == "GENERAL":
                continue
            try:
                # "Before" price: last close on or before the article date.
                cur.execute(
                    "SELECT close_price, price_date FROM stock_prices "
                    "WHERE ticker = %s AND price_date <= %s "
                    "ORDER BY price_date DESC LIMIT 1;",
                    (ticker, pub_date),
                )
                day_row = cur.fetchone()

                # "Next day" price: first close strictly AFTER the article date.
                cur.execute(
                    "SELECT close_price, price_date FROM stock_prices "
                    "WHERE ticker = %s AND price_date > %s "
                    "ORDER BY price_date ASC LIMIT 1;",
                    (ticker, pub_date),
                )
                next_row = cur.fetchone()

                if day_row is None:
                    skipped_before_history += 1
                    continue
                if next_row is None:
                    # We genuinely don't know the next-day move yet. Re-running
                    # after more trading days have prices will match this later.
                    skipped_too_recent += 1
                    continue

                close, actual_date = day_row[0], day_row[1]
                next_close = next_row[0]
                pct_change = round(((next_close - close) / close) * 100, 4)

                cur.execute(
                    """
                    INSERT INTO sentiment_correlations
                        (ticker, article_id, sentiment_score, sentiment_label,
                         price_date, close_price, next_day_close, next_day_pct_change)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (ticker, article_id, score, label, actual_date, close, next_close, pct_change),
                )
                matched += 1
            except Exception as e:
                log.error(f"Match failed for article {article_id} / {ticker}: {e}")
                continue

    conn.commit()
    cur.close()
    put_conn(conn)
    log.info(
        f"Matched {matched} real article-price pairs | "
        f"skipped {skipped_no_date} (unparseable date), "
        f"{skipped_before_history} (older than price history), "
        f"{skipped_too_recent} (too recent for a next-day price)"
    )


def calculate_correlations():
    """
    Correlate news sentiment with the next-day price move.

    The unit of analysis is one (ticker, day) — NOT one article. Several
    articles about the same ticker on the same day all share that day's single
    next-day return, so treating each article as an independent point inflates
    N and the apparent significance. We average sentiment per ticker-day first,
    then correlate those independent points.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT ticker, price_date,
               AVG(sentiment_score)    AS avg_sentiment,
               AVG(next_day_pct_change) AS next_day_pct_change
        FROM sentiment_correlations
        WHERE next_day_pct_change IS NOT NULL
        GROUP BY ticker, price_date;
    """)
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["ticker", "price_date", "avg_sentiment", "next_day_pct_change"])
    df["avg_sentiment"] = pd.to_numeric(df["avg_sentiment"], errors="coerce")
    df["next_day_pct_change"] = pd.to_numeric(df["next_day_pct_change"], errors="coerce")
    df = df.dropna(subset=["avg_sentiment", "next_day_pct_change"])

    log.info("=" * 66)
    log.info("SENTIMENT vs NEXT-DAY PRICE CHANGE   (one point per ticker-day)")
    log.info("=" * 66)
    log.info(f"{'Ticker':<8} {'Corr':>8} {'P-value':>10} {'N days':>7}  Interpretation")
    log.info("-" * 66)

    results = []
    for ticker in sorted(df["ticker"].unique()):
        sub = df[df["ticker"] == ticker]
        n = len(sub)
        if n < 5 or sub["avg_sentiment"].nunique() < 2:
            log.info(f"{ticker:<8} {'--':>8} {'--':>10} {n:>7}  not enough independent days")
            continue
        corr, pvalue = stats.pearsonr(sub["avg_sentiment"], sub["next_day_pct_change"])
        direction = "positive" if corr > 0 else "negative"
        strength = "strong" if abs(corr) > 0.5 else "moderate" if abs(corr) > 0.3 else "weak"
        sig = "significant" if pvalue < 0.05 else "not significant"
        log.info(f"{ticker:<8} {corr:>8.4f} {pvalue:>10.4f} {n:>7}  {strength} {direction} ({sig})")
        results.append((ticker, corr, pvalue, n))

    # Pooled across all ticker-days — the honest headline number.
    if len(df) >= 5 and df["avg_sentiment"].nunique() >= 2:
        pooled_r, pooled_p = stats.pearsonr(df["avg_sentiment"], df["next_day_pct_change"])
        log.info("-" * 66)
        log.info(f"{'POOLED':<8} {pooled_r:>8.4f} {pooled_p:>10.4f} {len(df):>7}  all ticker-days together")

    log.info("=" * 66)
    log.info("Caveats: small per-ticker samples; testing ~10 tickers means roughly one")
    log.info("will look 'significant' by chance (multiple comparisons). For a single")
    log.info("ticker, divide your 0.05 threshold by the number of tickers tested before")
    log.info("believing it. This is exploratory analysis, not a trading signal.")
    cur.close()
    put_conn(conn)
    return results


def export_for_tableau():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT sc.ticker, sc.sentiment_score, sc.sentiment_label, sc.price_date,
               sc.close_price, sc.next_day_close, sc.next_day_pct_change, a.title, a.source
        FROM sentiment_correlations sc
        JOIN articles a ON sc.article_id = a.id
        ORDER BY sc.ticker, sc.price_date;
    """)
    rows = cur.fetchall()
    cols = ['ticker', 'sentiment_score', 'sentiment_label', 'price_date', 'close_price', 'next_day_close', 'next_day_pct_change', 'title', 'source']
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv("correlation_dashboard.csv", index=False)
    cur.close()
    put_conn(conn)
    log.info(f"Exported {len(df)} rows to correlation_dashboard.csv")


def run_full_analysis():
    log.info("Step 1: Fetching stock prices from Yahoo Finance...")
    for ticker in TICKERS:
        fetch_and_store_prices(ticker, days_back=60)
        time.sleep(0.5)
    log.info("Step 2: Building sentiment-price correlation table...")
    build_correlation_table()
    log.info("Step 3: Calculating correlations...")
    calculate_correlations()
    log.info("Step 4: Exporting dashboard data...")
    export_for_tableau()
    log.info("Done! Refresh the Streamlit dashboard to see the updated numbers.")


if __name__ == "__main__":
    setup_logging()
    run_full_analysis()