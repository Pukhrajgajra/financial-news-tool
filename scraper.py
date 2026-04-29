import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import time
from db_writer import save_article

RSS_FEEDS = [
    # Yahoo Finance — most reliable, no blocking
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=AAPL&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=TSLA&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=MSFT&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GOOGL&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=AMZN&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=NVDA&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=META&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=JPM&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=NFLX&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=AMD&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=INTC&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=BAC&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GS&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=DIS&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=COIN&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UBER&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=PYPL&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=BABA&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=SPOT&region=US&lang=en-US",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=SHOP&region=US&lang=en-US",
    # General financial news
    "https://feeds.finance.yahoo.com/rss/2.0/headline",
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.reuters.com/reuters/technologyNews",
    "https://feeds.reuters.com/reuters/companyNews",
    "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "https://www.cnbc.com/id/10001147/device/rss/rss.html",
    "https://www.cnbc.com/id/15839135/device/rss/rss.html",
    "https://www.cnbc.com/id/20409666/device/rss/rss.html",
    "https://rss.cnn.com/rss/money_news_international.rss",
    "https://www.ft.com/?format=rss",
    "https://seekingalpha.com/market_currents.xml",
]

TICKER_KEYWORDS = {
    "AAPL":  ["apple", "iphone", "ipad", "mac", "tim cook", "app store", "ios"],
    "TSLA":  ["tesla", "elon musk", "electric vehicle", " ev ", "gigafactory", "cybertruck"],
    "MSFT":  ["microsoft", "azure", "windows", "copilot", "satya nadella", "xbox"],
    "GOOGL": ["google", "alphabet", "youtube", "gemini", "sundar pichai", "waymo"],
    "AMZN":  ["amazon", "aws", "prime", "andy jassy", "whole foods"],
    "NVDA":  ["nvidia", "gpu", "jensen huang", "cuda", "ai chip", "blackwell", "h100"],
    "META":  ["meta", "facebook", "instagram", "whatsapp", "mark zuckerberg", "threads"],
    "JPM":   ["jpmorgan", "jp morgan", "jamie dimon", "chase bank"],
    "NFLX":  ["netflix", "streaming", "ted sarandos"],
    "AMD":   ["amd", "advanced micro devices", "ryzen", "radeon", "lisa su"],
    "INTC":  ["intel", "pat gelsinger", "core processor"],
    "BAC":   ["bank of america", "bofa"],
    "GS":    ["goldman sachs", "goldman"],
    "DIS":   ["disney", "disney+", "bob iger", "pixar", "marvel", "espn"],
    "COIN":  ["coinbase", "cryptocurrency", "bitcoin", "crypto", "btc", "ethereum"],
    "UBER":  ["uber", "rideshare", "dara khosrowshahi"],
    "PYPL":  ["paypal", "venmo"],
    "BABA":  ["alibaba", "jack ma", "aliexpress"],
    "SPOT":  ["spotify", "daniel ek", "podcast"],
    "SHOP":  ["shopify", "tobi lutke", "ecommerce"],
    "FED":   ["federal reserve", "fed rate", "jerome powell", "interest rate", "inflation", "fomc"],
    "SPY":   ["s&p 500", "s&p500", "stock market", "wall street", "dow jones", "nasdaq"],
}

seen_hashes = set()

def load_seen_hashes():
    """Load already-scraped URLs from DB to avoid re-scraping on restart"""
    try:
        import psycopg2
        conn = psycopg2.connect(
            dbname="financial_news", user="pukhrajgajra",
            host="localhost", port="5432"
        )
        cur = conn.cursor()
        cur.execute("SELECT url FROM articles;")
        urls = cur.fetchall()
        for (url,) in urls:
            seen_hashes.add(hashlib.md5(url.encode()).hexdigest())
        cur.close()
        conn.close()
        print(f"Loaded {len(seen_hashes)} existing URLs from DB")
    except Exception as e:
        print(f"Could not load existing URLs: {e}")

def detect_tickers(text):
    text_lower = text.lower()
    found = []
    for ticker, keywords in TICKER_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            found.append(ticker)
    return ",".join(found) if found else "GENERAL"

def is_duplicate(url):
    url_hash = hashlib.md5(url.encode()).hexdigest()
    if url_hash in seen_hashes:
        return True
    seen_hashes.add(url_hash)
    return False

def get_full_text(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        paragraphs = soup.find_all("p")
        text = " ".join(p.get_text(strip=True) for p in paragraphs)
        return text[:5000] if len(text) > 100 else ""
    except Exception as e:
        return ""

def scrape_rss_feed(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        feed = feedparser.parse(url, request_headers=headers)
        articles = []
        for entry in feed.entries:
            article = {
                "title":        entry.get("title", "").strip(),
                "url":          entry.get("link", ""),
                "summary":      entry.get("summary", ""),
                "published_at": entry.get("published", str(datetime.now())),
                "source":       feed.feed.get("title", url),
            }
            if article["title"] and article["url"]:
                articles.append(article)
        return articles
    except Exception as e:
        print(f"Feed error {url}: {e}")
        return []

def run_pipeline():
    print(f"\nScraping started at {datetime.now()}")
    load_seen_hashes()
    total = 0
    skipped = 0

    for feed_url in RSS_FEEDS:
        print(f"\nFetching: {feed_url}")
        articles = scrape_rss_feed(feed_url)
        print(f"  Found {len(articles)} entries")

        for article in articles:
            if is_duplicate(article["url"]):
                skipped += 1
                continue

            article["full_text"] = get_full_text(article["url"])
            text_for_analysis = article["title"] + " " + article.get("summary", "")
            article["tickers"] = detect_tickers(text_for_analysis)
            save_article(article)
            total += 1
            print(f"  [{article['tickers']:15}] {article['title'][:55]}")
            time.sleep(0.3)  # be polite to servers

    print(f"\nDone! New articles saved: {total} | Duplicates skipped: {skipped}")

if __name__ == "__main__":
    run_pipeline()