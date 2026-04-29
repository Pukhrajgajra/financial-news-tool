#Scrape RSS feeds
import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib

RSS_FEEDS = [
    "https://feeds.finance.yahoo.com/rss/2.0/headline",
    "https://feeds.reuters.com/reuters/businessNews",
    "https://www.marketwatch.com/rss/topstories",
]

seen_hashes = set()

def is_duplicate(url):
    url_hash = hashlib.md5(url.encode()).hexdigest()
    if url_hash in seen_hashes:
        return True
    seen_hashes.add(url_hash)
    return False

#Scrape full article text
def get_full_text(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        for tag in soup(["script", "style", "nav", "footer"]):
            tag.decompose()
        paragraphs = soup.find_all("p")
        text = " ".join(p.get_text(strip=True) for p in paragraphs)
        return text[:5000]
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return ""

def scrape_rss_feed(url):
    feed = feedparser.parse(url)
    articles = []
    for entry in feed.entries:
        article = {
            "title":        entry.get("title", ""),
            "url":          entry.get("link", ""),
            "summary":      entry.get("summary", ""),
            "published_at": entry.get("published", str(datetime.now())),
            "source":       feed.feed.get("title", url),
        }
        articles.append(article)
    return articles


from db_writer import save_article

def run_pipeline():
    print(f"\nScraping started at {datetime.now()}")
    total = 0
    for feed_url in RSS_FEEDS:
        print(f"Fetching: {feed_url}")
        articles = scrape_rss_feed(feed_url)
        for article in articles:
            if not is_duplicate(article["url"]):
                article["full_text"] = get_full_text(article["url"])
                save_article(article)
                total += 1
                print(f"  Saved: {article['title'][:60]}")
    print(f"\nDone! Total articles saved to DB: {total}")

if __name__ == "__main__":
    run_pipeline()