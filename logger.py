import logging
import sys


def setup_logging(level=logging.INFO):
    """
    Call this once at app startup (in scraper.py __main__ block).
    Sets up two handlers:
      1. Console handler — prints to terminal
      2. File handler    — writes to financial_news.log
    """
    fmt = "%(asctime)s  %(name)-20s  %(levelname)-8s  %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt=date_fmt)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # capture everything; handlers filter below

    # Console: INFO and above
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    root.addHandler(console)

    # File: DEBUG and above (everything saved to disk)
    file_handler = logging.FileHandler("financial_news.log", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    # Silence noisy third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("feedparser").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)

    root.info("Logging started → console (INFO+) and financial_news.log (DEBUG+)")


def get_logger(name: str) -> logging.Logger:
    """
    Get a named logger for a module.

    Usage in any file:
        from logger import get_logger
        log = get_logger(__name__)

        log.info("Scraping started")
        log.warning("Feed returned 0 articles")
        log.error(f"DB error: {e}")
        log.debug(f"Article hash: {url_hash}")
    """
    return logging.getLogger(name)