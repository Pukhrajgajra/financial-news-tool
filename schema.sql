-- =============================================================================
-- Financial News Analytics Tool — Database Schema
-- PostgreSQL 14+
--
-- Usage:
--   createdb financial_news
--   psql -U postgres -d financial_news -f schema.sql
--
-- Tables:
--   articles              Raw scraped articles
--   sentiment_scores      TextBlob / FinBERT sentiment per article
--   named_entities        spaCy NER results (ORG, PERSON, GPE, MONEY)
--   stock_prices          Daily OHLCV data from yfinance
--   sentiment_correlations  Joined sentiment ↔ next-day price movement
--   failed_articles       Dead-letter queue for failed scrape / NLP jobs
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Extensions
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "pgcrypto";   -- for gen_random_uuid() if needed


-- ---------------------------------------------------------------------------
-- articles
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS articles (
    id              SERIAL          PRIMARY KEY,
    title           TEXT            NOT NULL,
    url             TEXT            NOT NULL,
    summary         TEXT,
    full_text       TEXT,
    source          TEXT,
    tickers         TEXT,           -- comma-separated, e.g. "AAPL,MSFT" or "GENERAL"
    published_at    TEXT,           -- stored as raw string; parsed in Python
    scraped_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT articles_url_unique UNIQUE (url)
);

CREATE INDEX IF NOT EXISTS idx_articles_tickers
    ON articles (tickers);

CREATE INDEX IF NOT EXISTS idx_articles_scraped_at
    ON articles (scraped_at DESC);

CREATE INDEX IF NOT EXISTS idx_articles_published_at
    ON articles (published_at);

COMMENT ON TABLE  articles              IS 'Raw articles scraped from RSS feeds and full-text fetcher.';
COMMENT ON COLUMN articles.tickers      IS 'Comma-separated ticker symbols detected via keyword matching. "GENERAL" if none matched.';
COMMENT ON COLUMN articles.published_at IS 'Raw publication date string from RSS feed — parsed to date in Python before joins.';


-- ---------------------------------------------------------------------------
-- sentiment_scores
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sentiment_scores (
    id              SERIAL          PRIMARY KEY,
    article_id      INTEGER         NOT NULL REFERENCES articles (id) ON DELETE CASCADE,
    score           NUMERIC(6, 4)   NOT NULL,   -- Polarity: -1.0 to +1.0
    label           TEXT            NOT NULL,   -- 'positive' | 'negative' | 'neutral'
    model           TEXT            NOT NULL DEFAULT 'textblob',  -- 'textblob' | 'finbert'
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT sentiment_scores_article_model_unique UNIQUE (article_id, model),
    CONSTRAINT sentiment_label_check CHECK (label IN ('positive', 'negative', 'neutral')),
    CONSTRAINT sentiment_score_range  CHECK (score BETWEEN -1.0 AND 1.0)
);

CREATE INDEX IF NOT EXISTS idx_sentiment_article_id
    ON sentiment_scores (article_id);

CREATE INDEX IF NOT EXISTS idx_sentiment_label
    ON sentiment_scores (label);

COMMENT ON TABLE  sentiment_scores       IS 'Sentiment analysis results. One row per article per model (supports TextBlob and FinBERT side-by-side).';
COMMENT ON COLUMN sentiment_scores.model IS 'Model that produced this score — allows running multiple models and comparing.';


-- ---------------------------------------------------------------------------
-- named_entities
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS named_entities (
    id              SERIAL          PRIMARY KEY,
    article_id      INTEGER         NOT NULL REFERENCES articles (id) ON DELETE CASCADE,
    entity          TEXT            NOT NULL,
    entity_type     TEXT            NOT NULL,   -- 'ORG' | 'PERSON' | 'GPE' | 'MONEY'
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT named_entities_entity_type_check
        CHECK (entity_type IN ('ORG', 'PERSON', 'GPE', 'MONEY'))
);

CREATE INDEX IF NOT EXISTS idx_named_entities_article_id
    ON named_entities (article_id);

CREATE INDEX IF NOT EXISTS idx_named_entities_entity
    ON named_entities (entity);

CREATE INDEX IF NOT EXISTS idx_named_entities_type
    ON named_entities (entity_type);

COMMENT ON TABLE named_entities IS 'spaCy NER results. ORG=organisations, PERSON=people, GPE=geopolitical entities, MONEY=monetary values.';


-- ---------------------------------------------------------------------------
-- stock_prices
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stock_prices (
    id              SERIAL          PRIMARY KEY,
    ticker          TEXT            NOT NULL,
    price_date      DATE            NOT NULL,
    open_price      NUMERIC(12, 4),
    close_price     NUMERIC(12, 4)  NOT NULL,
    high_price      NUMERIC(12, 4),
    low_price       NUMERIC(12, 4),
    volume          BIGINT,
    pct_change      NUMERIC(8, 4),  -- percentage change from previous close
    fetched_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT stock_prices_ticker_date_unique UNIQUE (ticker, price_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker
    ON stock_prices (ticker);

CREATE INDEX IF NOT EXISTS idx_stock_prices_date
    ON stock_prices (price_date DESC);

CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_date
    ON stock_prices (ticker, price_date DESC);

COMMENT ON TABLE  stock_prices            IS 'Daily OHLCV stock prices fetched from yfinance.';
COMMENT ON COLUMN stock_prices.pct_change IS 'Percentage change from the previous trading day close price.';


-- ---------------------------------------------------------------------------
-- sentiment_correlations
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sentiment_correlations (
    id                    SERIAL        PRIMARY KEY,
    ticker                TEXT          NOT NULL,
    article_id            INTEGER       NOT NULL REFERENCES articles (id) ON DELETE CASCADE,
    sentiment_score       NUMERIC(6, 4) NOT NULL,
    sentiment_label       TEXT          NOT NULL,
    price_date            DATE,
    close_price           NUMERIC(12, 4),
    next_day_close        NUMERIC(12, 4),
    next_day_pct_change   NUMERIC(8, 4),
    created_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    CONSTRAINT sentiment_correlations_label_check
        CHECK (sentiment_label IN ('positive', 'negative', 'neutral'))
);

CREATE INDEX IF NOT EXISTS idx_sent_corr_ticker
    ON sentiment_correlations (ticker);

CREATE INDEX IF NOT EXISTS idx_sent_corr_article_id
    ON sentiment_correlations (article_id);

CREATE INDEX IF NOT EXISTS idx_sent_corr_price_date
    ON sentiment_correlations (price_date DESC);

CREATE INDEX IF NOT EXISTS idx_sent_corr_ticker_date
    ON sentiment_correlations (ticker, price_date DESC);

COMMENT ON TABLE  sentiment_correlations                    IS 'Pre-joined table linking article sentiment to the next trading day price change. Rebuilt by stock_analyzer.py.';
COMMENT ON COLUMN sentiment_correlations.next_day_pct_change IS 'Percentage price change on the trading day after the article was published.';


-- ---------------------------------------------------------------------------
-- failed_articles  (dead-letter queue)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS failed_articles (
    id              SERIAL          PRIMARY KEY,
    url             TEXT            NOT NULL,
    title           TEXT,
    source          TEXT,
    stage           TEXT            NOT NULL,   -- 'scrape' | 'nlp' | 'stock'
    error_message   TEXT,
    raw_data        JSONB,          -- full raw article dict for later retry
    failed_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    retry_count     INTEGER         NOT NULL DEFAULT 0,
    resolved        BOOLEAN         NOT NULL DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ,

    CONSTRAINT failed_articles_stage_check
        CHECK (stage IN ('scrape', 'nlp', 'stock'))
);

CREATE INDEX IF NOT EXISTS idx_failed_articles_stage
    ON failed_articles (stage);

CREATE INDEX IF NOT EXISTS idx_failed_articles_resolved
    ON failed_articles (resolved) WHERE resolved = FALSE;

CREATE INDEX IF NOT EXISTS idx_failed_articles_failed_at
    ON failed_articles (failed_at DESC);

COMMENT ON TABLE  failed_articles           IS 'Dead-letter queue. Rows here represent articles that failed at some pipeline stage and need investigation or retry.';
COMMENT ON COLUMN failed_articles.stage     IS 'Pipeline stage where failure occurred: scrape=HTTP fetch, nlp=sentiment/NER, stock=price correlation.';
COMMENT ON COLUMN failed_articles.raw_data  IS 'Full article dict as JSONB so failed articles can be retried without re-scraping.';
COMMENT ON COLUMN failed_articles.resolved  IS 'Set to TRUE once the failure is investigated and either fixed or dismissed.';