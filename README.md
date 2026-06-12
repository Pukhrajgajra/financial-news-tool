# Financial News Sentiment Analyzer

![CI](https://github.com/Pukhrajgajra/financial-news-tool/actions/workflows/ci.yml/badge.svg)

A data pipeline and dashboard that scrapes financial news, scores its sentiment, and tests whether that sentiment predicts the **next day's stock price movement**. Built for anyone curious about the news-vs-market relationship — and as a study in doing the analysis *honestly*.

---

## Key finding (and why it matters)

Across the data collected, news sentiment showed **no statistically significant relationship** with next-day price movement:

> Pooled Pearson **r = −0.18**, **p = 0.37**, **N = 28 ticker-days**.

The interesting part is how I got there. An early version reported a strong correlation (r ≈ 0.67) — which turned out to be wrong for two reasons:

1. **Fabricated matches.** When an article had no real "next trading day" in the price table, the code silently fell back to the most recent price move, stamping many unrelated articles with the same number.
2. **Non-independent observations.** Dozens of articles about the same company on the same day all map to a *single* price move, so counting each article as a data point inflated the sample from ~28 real observations to "208."

After fixing the matching (skip-and-count instead of fabricate) and aggregating to **one point per ticker-day**, the effect disappeared. That null result — and being able to demonstrate it rigorously — is the honest outcome. This is exploratory analysis on a small, time-clustered sample, not a trading signal.

---

## What it does

The pipeline runs in four stages:

1. **Scrape** financial news from a set of RSS feeds and tag each article with the stock tickers it mentions (keyword matching).
2. **Analyze** each article for sentiment (TextBlob polarity, ±0.1 thresholds) and named entities (spaCy NER: organizations, people, places, money).
3. **Fetch** daily price history for the tracked tickers (yfinance) and match each article to its real before/after trading days.
4. **Correlate** sentiment against the next-day return, per ticker-day, and surface everything in an interactive dashboard.

A connection pool, structured logging, and a "dead-letter" table for failed jobs keep it from failing silently.

---

## Tech stack

| Area            | Tools                                              |
| --------------- | -------------------------------------------------- |
| Language        | Python 3.12                                        |
| Database        | PostgreSQL, Alembic (migrations), psycopg2 pool    |
| NLP             | spaCy (NER), TextBlob (sentiment)                  |
| Market data     | yfinance                                           |
| Analysis        | scipy, pandas                                      |
| Dashboard       | Streamlit, Altair                                  |
| Scheduling      | APScheduler                                        |
| Quality / CI    | pytest, ruff, GitHub Actions                       |
| Packaging       | Docker, Docker Compose                             |

---

## Quickstart (Docker — recommended)

The whole stack — Postgres with the schema pre-loaded, plus the dashboard — comes up with one command.

```bash
cp .env.example .env          # then set DB_PASSWORD to any value
docker compose up
```

Open **http://localhost:8501** for the dashboard.

On a fresh start the database is empty, so the dashboard shows an empty state until you load some data. Run the pipeline once:

```bash
docker compose run --rm dashboard python scraper.py
docker compose run --rm dashboard python nlp_processor.py
docker compose run --rm dashboard python stock_analyzer.py
```

Then refresh the dashboard. To run the scrape/analysis loop continuously instead, use `docker compose --profile pipeline up`.

---

## Quickstart (local, without Docker)

Requires a running PostgreSQL instance.

```bash
# 1. Install dependencies
pip install -r requirements.txt
python -m spacy download en_core_web_sm

# 2. Create the database and tables
createdb financial_news
psql -U postgres -d financial_news -f schema.sql

# 3. Configure credentials
cp .env.example .env          # edit DB_* values to match your setup

# 4. Run the pipeline, then the dashboard
python scheduler.py           # or run scraper.py / nlp_processor.py / stock_analyzer.py individually
streamlit run app.py
```

---

## Project structure

```
.
├── scraper.py            # RSS scraping + ticker detection
├── nlp_processor.py      # sentiment scoring + named-entity extraction
├── stock_analyzer.py     # price fetching + sentiment/price correlation
├── scheduler.py          # APScheduler jobs (scrape→NLP, and analysis)
├── app.py                # Streamlit dashboard
├── db_pool.py            # PostgreSQL connection pool
├── db_writer.py          # article persistence
├── db_config.py          # env-based configuration
├── logger.py             # shared logging setup
├── schema.sql            # database schema
├── migrations/           # Alembic migrations
├── tests/                # pytest suite
├── Dockerfile
├── docker-compose.yml
└── .github/workflows/    # GitHub Actions CI
```

---

## Testing

```bash
pip install -r requirements-dev.txt
ruff check .
pytest
```

The suite covers the pure logic — ticker detection, date parsing, and sentiment thresholds — and includes an `xfail` test documenting a known limitation of keyword-based ticker matching (e.g. "meta" matching inside "metabolism"). CI runs the same lint and tests on every push.

---

## Limitations & what I'd improve

- **Small, time-clustered sample.** The data spans a short window, so per-ticker correlations don't have enough independent days to be meaningful. Running the scraper over weeks would build a far stronger dataset.
- **Sentiment model.** TextBlob is general-purpose; a finance-tuned model like FinBERT would read market language ("beats earnings, falls on guidance") far better. The schema already supports storing multiple models per article for comparison.
- **Ticker detection** uses keyword matching, which has false positives. Word-boundary matching or an NER-based approach would be more precise.

---

## License

Released under the MIT License — see `LICENSE` for details.