# Financial News Analytics & Research Tool

A Python pipeline that scrapes live financial news, runs NLP analysis, and stores results in PostgreSQL for Tableau visualization.

# What it does
- Scrapes articles from Reuters, Yahoo Finance, MarketWatch via RSS
- Extracts full article text using BeautifulSoup
- Deduplicates articles using URL hashing
- Stores everything in PostgreSQL (articles, sentiment, named entities)
- Runs NLP sentiment analysis (TextBlob) and entity extraction (spaCy)

# Tech Stack
Python · PostgreSQL · spaCy · TextBlob · BeautifulSoup · APScheduler

# How to run
pip install requests beautifulsoup4 feedparser psycopg2-binary apscheduler textblob spacy
python -m spacy download en_core_web_sm
python scraper.py
python nlp_processor.py
