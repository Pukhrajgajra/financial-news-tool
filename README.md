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

## Live Dashboards
- Sentiment Dashboard: https://public.tableau.com/views/FinancialNewsAnalyticsDashboard/Dashboard1
- Stock Correlation Dashboard: https://public.tableau.com/views/Analysis_17775053649870/Dashboard1?:language=en-GB&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link

## Key Finding
Discovered statistically significant positive correlation (r=0.67, p=0.034) 
between news sentiment and next-day AAPL price movement across 208 article-price pairs.