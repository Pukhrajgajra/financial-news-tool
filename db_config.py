import os
from dotenv import load_dotenv

load_dotenv()

def get_db_config():
    return {
        "dbname":   os.environ.get("DB_NAME",     "financial_news"),
        "user":     os.environ.get("DB_USER",     "postgres"),
        "host":     os.environ.get("DB_HOST",     "localhost"),
        "port":     os.environ.get("DB_PORT",     "5432"),
        "password": os.environ.get("DB_PASSWORD", ""),
    }
