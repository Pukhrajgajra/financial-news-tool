from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from logger import setup_logging, get_logger
from db_pool import initialize_pool, close_pool

# Import the actual pipeline functions
from scraper import run_pipeline
from nlp_processor import process_all_articles
from stock_analyzer import run_full_analysis

log = get_logger(__name__)

# How often to run (change these to suit your needs)
SCRAPE_INTERVAL_HOURS = 2
STOCK_INTERVAL_HOURS = 24   # stock analysis once a day is enough


def run_scrape_and_nlp():
    """
    One combined job: scrape news → immediately run NLP on new articles.
    Runs every SCRAPE_INTERVAL_HOURS hours.
    """
    log.info("=" * 50)
    log.info("JOB STARTED: scrape + NLP")
    log.info("=" * 50)
    try:
        run_pipeline()          # step 1: scrape new articles
        process_all_articles()  # step 2: run sentiment on unprocessed ones
        log.info("JOB COMPLETE: scrape + NLP finished successfully")
    except Exception as e:
        # Log the error but don't crash the scheduler —
        # the next run will still happen as scheduled
        log.error(f"JOB FAILED: {e}", exc_info=True)
        # exc_info=True tells the logger to include the full traceback
        # You'll see the exact line that caused the error in the log file


def run_stock_job():
    """
    Stock price fetch + correlation analysis.
    Runs every STOCK_INTERVAL_HOURS hours (daily).
    """
    log.info("=" * 50)
    log.info("JOB STARTED: stock analysis")
    log.info("=" * 50)
    try:
        run_full_analysis()
        log.info("JOB COMPLETE: stock analysis finished successfully")
    except Exception as e:
        log.error(f"JOB FAILED (stock): {e}", exc_info=True)


def job_listener(event):
    """
    APScheduler fires events after each job runs.
    We use this to log whether each job succeeded or failed.

    EVENT_JOB_EXECUTED → job finished normally
    EVENT_JOB_ERROR    → job raised an unhandled exception
    """
    if event.exception:
        log.error(f"Scheduler caught unhandled error in job [{event.job_id}]")
    else:
        log.info(f"Job [{event.job_id}] completed successfully")


def main():
    setup_logging()
    log.info("Financial News Scheduler starting up...")
    log.info(f"Scrape + NLP  → every {SCRAPE_INTERVAL_HOURS} hours")
    log.info(f"Stock analysis → every {STOCK_INTERVAL_HOURS} hours")

    # Initialize the connection pool ONCE here — all jobs share it
    initialize_pool()

    scheduler = BlockingScheduler()

    # Listen to job events (success/failure)
    scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)

    # --- Job 1: scrape + NLP ---
    # run_date=None means "also run immediately when the scheduler starts"
    # trigger='interval' means "then repeat every N hours"
    scheduler.add_job(
        run_scrape_and_nlp,
        trigger='interval',
        hours=SCRAPE_INTERVAL_HOURS,
        id='scrape_nlp',
        name='Scrape + NLP Pipeline',
        misfire_grace_time=3600,    # if missed by <1hr, still run it
        next_run_time=__import__('datetime').datetime.now()  # run immediately on start
    )

    # --- Job 2: stock analysis ---
    scheduler.add_job(
        run_stock_job,
        trigger='interval',
        hours=STOCK_INTERVAL_HOURS,
        id='stock_analysis',
        name='Stock Price + Correlation Analysis',
        misfire_grace_time=3600,
    )

    log.info("Scheduler started. Press Ctrl+C to stop.")

    try:
        scheduler.start()   # blocks here forever — runs jobs on schedule
    except KeyboardInterrupt:
        # User pressed Ctrl+C — shut down cleanly
        log.info("Scheduler stopped by user (Ctrl+C)")
    finally:
        close_pool()        # always close DB connections on exit
        log.info("Shutdown complete.")


if __name__ == "__main__":
    main()