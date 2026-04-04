"""
APScheduler cron: runs the monthly scrape on the 15th of each month.

The 15th is when GACC typically publishes the previous month's trade statistics.
The job scrapes data for the *previous* month (e.g. running on Feb 15 → scrapes Jan).
"""
import logging
from datetime import date

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import DB_PATH, SCHEDULER_DAY, SCHEDULER_HOUR, SCHEDULER_TIMEZONE
from .db import init_db
from .orchestrator import ScrapeOrchestrator

logger = logging.getLogger(__name__)


def _run_monthly_job(db_path: str = DB_PATH) -> None:
    """Determine which month to scrape and run it."""
    today = date.today()
    # Scrape the previous month's data
    if today.month == 1:
        year, month = today.year - 1, 12
    else:
        year, month = today.year, today.month - 1

    logger.info(f"Scheduler triggered: scraping {year}-{month:02d}")
    try:
        orchestrator = ScrapeOrchestrator(year=year, month=month, db_path=db_path)
        status = orchestrator.run()
        logger.info(f"Scheduled run finished: status={status}")
    except Exception as exc:
        logger.error(f"Scheduled run failed: {exc}", exc_info=True)


def start_scheduler(db_path: str = DB_PATH) -> None:
    """
    Start the blocking APScheduler daemon.
    Runs on day=SCHEDULER_DAY, hour=SCHEDULER_HOUR (Asia/Shanghai time).
    Blocks until the process is killed.
    """
    init_db(db_path)

    scheduler = BlockingScheduler(timezone=SCHEDULER_TIMEZONE)
    trigger = CronTrigger(
        day=SCHEDULER_DAY,
        hour=SCHEDULER_HOUR,
        minute=0,
        timezone=SCHEDULER_TIMEZONE,
        jitter=300,  # ±5 min randomisation
    )
    scheduler.add_job(
        func=_run_monthly_job,
        trigger=trigger,
        kwargs={"db_path": db_path},
        id="monthly_customs_scrape",
        name="China Customs monthly export scrape",
        misfire_grace_time=3600,  # run within 1 hour if machine was off at trigger time
        coalesce=True,            # if multiple misfires, run once not many times
        max_instances=1,
    )

    logger.info(
        f"Scheduler started — fires on day={SCHEDULER_DAY} "
        f"hour={SCHEDULER_HOUR}:00 tz={SCHEDULER_TIMEZONE}"
    )
    scheduler.start()  # blocks here
