"""
Pipeline Scheduler
===================
Runs the daily pipeline at a configurable time each day using APScheduler.
This is a lightweight alternative to Airflow for single-machine deployments.

The scheduler runs as a persistent background process. Use a process manager
(launchd on macOS, systemd on Linux, or nohup) to keep it alive.

Usage:
    # Start scheduler (runs daily at 06:00 local time)
    python scheduler.py

    # Custom schedule
    python scheduler.py --hour 7 --minute 30

    # Run once immediately (useful for testing)
    python scheduler.py --run-now

    # macOS: run as background daemon via launchd
    # See: docs/launchd_setup.md

Installation:
    pip install apscheduler python-dotenv
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scheduler.log"),
    ],
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent


def run_daily_pipeline():
    """Job function called by the scheduler each day."""
    logger.info("=" * 60)
    logger.info("Scheduled daily pipeline triggered at %s", datetime.now().isoformat())
    logger.info("=" * 60)

    env_file = BASE_DIR / ".env"
    env = os.environ.copy()
    if env_file.exists():
        # Manually load .env into env dict
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, val = line.partition("=")
                    env[key.strip()] = val.strip().strip('"').strip("'")

    pipeline_script = BASE_DIR / "pipeline" / "daily_pipeline.py"
    cmd = [sys.executable, str(pipeline_script)]

    result = subprocess.run(cmd, env=env, capture_output=False)
    if result.returncode == 0:
        logger.info("Daily pipeline completed successfully")
    else:
        logger.error("Daily pipeline failed with exit code %d", result.returncode)


def main():
    parser = argparse.ArgumentParser(description="Flight data pipeline scheduler")
    parser.add_argument("--hour",    type=int, default=6,  help="Hour to run daily (24h, default 6)")
    parser.add_argument("--minute",  type=int, default=0,  help="Minute to run (default 0)")
    parser.add_argument("--run-now", action="store_true",  help="Run pipeline immediately then exit")
    args = parser.parse_args()

    if args.run_now:
        logger.info("Running pipeline immediately (--run-now)")
        run_daily_pipeline()
        return

    scheduler = BlockingScheduler(timezone="America/Chicago")  # WashU is in Central time

    trigger = CronTrigger(hour=args.hour, minute=args.minute)
    scheduler.add_job(
        run_daily_pipeline,
        trigger=trigger,
        id="daily_flight_pipeline",
        name="Daily Flight Data Pipeline",
        misfire_grace_time=3600,  # allow up to 1h late execution
    )

    logger.info(
        "Scheduler started. Daily pipeline will run at %02d:%02d (Central).",
        args.hour, args.minute
    )
    logger.info("Press Ctrl+C to stop.")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
        scheduler.shutdown()


if __name__ == "__main__":
    main()
