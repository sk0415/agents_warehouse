"""
ETL Scheduler
Purpose: Schedule extract and load operations to run daily at 4 PM EST (market close).
Runs the ETL pipeline automatically at the specified time using APScheduler.

Usage:
    python scheduler.py
    
    To run in the background:
    - Windows: pythonw scheduler.py
    - Linux/Mac: nohup python scheduler.py &
"""

import logging
import pytz
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pathlib import Path
from dotenv import load_dotenv

# Import ETL functions
from extract import run_extraction
from load import run_load

# Load environment variables
load_dotenv()

# ============================================================
# LOGGING SETUP
# ============================================================

def setup_scheduler_logging(log_dir="./etl_logs"):
    """Configure logging for scheduler operations."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    log_file = Path(log_dir) / "scheduler.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)


logger = setup_scheduler_logging()


# ============================================================
# ETL PIPELINE ORCHESTRATION
# ============================================================

def run_etl_pipeline():
    """
    Execute the complete ETL pipeline:
    1. Extract data from operational database
    2. Load data into warehouse
    """
    try:
        logger.info("=" * 60)
        logger.info("STARTING ETL PIPELINE - Market Close Run")
        logger.info(f"Time: {datetime.now(pytz.timezone('US/Eastern'))}")
        logger.info("=" * 60)
        
        # Step 1: Extract
        logger.info("STEP 1: Extracting data from operational database...")
        try:
            run_extraction(
                output_dir="./extracted_data",
                extract_since=None,
                full_extract=False
            )
            logger.info("✓ Extraction completed successfully")
        except Exception as e:
            logger.error(f"✗ Extraction failed: {e}")
            raise
        
        # Step 2: Load
        logger.info("STEP 2: Loading data into warehouse...")
        try:
            run_load(
                input_dir="./extracted_data",
                mode='incremental'
            )
            logger.info("✓ Load completed successfully")
        except Exception as e:
            logger.error(f"✗ Load failed: {e}")
            raise
        
        logger.info("=" * 60)
        logger.info("✓ ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"✗ ETL PIPELINE FAILED: {e}")
        logger.error("=" * 60)
        raise


# ============================================================
# SCHEDULER SETUP
# ============================================================

def setup_scheduler():
    """
    Configure and start the background scheduler.
    Runs ETL pipeline daily at 4 PM EST (16:00 Eastern Time).
    """
    
    # Create scheduler
    scheduler = BackgroundScheduler()
    
    # Use US/Eastern timezone for market close time
    eastern = pytz.timezone('US/Eastern')
    
    # Configure trigger for 4 PM EST, Monday-Friday (trading days)
    # hour=16 (4 PM), minute=0, day_of_week=0-4 (Mon-Fri)
    trigger = CronTrigger(
        hour=16,
        minute=0,
        day_of_week='mon-fri',
        timezone=eastern
    )
    
    # Add the job to the scheduler
    job = scheduler.add_job(
        run_etl_pipeline,
        trigger=trigger,
        id='etl_market_close',
        name='ETL Pipeline at Market Close (4 PM EST)',
        misfire_grace_time=300,  # Allow 5 minutes grace if execution was delayed
        coalesce=True  # Don't run multiple times if missed
    )
    
    logger.info("Scheduler configured:")
    logger.info(f"  Job ID: {job.id}")
    logger.info(f"  Job Name: {job.name}")
    logger.info(f"  Trigger: Daily at 4 PM EST (Mon-Fri)")
    logger.info(f"  Timezone: {eastern}")
    logger.info(f"  Next run: {job.next_run_time}")
    
    # Start the scheduler
    scheduler.start()
    logger.info("✓ Scheduler started successfully")
    
    return scheduler


# ============================================================
# MAIN ENTRY POINT
# ============================================================

if __name__ == "__main__":
    try:
        logger.info("Starting ETL Scheduler Service...")
        scheduler = setup_scheduler()
        
        # Keep the main thread alive
        import time
        logger.info("Scheduler running. Press Ctrl+C to stop.")
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Received shutdown signal (Ctrl+C)")
        scheduler.shutdown()
        logger.info("✓ Scheduler stopped gracefully")
        
    except Exception as e:
        logger.error(f"Fatal error in scheduler: {e}", exc_info=True)
        if 'scheduler' in locals():
            scheduler.shutdown()
        raise
