"""
ETL Extraction Layer
Purpose: Extract operational data from the operational database for warehouse loading.
Includes data validation, error handling, and incremental extraction support.

Usage:
    python extract.py --since "2024-01-01 00:00:00" --output ./extracted_data
    python extract.py --full  # Full extract (no timestamp filter)
"""

import psycopg2
from psycopg2 import sql
import os
import sys
import logging
import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
import csv
from io import StringIO

# Load environment variables
load_dotenv()

# ============================================================
# LOGGING SETUP
# ============================================================

def setup_logging(log_dir="./etl_logs"):
    """Configure logging for ETL operations."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    log_file = os.path.join(
        log_dir,
        f"extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

logger = setup_logging()

# ============================================================
# DATABASE CONNECTION
# ============================================================

def connect_to_db():
    """Establish connection to PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        logger.info("Connected to PostgreSQL database")
        return connection
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise

def close_connection(connection):
    """Close database connection."""
    if connection:
        connection.close()
        logger.info("Database connection closed")

# ============================================================
# DATA VALIDATION
# ============================================================

class DataValidator:
    """Validates extracted data for quality and consistency."""
    
    def __init__(self):
        self.validation_errors = []
        self.validation_warnings = []
    
    def validate_row_count(self, table_name, count):
        """Check if extracted row count is reasonable."""
        if count < 0:
            self.validation_errors.append(f"{table_name}: Negative row count {count}")
            return False
        if count == 0:
            self.validation_warnings.append(f"{table_name}: No rows extracted")
        return True
    
    def validate_null_critical_fields(self, table_name, rows, critical_fields):
        """Check for null values in critical fields."""
        null_count = 0
        for row in rows:
            for field in critical_fields:
                if row.get(field) is None:
                    null_count += 1
        
        if null_count > 0:
            self.validation_warnings.append(
                f"{table_name}: {null_count} null values in critical fields"
            )
        return True
    
    def validate_uuid_format(self, value):
        """Check if value is valid UUID format."""
        if value is None:
            return False
        try:
            import uuid
            uuid.UUID(str(value))
            return True
        except ValueError:
            return False
    
    def validate_numeric_range(self, table_name, rows, field, min_val=None, max_val=None):
        """Validate numeric fields are within expected range."""
        for row in rows:
            val = row.get(field)
            if val is not None:
                if min_val is not None and val < min_val:
                    self.validation_warnings.append(
                        f"{table_name}.{field}: Value {val} below minimum {min_val}"
                    )
                if max_val is not None and val > max_val:
                    self.validation_warnings.append(
                        f"{table_name}.{field}: Value {val} exceeds maximum {max_val}"
                    )
    
    def report(self):
        """Log all validation results."""
        if self.validation_errors:
            logger.error(f"[ERROR] Validation Errors ({len(self.validation_errors)}):")
            for err in self.validation_errors:
                logger.error(f"  - {err}")
            return False
        
        if self.validation_warnings:
            logger.warning(f"[WARNING] Validation Warnings ({len(self.validation_warnings)}):")
            for warn in self.validation_warnings:
                logger.warning(f"  - {warn}")
        
        return True

# ============================================================
# EXTRACTION FUNCTIONS
# ============================================================

def extract_to_csv(connection, query, output_file, table_name, params=None):
    """Execute query and write results to CSV."""
    try:
        cursor = connection.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Get column names
        colnames = [desc[0] for desc in cursor.description]
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        logger.info(f"  Extracting {table_name}: {len(rows)} rows")
        
        # Write to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(colnames)
            writer.writerows(rows)
        
        cursor.close()
        return len(rows), colnames
    
    except psycopg2.Error as e:
        logger.error(f"[ERROR] Extraction failed for {table_name}: {e}")
        raise

def extract_clients(connection, output_dir, extract_since=None):
    """Extract client dimension data."""
    logger.info("-> Extracting CLIENTS dimension...")
    
    query = """
        SELECT
            client_id,
            first_name,
            middle_name,
            last_name,
            email,
            date_of_birth,
            join_date,
            portfolio_size_range,
            risk_tolerance,
            CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS extract_timestamp
        FROM clients
        WHERE join_date >= %s
        ORDER BY join_date DESC
    """
    
    output_file = os.path.join(output_dir, "dim_clients.csv")
    row_count, cols = extract_to_csv(
        connection, 
        query,
        output_file,
        "clients",
        params=(extract_since or datetime(1900, 1, 1),)
    )
    
    # Validation
    validator = DataValidator()
    validator.validate_row_count("clients", row_count)
    
    logger.info(f"  [OK] Written to {output_file}")
    return validator

def extract_accounts(connection, output_dir, extract_since=None):
    """Extract account dimension data."""
    logger.info("-> Extracting ACCOUNTS dimension...")
    
    query = """
        SELECT
            account_id,
            client_id,
            name AS account_name,
            cash_balance,
            status,
            open_date,
            CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS extract_timestamp
        FROM accounts
        WHERE open_date >= %s
        ORDER BY open_date DESC
    """
    
    output_file = os.path.join(output_dir, "dim_accounts.csv")
    row_count, cols = extract_to_csv(
        connection,
        query,
        output_file,
        "accounts",
        params=(extract_since or datetime(1900, 1, 1),)
    )
    
    validator = DataValidator()
    validator.validate_row_count("accounts", row_count)
    
    logger.info(f"  [OK] Written to {output_file}")
    return validator

def extract_instruments(connection, output_dir):
    """Extract instrument/security master data."""
    logger.info("-> Extracting INSTRUMENTS dimension...")
    
    query = """
        SELECT
            instrument_id,
            ticker,
            name AS instrument_name,
            asset_class,
            industry,
            bid,
            ask,
            mid_price,
            price_updated_at,
            CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS extract_timestamp
        FROM instruments
        ORDER BY ticker
    """
    
    output_file = os.path.join(output_dir, "dim_instruments.csv")
    row_count, cols = extract_to_csv(
        connection,
        query,
        output_file,
        "instruments"
    )
    
    validator = DataValidator()
    validator.validate_row_count("instruments", row_count)
    
    logger.info(f"  [OK] Written to {output_file}")
    return validator

def extract_orders(connection, output_dir, extract_since=None):
    """Extract order fact data."""
    logger.info("-> Extracting ORDERS fact table...")
    
    query = """
        SELECT
            order_id,
            account_id,
            instrument_id,
            order_type,
            quantity,
            limit_price,
            filled_price,
            status,
            created_at,
            filled_at,
            cancelled_at,
            cancel_reason,
            EXTRACT(EPOCH FROM (
                COALESCE(filled_at, cancelled_at, CURRENT_TIMESTAMP AT TIME ZONE 'UTC') 
                - created_at
            )) AS duration_seconds,
            CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS extract_timestamp
        FROM orders
        WHERE created_at >= %s
           OR COALESCE(filled_at, cancelled_at) >= %s
        ORDER BY created_at DESC
    """
    
    output_file = os.path.join(output_dir, "fact_orders.csv")
    extract_since_dt = extract_since or datetime(1900, 1, 1)
    row_count, cols = extract_to_csv(
        connection,
        query,
        output_file,
        "orders",
        params=(extract_since_dt, extract_since_dt)
    )
    
    validator = DataValidator()
    validator.validate_row_count("orders", row_count)
    
    logger.info(f"  [OK] Written to {output_file}")
    return validator

def extract_transactions(connection, output_dir, extract_since=None):
    """Extract transaction (cash flow) fact data."""
    logger.info("-> Extracting TRANSACTIONS fact table...")
    
    query = """
        SELECT
            transaction_id,
            account_id,
            txn_type,
            amount,
            created_at,
            CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS extract_timestamp
        FROM transactions
        WHERE created_at >= %s
        ORDER BY created_at DESC
    """
    
    output_file = os.path.join(output_dir, "fact_transactions.csv")
    row_count, cols = extract_to_csv(
        connection,
        query,
        output_file,
        "transactions",
        params=(extract_since or datetime(1900, 1, 1),)
    )
    
    validator = DataValidator()
    validator.validate_row_count("transactions", row_count)
    
    logger.info(f"  [OK] Written to {output_file}")
    return validator

def extract_holdings(connection, output_dir):
    """Extract current holdings snapshot."""
    logger.info("-> Extracting HOLDINGS snapshot...")
    
    query = """
        SELECT
            holding_id,
            account_id,
            instrument_id,
            quantity,
            average_cost_basis,
            CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS extract_timestamp
        FROM holdings
        ORDER BY account_id
    """
    
    output_file = os.path.join(output_dir, "snapshot_holdings.csv")
    row_count, cols = extract_to_csv(
        connection,
        query,
        output_file,
        "holdings"
    )
    
    validator = DataValidator()
    validator.validate_row_count("holdings", row_count)
    
    logger.info(f"  [OK] Written to {output_file}")
    return validator

def extract_historical_snapshots(connection, output_dir, extract_since=None):
    """Extract EOD portfolio snapshots."""
    logger.info("-> Extracting HISTORICAL_SNAPSHOTS...")
    
    query = """
        SELECT
            snapshot_id,
            account_id,
            snapshot_date,
            cash_balance,
            holdings_value,
            total_value,
            CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS extract_timestamp
        FROM historical_snapshot
        WHERE snapshot_date >= %s
        ORDER BY snapshot_date DESC
    """
    
    output_file = os.path.join(output_dir, "fact_historical_snapshots.csv")
    extract_since_date = extract_since.date() if extract_since else datetime(1900, 1, 1)
    row_count, cols = extract_to_csv(
        connection,
        query,
        output_file,
        "historical_snapshot",
        params=(extract_since_date,)
    )
    
    validator = DataValidator()
    validator.validate_row_count("historical_snapshot", row_count)
    
    logger.info(f"  [OK] Written to {output_file}")
    return validator

def extract_audit_logs(connection, output_dir, extract_since=None):
    """Extract audit trail event stream."""
    logger.info("-> Extracting AUDIT_LOGS event stream...")
    
    query = """
        SELECT
            audit_log_id,
            client_id,
            account_id,
            order_id,
            event_type,
            event_time,
            reason,
            details,
            CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS extract_timestamp
        FROM audit_logs
        WHERE event_time >= %s
        ORDER BY event_time DESC
    """
    
    output_file = os.path.join(output_dir, "fact_audit_logs.csv")
    row_count, cols = extract_to_csv(
        connection,
        query,
        output_file,
        "audit_logs",
        params=(extract_since or datetime(1900, 1, 1),)
    )
    
    validator = DataValidator()
    validator.validate_row_count("audit_logs", row_count)
    
    logger.info(f"  [OK] Written to {output_file}")
    return validator

def extract_price_history(connection, output_dir, extract_since=None):
    """Extract instrument price history time series."""
    logger.info("-> Extracting INSTRUMENT_PRICE_HISTORY time series...")
    
    query = """
        SELECT
            price_history_id,
            instrument_id,
            timestamp,
            open,
            high,
            low,
            close,
            volume,
            CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS extract_timestamp
        FROM instrument_price_history
        WHERE timestamp >= %s
        ORDER BY timestamp DESC
    """
    
    output_file = os.path.join(output_dir, "fact_price_history.csv")
    row_count, cols = extract_to_csv(
        connection,
        query,
        output_file,
        "instrument_price_history",
        params=(extract_since or datetime(1900, 1, 1),)
    )
    
    validator = DataValidator()
    validator.validate_row_count("instrument_price_history", row_count)
    
    logger.info(f"  [OK] Written to {output_file}")
    return validator

def extract_watchlists(connection, output_dir, extract_since=None):
    """Extract watchlist behavioral signals."""
    logger.info("-> Extracting WATCHLISTS behavioral data...")
    
    query = """
        SELECT
            watchlist_id,
            client_id,
            instrument_id,
            added_at,
            CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS extract_timestamp
        FROM watchlists
        WHERE added_at >= %s
        ORDER BY added_at DESC
    """
    
    output_file = os.path.join(output_dir, "fact_watchlists.csv")
    row_count, cols = extract_to_csv(
        connection,
        query,
        output_file,
        "watchlists",
        params=(extract_since or datetime(1900, 1, 1),)
    )
    
    validator = DataValidator()
    validator.validate_row_count("watchlists", row_count)
    
    logger.info(f"  [OK] Written to {output_file}")
    return validator

# ============================================================
# MAIN ORCHESTRATION
# ============================================================

def run_extraction(output_dir="./extracted_data", extract_since=None, full_extract=False):
    """Orchestrate full ETL extraction."""
    
    logger.info("=" * 60)
    logger.info("STARTING ETL EXTRACTION")
    logger.info("=" * 60)
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Full extract: {full_extract}")
    if not full_extract and extract_since:
        logger.info(f"Incremental since: {extract_since}")
    logger.info("=" * 60)
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Connect to database
    connection = None
    extraction_summary = {}
    
    try:
        connection = connect_to_db()
        
        # Extract all datasets
        validators = []
        
        try:
            validators.append(("clients", extract_clients(connection, output_dir, extract_since)))
        except Exception as e:
            logger.error(f"Clients extraction failed: {e}")
        
        try:
            validators.append(("accounts", extract_accounts(connection, output_dir, extract_since)))
        except Exception as e:
            logger.error(f"Accounts extraction failed: {e}")
        
        try:
            validators.append(("instruments", extract_instruments(connection, output_dir)))
        except Exception as e:
            logger.error(f"Instruments extraction failed: {e}")
        
        try:
            validators.append(("orders", extract_orders(connection, output_dir, extract_since)))
        except Exception as e:
            logger.error(f"Orders extraction failed: {e}")
        
        try:
            validators.append(("transactions", extract_transactions(connection, output_dir, extract_since)))
        except Exception as e:
            logger.error(f"Transactions extraction failed: {e}")
        
        try:
            validators.append(("holdings", extract_holdings(connection, output_dir)))
        except Exception as e:
            logger.error(f"Holdings extraction failed: {e}")
        
        try:
            validators.append(("snapshots", extract_historical_snapshots(connection, output_dir, extract_since)))
        except Exception as e:
            logger.error(f"Historical snapshots extraction failed: {e}")
        
        try:
            validators.append(("audit_logs", extract_audit_logs(connection, output_dir, extract_since)))
        except Exception as e:
            logger.error(f"Audit logs extraction failed: {e}")
        
        try:
            validators.append(("price_history", extract_price_history(connection, output_dir, extract_since)))
        except Exception as e:
            logger.error(f"Price history extraction failed: {e}")
        
        try:
            validators.append(("watchlists", extract_watchlists(connection, output_dir, extract_since)))
        except Exception as e:
            logger.error(f"Watchlists extraction failed: {e}")
        
        # Report validation results
        logger.info("=" * 60)
        logger.info("VALIDATION SUMMARY")
        logger.info("=" * 60)
        
        all_valid = True
        for table_name, validator in validators:
            if not validator.report():
                all_valid = False
        
        if all_valid:
            logger.info("All validations passed")
        
        logger.info("=" * 60)
        logger.info("EXTRACTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
        return 0
    
    except Exception as e:
        logger.error(f"EXTRACTION FAILED: {e}", exc_info=True)
        return 1
    
    finally:
        if connection:
            close_connection(connection)

def main():
    parser = argparse.ArgumentParser(
        description="ETL Extraction: Extract data from operational database"
    )
    parser.add_argument(
        "--output",
        default="./extracted_data",
        help="Output directory for extracted CSV files (default: ./extracted_data)"
    )
    parser.add_argument(
        "--since",
        help="Incremental extraction since ISO datetime (e.g., '2024-01-01 00:00:00')"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Force full extraction (ignore --since)"
    )
    parser.add_argument(
        "--days",
        type=int,
        help="Extract last N days (alternative to --since)"
    )
    
    args = parser.parse_args()
    
    extract_since = None
    
    if not args.full:
        if args.since:
            try:
                extract_since = datetime.fromisoformat(args.since)
                logger.info(f"Using --since: {extract_since}")
            except ValueError:
                logger.error(f"Invalid datetime format: {args.since}")
                logger.error("Use ISO format: YYYY-MM-DD HH:MM:SS")
                return 1
        elif args.days:
            extract_since = datetime.now() - timedelta(days=args.days)
            logger.info(f"Using last {args.days} days, since: {extract_since}")
    
    return run_extraction(
        output_dir=args.output,
        extract_since=extract_since,
        full_extract=args.full
    )

if __name__ == "__main__":
    sys.exit(main())
