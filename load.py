"""
ETL Load Layer
Purpose: Load extracted CSV data from operational database into warehouse schema.
Includes validation, error handling, and transactional integrity.

Usage:
    python load.py --input ./extracted_data --warehouse_db warehouse_db
    python load.py --input ./extracted_data --mode incremental  # Incremental upsert
    python load.py --input ./extracted_data --mode full         # Full truncate + load
"""

import psycopg2
from psycopg2 import sql
import os
import sys
import logging
import argparse
import csv
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import json
import ast

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
        f"load_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

def connect_to_warehouse():
    """Establish connection to warehouse PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            host=os.getenv("WH_DB_HOST", "localhost"),
            port=int(os.getenv("WH_DB_PORT", 5432)),
            database=os.getenv("WH_DB_NAME", "warehouse"),
            user=os.getenv("WH_DB_USER"),
            password=os.getenv("WH_DB_PASSWORD")
        )
        logger.info("Connected to warehouse database")
        return connection
    except psycopg2.Error as e:
        logger.error(f"Warehouse connection failed: {e}")
        raise

def close_connection(connection):
    """Close database connection."""
    if connection:
        connection.close()
        logger.info("Warehouse connection closed")

# ============================================================
# DATA SANITIZATION
# ============================================================

def sanitize_row(row, jsonb_columns=None):
    """Sanitize row data for warehouse loading.
    
    - Convert empty strings to None for nullable columns
    - Convert Python dict format JSON to valid JSON
    """
    if jsonb_columns is None:
        jsonb_columns = []
    
    sanitized = {}
    
    for key, value in row.items():
        # Handle JSONB columns: convert Python dict format to JSON
        if key in jsonb_columns:
            if value and value.strip():
                try:
                    # Try to parse as Python dict literal first
                    parsed = ast.literal_eval(value)
                    # Convert to valid JSON string
                    sanitized[key] = json.dumps(parsed)
                except (ValueError, SyntaxError):
                    # If it's already valid JSON, keep it
                    try:
                        json.loads(value)
                        sanitized[key] = value
                    except:
                        # If parsing fails, set to None
                        sanitized[key] = None
            else:
                sanitized[key] = None
        else:
            # For other columns: convert empty strings to None
            sanitized[key] = None if (value == '' or value is None) else value
    
    return sanitized

def cleanup_staging_files(input_dir):
    """Delete staging CSV files after successful load."""
    try:
        csv_files = list(Path(input_dir).glob("*.csv"))
        for csv_file in csv_files:
            os.remove(csv_file)
            logger.info(f"  Deleted staging file: {csv_file.name}")
        logger.info(f"[OK] Cleaned up {len(csv_files)} staging files")
    except Exception as e:
        logger.warning(f"[WARNING] Could not cleanup staging files: {e}")

# ============================================================
# LOAD FUNCTIONS
# ============================================================

class CSVLoader:
    """Loads CSV files into warehouse tables."""
    
    def __init__(self, connection):
        self.connection = connection
        self.cursor = connection.cursor()
        self.total_rows_loaded = 0
        self.duplicate_count = 0
    
    def check_for_duplicates(self, table_name, primary_key_column, data_tuples, columns):
        """Check if any rows already exist in the table (deduplication)."""
        duplicates = []
        pk_index = columns.index(primary_key_column)
        
        for idx, row in enumerate(data_tuples):
            pk_value = row[pk_index]
            self.cursor.execute(
                f"SELECT 1 FROM {table_name} WHERE {primary_key_column} = %s LIMIT 1",
                (pk_value,)
            )
            if self.cursor.fetchone():
                duplicates.append(idx)
        
        return duplicates
    
    def load_csv_to_table(self, csv_file, table_name, truncate=False, primary_key=None):
        """Load CSV file into warehouse table."""
        try:
            if not os.path.exists(csv_file):
                logger.warning(f"CSV file not found: {csv_file}")
                return 0
            
            # Count rows in CSV
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            if not rows:
                logger.info(f"  {table_name}: 0 rows (empty file)")
                return 0
            
            # Truncate table if requested
            if truncate:
                self.cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
                logger.info(f"  Truncated {table_name}")
            
            # Get column names from CSV
            columns = list(rows[0].keys())
            
            # Sanitize rows (handle JSONB details column)
            jsonb_columns = ["details"] if "details" in columns else []
            sanitized_rows = [sanitize_row(row, jsonb_columns) for row in rows]
            
            # Convert rows to tuples (maintaining column order)
            data_tuples = [tuple(sanitized_rows[i][col] for col in columns) for i in range(len(sanitized_rows))]
            
            # Deduplicate within CSV if not truncating (and primary key provided)
            if not truncate and primary_key and primary_key in columns:
                seen_keys = set()
                unique_tuples = []
                duplicates_in_csv = 0
                pk_index = columns.index(primary_key)
                
                for row_tuple in data_tuples:
                    pk_value = row_tuple[pk_index]
                    if pk_value not in seen_keys:
                        seen_keys.add(pk_value)
                        unique_tuples.append(row_tuple)
                    else:
                        duplicates_in_csv += 1
                
                if duplicates_in_csv > 0:
                    logger.warning(f"  Removed {duplicates_in_csv} duplicate(s) within CSV for {table_name}")
                    self.duplicate_count += duplicates_in_csv
                    data_tuples = unique_tuples
            
            # Check for duplicates in database if not truncating (and primary key provided)
            duplicates = []
            if not truncate and primary_key and primary_key in columns:
                duplicates = self.check_for_duplicates(table_name, primary_key, data_tuples, columns)
                if duplicates:
                    logger.warning(f"  Found {len(duplicates)} duplicate(s) in warehouse {table_name} - skipping duplicates")
                    self.duplicate_count += len(duplicates)
                    # Remove duplicates from data_tuples
                    data_tuples = [t for i, t in enumerate(data_tuples) if i not in duplicates]
            
            if not data_tuples:
                logger.info(f"  {table_name}: 0 rows to load (all duplicates)")
                return 0
            
            # Prepare INSERT statement
            placeholders = ','.join(['%s'] * len(columns))
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            # Execute batch insert
            self.cursor.executemany(insert_query, data_tuples)
            self.connection.commit()
            
            logger.info(f"  {table_name}: {len(data_tuples)} rows loaded")
            self.total_rows_loaded += len(data_tuples)
            return len(data_tuples)
        
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"[ERROR] Failed to load {table_name}: {e}")
            raise
        except Exception as e:
            self.connection.rollback()
            logger.error(f"[ERROR] Unexpected error loading {table_name}: {e}")
            raise
    
    def upsert_csv_to_table(self, csv_file, table_name, key_columns, deduplicate_column=None):
        """Load CSV with upsert logic (INSERT OR UPDATE on conflict).
        
        Args:
            csv_file: Path to CSV file
            table_name: Target table name
            key_columns: Columns to use for ON CONFLICT clause (must match DB constraint)
            deduplicate_column: Optional column to deduplicate by first (e.g., 'ticker' for instruments)
                              Keeps the last occurrence of each value in this column.
        """
        try:
            if not os.path.exists(csv_file):
                logger.warning(f"CSV file not found: {csv_file}")
                return 0
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            if not rows:
                logger.info(f"  {table_name}: 0 rows (empty file)")
                return 0
            
            columns = list(rows[0].keys())
            
            # Sanitize rows (handle JSONB details column)
            jsonb_columns = ["details"] if "details" in columns else []
            sanitized_rows = [sanitize_row(row, jsonb_columns) for row in rows]
            
            # Convert rows to tuples (maintaining column order)
            data_tuples = [tuple(sanitized_rows[i][col] for col in columns) for i in range(len(sanitized_rows))]
            
            # First: Deduplicate by deduplicate_column if provided
            # (e.g., remove duplicate tickers, keeping only the last one)
            if deduplicate_column and deduplicate_column in columns:
                seen_dedup = {}
                dedup_indices = []
                dedup_col_idx = columns.index(deduplicate_column)
                
                for idx, row_tuple in enumerate(data_tuples):
                    dedup_value = row_tuple[dedup_col_idx]
                    if dedup_value in seen_dedup:
                        dedup_indices.append(seen_dedup[dedup_value])
                    seen_dedup[dedup_value] = idx
                
                duplicates_by_dedup = len(dedup_indices)
                if duplicates_by_dedup > 0:
                    logger.warning(f"  Removed {duplicates_by_dedup} duplicate(s) by {deduplicate_column} for {table_name}")
                    self.duplicate_count += duplicates_by_dedup
                    # Remove duplicates (keeping last occurrence)
                    data_tuples = [t for i, t in enumerate(data_tuples) if i not in dedup_indices]
            
            # Second: Deduplicate rows within the CSV by primary key
            seen_keys = set()
            unique_tuples = []
            duplicates_in_csv = 0
            
            for row_tuple in data_tuples:
                # Create key from key_columns
                key_indices = [columns.index(k) for k in key_columns]
                key = tuple(row_tuple[i] for i in key_indices)
                
                if key not in seen_keys:
                    seen_keys.add(key)
                    unique_tuples.append(row_tuple)
                else:
                    duplicates_in_csv += 1
            
            if duplicates_in_csv > 0:
                logger.warning(f"  Removed {duplicates_in_csv} duplicate(s) within CSV for {table_name}")
                self.duplicate_count += duplicates_in_csv
            
            if not unique_tuples:
                logger.info(f"  {table_name}: 0 rows to load (all duplicates)")
                return 0
            
            # Exclude extract_timestamp and key_columns from update
            update_columns = [c for c in columns if c not in key_columns and c != 'extract_timestamp']
            
            # Build UPSERT query
            placeholders = ','.join(['%s'] * len(columns))
            update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            
            upsert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT ({', '.join(key_columns)}) DO UPDATE SET {update_set}
            """
            
            self.cursor.executemany(upsert_query, unique_tuples)
            self.connection.commit()
            
            logger.info(f"  {table_name}: {len(unique_tuples)} rows upserted")
            self.total_rows_loaded += len(unique_tuples)
            return len(unique_tuples)
        
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"[ERROR] Failed to upsert {table_name}: {e}")
            raise

def load_dimensions(connection, input_dir, mode='incremental'):
    """Load dimension tables (no truncate needed for incremental)."""
    logger.info("")
    logger.info("-> Loading DIMENSIONS...")
    
    loader = CSVLoader(connection)
    
    # Clients dimension
    # NOTE: Using email as key column because it has a UNIQUE constraint
    # and is the natural business key for clients
    try:
        loader.upsert_csv_to_table(
            os.path.join(input_dir, "dim_clients.csv"),
            "dim_clients",
            key_columns=["email"]
        )
    except Exception as e:
        logger.error(f"Clients load failed: {e}")
    
    # Accounts dimension
    try:
        loader.upsert_csv_to_table(
            os.path.join(input_dir, "dim_accounts.csv"),
            "dim_accounts",
            key_columns=["account_id"]
        )
    except Exception as e:
        logger.error(f"Accounts load failed: {e}")
    
    # Instruments dimension
    # Use instrument_id as key for upsert, but deduplicate by ticker first
    # (ensures only one instrument_id per ticker in the extracted data)
    try:
        loader.upsert_csv_to_table(
            os.path.join(input_dir, "dim_instruments.csv"),
            "dim_instruments",
            key_columns=["instrument_id"],
            deduplicate_column="ticker"
        )
    except Exception as e:
        logger.error(f"Instruments load failed: {e}")
    
    return loader

def load_facts(connection, input_dir, mode='incremental'):
    """Load fact tables."""
    logger.info("")
    logger.info("-> Loading FACTS...")
    
    loader = CSVLoader(connection)
    
    # Determine truncate behavior
    truncate = (mode == 'full')
    
    # Orders fact
    try:
        if truncate:
            loader.load_csv_to_table(
                os.path.join(input_dir, "fact_orders.csv"),
                "fact_orders",
                truncate=True
            )
        else:
            loader.upsert_csv_to_table(
                os.path.join(input_dir, "fact_orders.csv"),
                "fact_orders",
                key_columns=["order_id"]
            )
    except Exception as e:
        logger.error(f"Orders load failed: {e}")
    
    # Transactions fact
    try:
        if truncate:
            loader.load_csv_to_table(
                os.path.join(input_dir, "fact_transactions.csv"),
                "fact_transactions",
                truncate=True
            )
        else:
            loader.upsert_csv_to_table(
                os.path.join(input_dir, "fact_transactions.csv"),
                "fact_transactions",
                key_columns=["transaction_id"]
            )
    except Exception as e:
        logger.error(f"Transactions load failed: {e}")
    
    # Audit logs fact
    try:
        if truncate:
            loader.load_csv_to_table(
                os.path.join(input_dir, "fact_audit_logs.csv"),
                "fact_audit_logs",
                truncate=True
            )
        else:
            loader.upsert_csv_to_table(
                os.path.join(input_dir, "fact_audit_logs.csv"),
                "fact_audit_logs",
                key_columns=["audit_log_id"]
            )
    except Exception as e:
        logger.error(f"Audit logs load failed: {e}")
    
    # Price history fact
    # NOTE: Using (instrument_id, timestamp) as key because there's a UNIQUE constraint
    # on that combination to prevent duplicate price records
    try:
        if truncate:
            loader.load_csv_to_table(
                os.path.join(input_dir, "fact_price_history.csv"),
                "fact_price_history",
                truncate=True
            )
        else:
            loader.upsert_csv_to_table(
                os.path.join(input_dir, "fact_price_history.csv"),
                "fact_price_history",
                key_columns=["instrument_id", "timestamp"]
            )
    except Exception as e:
        logger.error(f"Price history load failed: {e}")
    
    # Watchlists fact
    try:
        if truncate:
            loader.load_csv_to_table(
                os.path.join(input_dir, "fact_watchlists.csv"),
                "fact_watchlists",
                truncate=True
            )
        else:
            loader.upsert_csv_to_table(
                os.path.join(input_dir, "fact_watchlists.csv"),
                "fact_watchlists",
                key_columns=["watchlist_id"]
            )
    except Exception as e:
        logger.error(f"Watchlists load failed: {e}")
    
    # Historical snapshots fact
    # NOTE: Using (account_id, snapshot_date) as key because there's a UNIQUE constraint
    # on that combination to prevent duplicate snapshots per account per day
    try:
        if truncate:
            loader.load_csv_to_table(
                os.path.join(input_dir, "fact_historical_snapshots.csv"),
                "fact_historical_snapshots",
                truncate=True
            )
        else:
            loader.upsert_csv_to_table(
                os.path.join(input_dir, "fact_historical_snapshots.csv"),
                "fact_historical_snapshots",
                key_columns=["account_id", "snapshot_date"]
            )
    except Exception as e:
        logger.error(f"Historical snapshots load failed: {e}")
    
    return loader

def load_snapshots(connection, input_dir):
    """Load snapshot/current state tables."""
    logger.info("")
    logger.info("-> Loading SNAPSHOTS...")
    
    loader = CSVLoader(connection)
    
    # Holdings snapshot (replace current state)
    try:
        loader.load_csv_to_table(
            os.path.join(input_dir, "snapshot_holdings.csv"),
            "snapshot_holdings",
            truncate=True,  # Always truncate holdings to get current state
            primary_key="holding_id"
        )
    except Exception as e:
        logger.error(f"Holdings snapshot load failed: {e}")
    
    return loader

def validate_load(connection):
    """Validate warehouse tables have data."""
    logger.info("")
    logger.info("-> Validating load...")
    
    cursor = connection.cursor()
    tables = [
        "dim_clients", "dim_accounts", "dim_instruments",
        "fact_orders", "fact_transactions", "fact_audit_logs",
        "fact_price_history", "fact_watchlists", "fact_historical_snapshots",
        "snapshot_holdings"
    ]
    
    all_valid = True
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            if count == 0:
                logger.warning(f"  {table}: 0 rows (empty)")
            else:
                logger.info(f"  {table}: {count} rows")
        except psycopg2.Error as e:
            logger.error(f"  {table}: validation failed - {e}")
            all_valid = False
    
    cursor.close()
    return all_valid

# ============================================================
# MAIN ORCHESTRATION
# ============================================================

def run_load(input_dir, mode='incremental'):
    """Orchestrate full ETL load."""
    
    logger.info("=" * 60)
    logger.info("STARTING ETL LOAD")
    logger.info("=" * 60)
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Load mode: {mode}")
    logger.info("=" * 60)
    
    # Verify input directory exists
    if not os.path.isdir(input_dir):
        logger.error(f"Input directory not found: {input_dir}")
        return 1
    
    connection = None
    
    try:
        # Connect to warehouse
        connection = connect_to_warehouse()
        
        # Load dimensions
        dim_loader = load_dimensions(connection, input_dir, mode)
        
        # Load facts
        fact_loader = load_facts(connection, input_dir, mode)
        
        # Load snapshots
        snap_loader = load_snapshots(connection, input_dir)
        
        # Validate
        is_valid = validate_load(connection)
        
        # Calculate totals
        total_loaded = dim_loader.total_rows_loaded + fact_loader.total_rows_loaded + snap_loader.total_rows_loaded
        total_duplicates = dim_loader.duplicate_count + fact_loader.duplicate_count + snap_loader.duplicate_count
        
        logger.info("=" * 60)
        logger.info(f"Total rows loaded: {total_loaded}")
        if total_duplicates > 0:
            logger.warning(f"Total duplicates skipped: {total_duplicates}")
        if is_valid:
            logger.info("[OK] All validations passed")
        else:
            logger.warning("[WARNING] Some validations failed")
        logger.info("=" * 60)
        
        # Cleanup staging files
        logger.info("")
        logger.info("-> Cleaning up staging files...")
        cleanup_staging_files(input_dir)
        
        logger.info("=" * 60)
        logger.info("[OK] LOAD COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
        return 0
    
    except Exception as e:
        logger.error(f"[ERROR] LOAD FAILED: {e}", exc_info=True)
        return 1
    
    finally:
        if connection:
            close_connection(connection)

# ============================================================
# CLI INTERFACE
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="ETL Load: Load extracted data into warehouse database"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Input directory containing extracted CSV files"
    )
    parser.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help="Load mode: 'incremental' (upsert) or 'full' (truncate + load)"
    )
    
    args = parser.parse_args()
    
    # Validate input directory
    if not os.path.isdir(args.input):
        logger.error(f"Input directory does not exist: {args.input}")
        return 1
    
    return run_load(input_dir=args.input, mode=args.mode)

if __name__ == "__main__":
    sys.exit(main())
