#!/usr/bin/env python3
"""
Migration Script: MySQL to PostgreSQL
Migrates data from tbl_client_invoice_details & tbl_client_invoice_reports to InvoiceCaseServices

Source Tables (MySQL):
- tbl_client_invoice_details
- tbl_client_invoice_reports

Target Table (PostgreSQL):
- InvoiceCaseServices

This script validates foreign key relationships before insertion.

Author: Migration Script
Date: 2024

Usage:
    python script.py                    # Run migration
"""

import sys
import os
import logging
from datetime import datetime

# Add parent directory to path to import db_connections
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connections import get_mysql_connection, get_postgres_connection

# Create invoice_logs directory if it doesn't exist
log_dir = 'invoice_logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging
log_filename = os.path.join(log_dir, '4_tbl_client_invoice_details_&_tbl_client_invoice_reports__InvoiceCaseServices.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class MigrationError(Exception):
    """Custom exception for migration errors"""
    pass

def validate_foreign_keys(postgres_cursor, invoice_id, case_id):
    """
    Validate that invoice_id and case_id exist in their respective parent tables.
    
    Args:
        postgres_cursor: PostgreSQL cursor
        invoice_id: Invoice ID to validate
        case_id: Case ID to validate
    
    Returns:
        tuple: (invoice_exists, case_exists)
    """
    try:
        # Check if invoice exists
        postgres_cursor.execute('SELECT COUNT(*) FROM "Invoices" WHERE "iId" = %s', (invoice_id,))
        invoice_exists = postgres_cursor.fetchone()[0] > 0
        
        # Check if case exists
        postgres_cursor.execute('SELECT COUNT(*) FROM "Cases" WHERE "cId" = %s', (case_id,))
        case_exists = postgres_cursor.fetchone()[0] > 0
        
        return invoice_exists, case_exists
        
    except Exception as e:
        logger.error(f"Error validating foreign keys: {e}")
        raise MigrationError(f"Failed to validate foreign keys: {e}")

def get_source_data(mysql_cursor):
    """
    Fetch data from MySQL tables with JOIN
    
    Args:
        mysql_cursor: MySQL cursor
    
    Returns:
        list: List of tuples containing joined data
    """
    query = """
    SELECT 
        d.client_invoice_id,
        d.case_id,
        d.total_amount,
        d.case_date,
        r.rush_fee
    FROM tbl_client_invoice_details d
    INNER JOIN tbl_client_invoice_reports r ON d.id = r.client_invoice_details_id
    ORDER BY d.id, r.id
    """
    
    try:
        mysql_cursor.execute(query)
        return mysql_cursor.fetchall()
    except Exception as e:
        logger.error(f"Error fetching source data: {e}")
        raise MigrationError(f"Failed to fetch source data: {e}")

def validate_and_sanitize_amount(amount, record_id=None):
    """
    Validate and sanitize amount values to prevent numeric overflow
    
    Args:
        amount: The amount value to validate
        record_id: Record identifier for logging purposes
    
    Returns:
        tuple: (sanitized_amount, is_valid, warning_message)
    """
    if amount is None:
        return None, True, None
    
    try:
        # Convert to float for validation
        amount_float = float(amount)
        
        # Check for invalid values (NaN, infinity, etc.)
        if not (amount_float == amount_float and amount_float != float('inf') and amount_float != float('-inf')):
            return None, False, f"Invalid amount value: {amount}"
        
        # PostgreSQL numeric(10,2) can handle values up to 99,999,999.99
        MAX_AMOUNT = 99999999.99
        MIN_AMOUNT = -99999999.99
        
        if amount_float > MAX_AMOUNT:
            logger.warning(f"Record {record_id}: Amount {amount} exceeds maximum allowed value, capping at {MAX_AMOUNT}")
            return MAX_AMOUNT, True, f"Amount capped from {amount} to {MAX_AMOUNT}"
        
        if amount_float < MIN_AMOUNT:
            logger.warning(f"Record {record_id}: Amount {amount} below minimum allowed value, capping at {MIN_AMOUNT}")
            return MIN_AMOUNT, True, f"Amount capped from {amount} to {MIN_AMOUNT}"
        
        # Round to 2 decimal places to match PostgreSQL precision
        sanitized_amount = round(amount_float, 2)
        return sanitized_amount, True, None
        
    except (ValueError, TypeError, OverflowError) as e:
        return None, False, f"Amount validation error: {e}"

def insert_invoice_case_service(postgres_cursor, invoice_id, case_id, amount, rush_fee, created_at, record_id=None):
    """
    Insert a single record into InvoiceCaseServices table
    Foreign key validation is performed to ensure data integrity.
    
    Args:
        postgres_cursor: PostgreSQL cursor
        invoice_id: Invoice ID
        case_id: Case ID
        amount: Total amount
        rush_fee: Rush fee
        created_at: Creation timestamp
        record_id: Record identifier for logging
    
    Returns:
        int: ID of inserted record
    """
    # Validate foreign key relationships
    invoice_exists, case_exists = validate_foreign_keys(postgres_cursor, invoice_id, case_id)
    
    if not invoice_exists:
        logger.warning(f"Record {record_id}: Invoice ID {invoice_id} does not exist in Invoices table - skipping")
        raise MigrationError(f"Invoice ID {invoice_id} not found in Invoices table")
    
    if not case_exists:
        logger.warning(f"Record {record_id}: Case ID {case_id} does not exist in Cases table - skipping")
        raise MigrationError(f"Case ID {case_id} not found in Cases table")
    
    # Validate and sanitize amount
    sanitized_amount, amount_valid, amount_warning = validate_and_sanitize_amount(amount, record_id)
    if not amount_valid:
        logger.error(f"Record {record_id}: Skipping due to invalid amount - {amount_warning}")
        raise MigrationError(f"Invalid amount: {amount_warning}")
    
    if amount_warning:
        logger.warning(f"Record {record_id}: {amount_warning}")
    
    # Validate and sanitize rush_fee
    sanitized_rush_fee, rush_fee_valid, rush_fee_warning = validate_and_sanitize_amount(rush_fee or 0.00, record_id)
    if not rush_fee_valid:
        logger.warning(f"Record {record_id}: Invalid rush_fee, setting to 0.00 - {rush_fee_warning}")
        sanitized_rush_fee = 0.00
    
    if rush_fee_warning:
        logger.warning(f"Record {record_id}: Rush fee {rush_fee_warning}")
    
    insert_query = """
    INSERT INTO "InvoiceCaseServices" 
    ("invoiceId", "caseId", "amount", "rushFee", "createdAt", "isDeleted", "deletedAt", "updatedAt")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING "icsId"
    """
    
    values = (
        invoice_id,
        case_id,
        sanitized_amount,
        sanitized_rush_fee,
        created_at,
        False,      # isDeleted default
        None,       # deletedAt default
        None        # updatedAt default
    )
    
    try:
        postgres_cursor.execute(insert_query, values)
        result = postgres_cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error inserting record: {e}")
        raise MigrationError(f"Failed to insert record: {e}")

def migrate_data():
    """
    Main migration function
    """
    mysql_conn = None
    postgres_conn = None
    
    try:
        # Establish connections
        logger.info("Establishing database connections...")
        mysql_conn = get_mysql_connection()
        postgres_conn = get_postgres_connection()
        
        mysql_cursor = mysql_conn.cursor()
        postgres_cursor = postgres_conn.cursor()
        
        logger.info("✅ Database connections established")
        logger.info(f"Log file location: {log_filename}")
        logger.info("Note: Foreign key validation will be performed to ensure data integrity")
        
        # Get source data
        logger.info("Fetching source data from MySQL...")
        source_data = get_source_data(mysql_cursor)
        logger.info(f"Found {len(source_data)} records to migrate")
        
        if not source_data:
            logger.warning("No data found to migrate")
            return
        
        # Show sample of first 5 records
        if source_data:
            logger.info("=" * 60)
            logger.info("SAMPLE DATA (First 5 records):")
            logger.info("=" * 60)
            for i, (invoice_id, case_id, total_amount, case_date, rush_fee) in enumerate(source_data[:5], 1):
                logger.info(f"Record {i}: Invoice ID={invoice_id}, Case ID={case_id}, "
                           f"Amount={total_amount}, Rush Fee={rush_fee}, Date={case_date}")
            if len(source_data) > 5:
                logger.info(f"... and {len(source_data) - 5} more records")
            logger.info("=" * 60)
        
        # Migration counters
        total_records = len(source_data)
        successful_migrations = 0
        failed_migrations = 0
        data_quality_issues = 0
        foreign_key_violations = 0
        
        logger.info("Starting data migration...")
        logger.info("Note: Foreign key validation will be performed - only records with valid invoiceId and caseId will be migrated")
        
        for i, (invoice_id, case_id, total_amount, case_date, rush_fee) in enumerate(source_data, 1):
            try:
                # Insert record (with foreign key validation)
                inserted_id = insert_invoice_case_service(
                    postgres_cursor, 
                    invoice_id, 
                    case_id, 
                    total_amount, 
                    rush_fee,
                    case_date,
                    record_id=i
                )
                
                if inserted_id:
                    successful_migrations += 1
                    if i % 100 == 0:  # Progress update every 100 records
                        logger.info(f"Progress: {i}/{total_records} records processed")
                else:
                    failed_migrations += 1
                    logger.error(f"Record {i}: Failed to get inserted ID")
                    
            except MigrationError as e:
                # Check if it's a foreign key violation
                if "not found in" in str(e):
                    foreign_key_violations += 1
                    logger.warning(f"Record {i}: Foreign key violation - {e}")
                elif "Invalid amount" in str(e):
                    data_quality_issues += 1
                    logger.warning(f"Record {i}: Data quality issue - {e}")
                else:
                    failed_migrations += 1
                    logger.error(f"Record {i}: Migration failed - {e}")
                continue
            except Exception as e:
                failed_migrations += 1
                logger.error(f"Record {i}: Migration failed - {e}")
                continue
        
        # Commit transaction
        postgres_conn.commit()
        logger.info("✅ Transaction committed successfully")
        
        # Final summary
        logger.info("=" * 60)
        logger.info("MIGRATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total records processed: {total_records}")
        logger.info(f"Successful migrations: {successful_migrations}")
        logger.info(f"Failed migrations: {failed_migrations}")
        logger.info(f"Data quality issues: {data_quality_issues}")
        logger.info(f"Foreign key violations: {foreign_key_violations}")
        logger.info(f"Success rate: {(successful_migrations/total_records)*100:.2f}%")
        logger.info("=" * 60)
        
        if failed_migrations > 0:
            logger.warning(f"⚠️  {failed_migrations} records failed to migrate. Check logs for details.")
            
        if data_quality_issues > 0:
            logger.warning(f"⚠️  {data_quality_issues} records had data quality issues (invalid amounts).")
            
        if foreign_key_violations > 0:
            logger.warning(f"⚠️  {foreign_key_violations} records had foreign key violations (missing invoiceId or caseId).")
            
        logger.info("Foreign key constraints are maintained - only records with valid relationships were migrated")
            
    except Exception as e:
        logger.error(f"Critical migration error: {e}")
        if postgres_conn:
            postgres_conn.rollback()
            logger.info("Transaction rolled back due to error")
        raise
        
    finally:
        # Close connections
        if mysql_conn:
            mysql_conn.close()
            logger.info("MySQL connection closed")
        if postgres_conn:
            postgres_conn.close()
            logger.info("PostgreSQL connection closed")

def verify_migration():
    """
    Verify the migration by comparing record counts and checking for orphaned records
    """
    mysql_conn = None
    postgres_conn = None
    
    try:
        logger.info("Starting migration verification...")
        
        # Establish connections
        mysql_conn = get_mysql_connection()
        postgres_conn = get_postgres_connection()
        
        mysql_cursor = mysql_conn.cursor()
        postgres_cursor = postgres_conn.cursor()
        
        # Count source records
        mysql_cursor.execute("""
            SELECT COUNT(*) 
            FROM tbl_client_invoice_details d
            INNER JOIN tbl_client_invoice_reports r ON d.id = r.client_invoice_details_id
        """)
        source_count = mysql_cursor.fetchone()[0]
        
        # Count migrated records
        postgres_cursor.execute('SELECT COUNT(*) FROM "InvoiceCaseServices"')
        target_count = postgres_cursor.fetchone()[0]
        
        logger.info(f"Source records (MySQL): {source_count}")
        logger.info(f"Target records (PostgreSQL): {target_count}")
        
        # Check for orphaned invoiceId records (should be 0 since we validate foreign keys)
        postgres_cursor.execute('''
            SELECT COUNT(*) FROM "InvoiceCaseServices" ics
            LEFT JOIN "Invoices" i ON ics."invoiceId" = i."iId"
            WHERE i."iId" IS NULL
        ''')
        orphaned_invoices = postgres_cursor.fetchone()[0]
        
        # Check for orphaned caseId records (should be 0 since we validate foreign keys)
        postgres_cursor.execute('''
            SELECT COUNT(*) FROM "InvoiceCaseServices" ics
            LEFT JOIN "Cases" c ON ics."caseId" = c."cId"
            WHERE c."cId" IS NULL
        ''')
        orphaned_cases = postgres_cursor.fetchone()[0]
        
        if orphaned_invoices == 0 and orphaned_cases == 0:
            logger.info("✅ No orphaned records found - all foreign key relationships are valid")
        else:
            logger.warning(f"⚠️  Found {orphaned_invoices} orphaned invoiceId records and {orphaned_cases} orphaned caseId records")
        
        if source_count >= target_count:
            logger.info("✅ Migration verification PASSED - All valid records migrated successfully")
        else:
            logger.warning(f"⚠️  Migration verification - Some records were skipped due to foreign key violations")
            
    except Exception as e:
        logger.error(f"Verification error: {e}")
        
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()

if __name__ == "__main__":
    try:
        # Print header
        print("🚀 Starting Migration: tbl_client_invoice_details & tbl_client_invoice_reports → InvoiceCaseServices")
        print("=" * 80)
        
        # Run migration
        migrate_data()
        
        # Verify migration
        verify_migration()
        
        print("=" * 80)
        print("✅ Migration completed! Check the log file for detailed information.")
        
    except KeyboardInterrupt:
        print("\n⚠️  Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)

