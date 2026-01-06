#!/usr/bin/env python3
"""
MySQL to PostgreSQL RadiologistInvoices Data Migration Script

This script migrates data from MySQL tbl_radiologist_invoices table 
to PostgreSQL RadiologistInvoices table.
"""

import sys
import os
# Add parent directory to Python path to find db_connections module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import mysql.connector
import psycopg2
from datetime import datetime
import logging
from db_connections import get_mysql_connection, get_postgres_connection

# Configure logging
script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(script_dir, 'invoice_logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, '1_tbl_radiologist_invoices_&_tbl_radiologist_invoice_details__RadiologistInvoices.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def create_radiologist_mapping(postgres_cursor, mysql_cursor):
    """Create mapping from old radiologist IDs to new user IDs"""
    try:
        # Get all radiologist mappings from PostgreSQL Users table using oldUserId
        postgres_cursor.execute('SELECT "uId", "olduserid" FROM "Users" WHERE "userType" = \'RADIOLOGIST\' AND "olduserid" IS NOT NULL')
        postgres_results = postgres_cursor.fetchall()
        radiologist_mapping = {}
        for row in postgres_results:
            uid, old_user_id = row
            try:
                old_user_id_int = int(old_user_id) if old_user_id is not None else None
                if old_user_id_int is not None:
                    radiologist_mapping[old_user_id_int] = uid
            except (ValueError, TypeError):
                continue
        logger.info(f"Found {len(radiologist_mapping)} radiologist mappings in PostgreSQL")
        return radiologist_mapping
    except Exception as e:
        logger.error(f"Error creating radiologist mapping: {e}")
        return {}

def remove_riid_default(postgres_cursor, postgres_conn):
    """Remove the default/sequence from riId in RadiologistInvoices table so we can insert explicit values."""
    try:
        postgres_cursor.execute('''
            ALTER TABLE "RadiologistInvoices" ALTER COLUMN "riId" DROP DEFAULT
        ''')
        postgres_conn.commit()
        logger.info("Dropped default/sequence from RadiologistInvoices.riId to allow explicit inserts.")
    except Exception as e:
        logger.warning(f"Could not drop default/sequence from RadiologistInvoices.riId: {e}")

def drop_invoice_no_trigger(postgres_cursor):
    """Drop the trigger that auto-generates invoiceNo on RadiologistInvoices table if it exists."""
    try:
        # Find trigger name(s) for invoiceNo on RadiologistInvoices
        postgres_cursor.execute("""
            SELECT trigger_name
            FROM information_schema.triggers
            WHERE event_object_table = 'RadiologistInvoices'
        """)
        triggers = [row[0] for row in postgres_cursor.fetchall()]
        for trigger in triggers:
            try:
                postgres_cursor.execute(f'DROP TRIGGER IF EXISTS "{trigger}" ON "RadiologistInvoices";')
                logger.info(f"Dropped trigger: {trigger} on RadiologistInvoices")
            except Exception as e:
                logger.warning(f"Could not drop trigger {trigger}: {e}")
    except Exception as e:
        logger.warning(f"Error checking/dropping triggers: {e}")

def restore_riid_default(postgres_cursor, postgres_conn):
    """Restore the default/sequence for riId in RadiologistInvoices table after migration."""
    try:
        # Get the sequence name for riId
        postgres_cursor.execute("""
            SELECT pg_get_serial_sequence('"RadiologistInvoices"', 'riId')
        """)
        sequence_name = postgres_cursor.fetchone()[0]
        
        if sequence_name:
            # Set the default to use the sequence
            postgres_cursor.execute(f'''
                ALTER TABLE "RadiologistInvoices" ALTER COLUMN "riId" SET DEFAULT nextval('{sequence_name}')
            ''')
            postgres_conn.commit()
            logger.info(f"Restored default/sequence for RadiologistInvoices.riId using {sequence_name}")
        else:
            logger.warning("Could not find sequence for riId, skipping default restoration")
    except Exception as e:
        logger.warning(f"Could not restore default/sequence for RadiologistInvoices.riId: {e}")

def restore_invoice_no_trigger(postgres_cursor, postgres_conn):
    """Restore the trigger that auto-generates invoiceNo on RadiologistInvoices table after migration."""
    try:
        # Create a trigger function if it doesn't exist
        postgres_cursor.execute("""
            CREATE OR REPLACE FUNCTION generate_radiologist_invoice_no()
            RETURNS TRIGGER AS $$
            BEGIN
                IF NEW."invoiceNo" IS NULL OR NEW."invoiceNo" = '' THEN
                    NEW."invoiceNo" := 'RI-' || NEW."riId"::text;
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        # Create the trigger
        postgres_cursor.execute("""
            CREATE TRIGGER radiologist_invoice_no_trigger
            BEFORE INSERT ON "RadiologistInvoices"
            FOR EACH ROW
            EXECUTE FUNCTION generate_radiologist_invoice_no();
        """)
        
        postgres_conn.commit()
        logger.info("Restored invoiceNo trigger on RadiologistInvoices table")
    except Exception as e:
        logger.warning(f"Could not restore invoiceNo trigger: {e}")

def convert_month_to_number(month_str):
    """Convert month string to number"""
    if not month_str:
        return None
    
    month_str = str(month_str).strip().lower()
    
    # If it's already a number, return it
    try:
        month_num = int(month_str)
        if 1 <= month_num <= 12:
            return month_num
    except (ValueError, TypeError):
        pass
    
    # Convert month names to numbers
    month_mapping = {
        'january': 1, 'jan': 1,
        'february': 2, 'feb': 2,
        'march': 3, 'mar': 3,
        'april': 4, 'apr': 4,
        'may': 5,
        'june': 6, 'jun': 6,
        'july': 7, 'jul': 7,
        'august': 8, 'aug': 8,
        'september': 9, 'sep': 9, 'sept': 9,
        'october': 10, 'oct': 10,
        'november': 11, 'nov': 11,
        'december': 12, 'dec': 12
    }
    
    return month_mapping.get(month_str)

def validate_invoice_number(invoice_no):
    """Validate and potentially truncate invoice number"""
    if not invoice_no:
        return None
    
    invoice_str = str(invoice_no).strip()
    if len(invoice_str) > 30:
        logger.error(f"Invoice number too long (>{30} chars): {invoice_str}")
        raise ValueError(f"Invoice number exceeds 30 character limit: {invoice_str}")
    
    return invoice_str

def migrate_radiologist_invoices(mysql_cursor, postgres_cursor, radiologist_mapping):
    """Migrate radiologist invoices from MySQL to PostgreSQL, including riId from MySQL id. Allow NULL for unmapped radiologist IDs."""
    # Get MySQL data
    mysql_cursor.execute("""
        SELECT id, invoice_no, radiologist_id, revenue_amount, month, year, created_by
        FROM tbl_radiologist_invoices
        ORDER BY id
    """)
    mysql_records = mysql_cursor.fetchall()
    logger.info(f"Found {len(mysql_records)} radiologist invoices in MySQL")
    migrated_count = 0
    skipped_count = 0
    failed_count = 0
    invoice_id_mapping = {}
    for record in mysql_records:
        mysql_id, invoice_no, radiologist_id, revenue_amount, month, year, created_by = record
        try:
            # Map radiologist ID (allow NULL if not found)
            radiologist_user_id = None
            if radiologist_id:
                try:
                    radiologist_user_id = radiologist_mapping.get(int(radiologist_id))
                except Exception:
                    radiologist_user_id = None
            # Convert month and year
            month_number = convert_month_to_number(month)
            if month_number is None:
                logger.warning(f"Invalid month '{month}' for invoice {mysql_id}, skipping")
                skipped_count += 1
                continue
            try:
                year_number = int(year) if year else None
                if year_number is None:
                    logger.warning(f"Invalid year '{year}' for invoice {mysql_id}, skipping")
                    skipped_count += 1
                    continue
            except (ValueError, TypeError):
                logger.warning(f"Invalid year '{year}' for invoice {mysql_id}, skipping")
                skipped_count += 1
                continue
            # Validate invoice number
            try:
                validated_invoice_no = validate_invoice_number(invoice_no)
            except ValueError as e:
                logger.error(f"Invoice validation failed for {mysql_id}: {e}")
                failed_count += 1
                continue
            # Insert into PostgreSQL (include riId)
            postgres_cursor.execute("""
                INSERT INTO "RadiologistInvoices" (
                    "riId", "radioLogistUserId", "monthNumber", "yearNumber", 
                    "emailedStatus", "invoiceNo", "isDeleted"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING "riId"
            """, (
                mysql_id,
                radiologist_user_id,
                month_number,
                year_number,
                False,  # emailedStatus default to false
                validated_invoice_no,
                False   # isDeleted default to false
            ))
            new_id = postgres_cursor.fetchone()[0]
            invoice_id_mapping[mysql_id] = new_id
            migrated_count += 1
            if migrated_count % 50 == 0:
                logger.info(f"Migrated {migrated_count} radiologist invoices...")
        except Exception as e:
            logger.error(f"Failed to migrate invoice {mysql_id}: {e}")
            failed_count += 1
            continue
    logger.info(f"RadiologistInvoices migration completed:")
    logger.info(f"  Migrated: {migrated_count}")
    logger.info(f"  Skipped: {skipped_count}")
    logger.info(f"  Failed: {failed_count}")
    return invoice_id_mapping, migrated_count, skipped_count, failed_count

def verify_migration(mysql_cursor, postgres_cursor):
    """Verify the migration by comparing record counts and sampling data"""
    logger.info("Running verification...")
    
    # Count records
    mysql_cursor.execute("SELECT COUNT(*) FROM tbl_radiologist_invoices")
    mysql_count = mysql_cursor.fetchone()[0]
    
    postgres_cursor.execute('SELECT COUNT(*) FROM "RadiologistInvoices"')
    postgres_count = postgres_cursor.fetchone()[0]
    
    logger.info("=" * 60)
    logger.info("MIGRATION VERIFICATION")
    logger.info("=" * 60)
    logger.info(f"RadiologistInvoices:")
    logger.info(f"  MySQL records: {mysql_count}")
    logger.info(f"  PostgreSQL records: {postgres_count}")
    
    if mysql_count != postgres_count:
        logger.warning(f"  ⚠️  Record count mismatch. Difference: {abs(mysql_count - postgres_count)}")
    else:
        logger.info(f"  ✅ Record counts match!")
    
    # Sample migrated records
    postgres_cursor.execute("""
        SELECT "riId", "radioLogistUserId", "monthNumber", "yearNumber", "invoiceNo"
        FROM "RadiologistInvoices"
        ORDER BY "riId"
        LIMIT 5
    """)
    sample_records = postgres_cursor.fetchall()
    
    logger.info(f"\nSample migrated invoices:")
    for record in sample_records:
        logger.info(f"  riId: {record[0]}, User: {record[1]}, Month: {record[2]}, Year: {record[3]}, Invoice: {record[4]}")
    
    logger.info("=" * 60)

def main():
    """Main migration function"""
    mysql_conn = None
    postgres_conn = None
    try:
        logger.info("Starting RadiologistInvoices migration...")
        # Connect to databases
        logger.info("Connecting to MySQL database...")
        mysql_conn = get_mysql_connection()
        mysql_cursor = mysql_conn.cursor()
        logger.info("Successfully connected to MySQL database")
        logger.info("Connecting to PostgreSQL database...")
        postgres_conn = get_postgres_connection()
        postgres_cursor = postgres_conn.cursor()
        logger.info("Successfully connected to PostgreSQL database")
        # Remove default/sequence from riId so we can insert explicit values
        remove_riid_default(postgres_cursor, postgres_conn)
        # Drop invoiceNo trigger before migration
        drop_invoice_no_trigger(postgres_cursor)
        postgres_conn.commit()
        # Create mappings
        logger.info("Creating radiologist mapping...")
        radiologist_mapping = create_radiologist_mapping(postgres_cursor, mysql_cursor)
        # Migrate data
        logger.info("Starting radiologist invoices migration...")
        invoice_mapping, migrated, skipped, failed = migrate_radiologist_invoices(
            mysql_cursor, postgres_cursor, radiologist_mapping
        )
        # Commit transaction
        postgres_conn.commit()
        logger.info("Migration transaction committed")
        
        # Restore defaults and triggers after successful migration
        logger.info("Restoring defaults and triggers...")
        restore_riid_default(postgres_cursor, postgres_conn)
        restore_invoice_no_trigger(postgres_cursor, postgres_conn)
        postgres_conn.commit()
        logger.info("Defaults and triggers restored successfully")
        
        # Final summary
        logger.info("=" * 60)
        logger.info("MIGRATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"RadiologistInvoices:")
        logger.info(f"  Total records processed: {migrated + skipped + failed}")
        logger.info(f"  Successfully migrated: {migrated}")
        logger.info(f"  Skipped records: {skipped}")
        logger.info(f"  Failed records: {failed}")
        logger.info("=" * 60)
        # Run verification
        verify_migration(mysql_cursor, postgres_cursor)
        logger.info("Migration completed successfully!")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        if postgres_conn:
            postgres_conn.rollback()
            logger.info("Transaction rolled back due to error")
        raise
    finally:
        # Close connections
        if 'mysql_cursor' in locals():
            mysql_cursor.close()
        if mysql_conn:
            mysql_conn.close()
            logger.info("MySQL connection closed")
        if 'postgres_cursor' in locals():
            postgres_cursor.close()
        if postgres_conn:
            postgres_conn.close()
            logger.info("PostgreSQL connection closed")

if __name__ == "__main__":
    main() 