#!/usr/bin/env python3
"""
Data Migration Script: tbl_cases_files (MySQL) -> CaseFiles (PostgreSQL)

This script migrates data from the MySQL table 'tbl_cases_files' to the PostgreSQL table 'CaseFiles'.
Includes comprehensive logging to both console and log files.
Foreign key validation is performed to ensure data integrity.
"""

import sys
import os
import logging
import colorama
from colorama import Fore, Style
from datetime import datetime

# Add parent directory to path to import db_connections
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connections import get_mysql_connection, get_postgres_connection

colorama.init(autoreset=True)

log_dir = 'cases_logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_filename = os.path.join(log_dir, '2_tbl_cases_files_new__CaseFiles.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def setup_logging():
    """
    Setup logging configuration for both file and console output
    """
    # Setup logging directory relative to script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    logs_dir = os.path.join(script_dir, "cases_logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(logs_dir, f"2_tbl_cases_files_new__CaseFiles.log")
    
    # Create logger
    logger = logging.getLogger('migration_logger')
    logger.setLevel(logging.DEBUG)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create file handler for detailed logs
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    
    # Create console handler for user-friendly output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger, log_filename

def log_colored_message(logger, level, message, color=None):
    """
    Log message with color to console and plain text to file
    """
    # Log to file (plain text)
    getattr(logger, level.lower())(message)
    
    # Print colored message to console
    if color:
        print(color + message)
    else:
        print(message)

def get_valid_cases(postgres_cursor):
    """
    Get all valid case IDs from PostgreSQL Cases table
    """
    try:
        postgres_cursor.execute('SELECT "cId" FROM "Cases"')
        valid_cases = set(row[0] for row in postgres_cursor.fetchall())
        return valid_cases
    except Exception as e:
        return set()

def get_case_creators(postgres_cursor):
    """
    Get mapping of case IDs to their doctorUserId from PostgreSQL Cases table
    """
    try:
        postgres_cursor.execute('SELECT "cId", "doctorUserId" FROM "Cases"')
        case_creators = {row[0]: row[1] for row in postgres_cursor.fetchall()}
        return case_creators
    except Exception as e:
        return {}

def get_doctor_id_mapping(mysql_cursor, postgres_cursor):
    """
    Create mapping from MySQL doctor_id to PostgreSQL Users.uId
    """
    try:
        # Get all doctor_ids from MySQL tbl_cases
        mysql_cursor.execute("SELECT DISTINCT doctor_id FROM tbl_cases WHERE doctor_id IS NOT NULL")
        mysql_doctor_ids = [row['doctor_id'] for row in mysql_cursor.fetchall()]
        
        # Create mapping: MySQL doctor_id -> PostgreSQL Users.uId
        doctor_mapping = {}
        
        for doctor_id in mysql_doctor_ids:
            # Look up in PostgreSQL Users table using olduserid
            postgres_cursor.execute('SELECT "uId" FROM "Users" WHERE "olduserid" = %s', (doctor_id,))
            result = postgres_cursor.fetchone()
            
            if result:
                doctor_mapping[doctor_id] = result[0]
                
        return doctor_mapping
        
    except Exception as e:
        logger.error(f"Error creating doctor ID mapping: {e}")
        return {}

def get_mysql_case_doctors(mysql_cursor):
    """
    Get mapping of cases_id to doctor_id from MySQL tbl_cases
    """
    try:
        mysql_cursor.execute("SELECT id, doctor_id FROM tbl_cases")
        case_doctors = {row['id']: row['doctor_id'] for row in mysql_cursor.fetchall()}
        return case_doctors
    except Exception as e:
        return {}

def get_valid_users(postgres_cursor):
    """
    Get all valid user IDs from PostgreSQL Users table
    """
    try:
        postgres_cursor.execute('SELECT "uId" FROM "Users"')
        valid_users = set(row[0] for row in postgres_cursor.fetchall())
        return valid_users
    except Exception as e:
        return set()

def get_default_doctor_id(postgres_cursor):
    """
    Get a valid default user ID from PostgreSQL Users table (for foreign key compatibility)
    """
    try:
        postgres_cursor.execute('SELECT "uId" FROM "Users" ORDER BY "uId" LIMIT 1')
        result = postgres_cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        return None

def remove_cfid_default(postgres_cursor):
    """Remove the default/sequence from cfId in CaseFiles table so we can insert explicit values."""
    try:
        postgres_cursor.execute('''
            ALTER TABLE "CaseFiles" ALTER COLUMN "cfId" DROP DEFAULT
        ''')
        logger.info("Dropped default/sequence from CaseFiles.cfId to allow explicit inserts.")
    except Exception as e:
        logger.warning(f"Could not drop default/sequence from CaseFiles.cfId: {e}")

def validate_foreign_keys(postgres_cursor, case_id, upload_by_user_id):
    """
    Validate that case_id and upload_by_user_id exist in their respective parent tables.
    
    Args:
        postgres_cursor: PostgreSQL cursor
        case_id: Case ID to validate
        upload_by_user_id: User ID to validate
    
    Returns:
        tuple: (case_exists, user_exists)
    """
    try:
        # Check if case exists
        postgres_cursor.execute('SELECT COUNT(*) FROM "Cases" WHERE "cId" = %s', (case_id,))
        case_exists = postgres_cursor.fetchone()[0] > 0
        
        # Check if user exists
        postgres_cursor.execute('SELECT COUNT(*) FROM "Users" WHERE "uId" = %s', (upload_by_user_id,))
        user_exists = postgres_cursor.fetchone()[0] > 0
        
        return case_exists, user_exists
        
    except Exception as e:
        logger.error(f"Error validating foreign keys: {e}")
        return False, False

def get_caseid_to_createdbyuser_mapping(postgres_cursor):
    """Build a mapping from caseId to createdByUserId from the Cases table."""
    mapping = {}
    try:
        postgres_cursor.execute('SELECT "cId", "createdByUserId" FROM "Cases"')
        for row in postgres_cursor.fetchall():
            mapping[row[0]] = row[1]
    except Exception as e:
        logger.warning(f"Could not build caseId to createdByUserId mapping: {e}")
    return mapping

def get_users_mapping(postgres_cursor):
    """
    Build a mapping from olduserid to uId from Users table.
    """
    mapping = {}
    postgres_cursor.execute('SELECT "uId", "olduserid" FROM "Users" WHERE "olduserid" IS NOT NULL')
    for row in postgres_cursor.fetchall():
        uId, olduserid = row
        mapping[olduserid] = uId
    logger.info(f"Built Users mapping with {len(mapping)} entries")
    return mapping

def get_source_data(mysql_cursor):
    """
    Fetch all data from tbl_cases_files_new (no add_time column)
    """
    query = """
    SELECT 
        cases_file_id, 
        cases_id, 
        filetitle, 
        filesize, 
        bucket_url, 
        uploaded_by,
        usertype
    FROM tbl_cases_files_new
    ORDER BY cases_file_id
    """
    mysql_cursor.execute(query)
    return mysql_cursor.fetchall()

def migrate_data():
    mysql_conn = None
    postgres_conn = None
    try:
        logger.info("Establishing database connections...")
        mysql_conn = get_mysql_connection()
        postgres_conn = get_postgres_connection()
        mysql_cursor = mysql_conn.cursor()
        postgres_cursor = postgres_conn.cursor()

        # Truncate the table and reset the sequence for cfId
        logger.info("Truncating CaseFiles and resetting cfId sequence to 1...")
        postgres_cursor.execute('TRUNCATE TABLE "CaseFiles" RESTART IDENTITY CASCADE')
        postgres_conn.commit()
        logger.info("Table truncated and sequence reset.")

        # Build Users mapping
        users_mapping = get_users_mapping(postgres_cursor)

        # Build set of valid user_ids from tbl_users (MySQL)
        mysql_cursor.execute('SELECT user_id FROM tbl_users')
        valid_user_ids_users = set(row[0] for row in mysql_cursor.fetchall())
        # Build set of valid user_ids from tbl_radiologist (MySQL)
        mysql_cursor.execute('SELECT radiologist_id FROM tbl_radiologist')
        valid_user_ids_radiologist = set(row[0] for row in mysql_cursor.fetchall())
        # Combine both sets
        valid_user_ids = valid_user_ids_users.union(valid_user_ids_radiologist)

        # Fetch source data
        logger.info("Fetching source data from tbl_cases_files_new...")
        source_data = get_source_data(mysql_cursor)
        logger.info(f"Found {len(source_data)} records to migrate")
        if not source_data:
            logger.warning("No data found to migrate")
            return

        # Migration counters
        total_records = len(source_data)
        successful_migrations = 0
        failed_migrations = 0
        skipped_client = 0
        foreign_key_violations = 0

        logger.info("Starting data migration...")
        logger.info("Note: Foreign key validation will be performed - only records with valid caseId and uploadByUserId will be migrated")
        
        for i, (cases_file_id, cases_id, filetitle, filesize, bucket_url, uploaded_by, usertype) in enumerate(source_data, 1):
            try:
                usertype_lower = (usertype or '').strip().lower()
                # For 'client', only migrate if uploaded_by exists in tbl_users or tbl_radiologist
                if usertype_lower == 'client' and uploaded_by not in valid_user_ids:
                    logger.warning(f"Skipping record {cases_file_id}: usertype=client and uploaded_by {uploaded_by} not found in tbl_users or tbl_radiologist")
                    skipped_client += 1
                    continue
                # For all, map uploaded_by to uId (uploadByUserId)
                upload_by_user_id = users_mapping.get(uploaded_by)
                if upload_by_user_id is None:
                    logger.warning(f"Skipping record {cases_file_id}: uploaded_by {uploaded_by} not found in Users mapping, uploadByUserId would be NULL")
                    failed_migrations += 1
                    continue
                
                # Validate foreign key relationships
                case_exists, user_exists = validate_foreign_keys(postgres_cursor, cases_id, upload_by_user_id)
                
                if not case_exists:
                    logger.warning(f"Record {cases_file_id}: Case ID {cases_id} does not exist in Cases table - skipping")
                    foreign_key_violations += 1
                    continue
                
                if not user_exists:
                    logger.warning(f"Record {cases_file_id}: User ID {upload_by_user_id} does not exist in Users table - skipping")
                    foreign_key_violations += 1
                    continue
                
                created_at = datetime.now()
                insert_query = """
                INSERT INTO "CaseFiles" (
                    "cfId", "caseId", "fileName", "fileSize", "objectKey", "uploadByUserId", "createdAt", "isDeleted", "deletedAt", "deletedByUserId"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    cases_file_id,  # cfId
                    cases_id,       # caseId
                    filetitle,      # fileName
                    int(filesize) if filesize is not None else 0,  # fileSize
                    bucket_url or '',  # objectKey
                    upload_by_user_id, # uploadByUserId (must not be None)
                    created_at,        # createdAt (now())
                    False,             # isDeleted
                    None,              # deletedAt
                    None               # deletedByUserId
                )
                postgres_cursor.execute(insert_query, values)
                successful_migrations += 1
                if successful_migrations % 100 == 0:
                    logger.info(f"Migrated {successful_migrations} records...")
                    postgres_conn.commit()
            except Exception as e:
                failed_migrations += 1
                logger.error(f"Record {cases_file_id}: Migration failed - {e}")
                postgres_conn.rollback()
                continue
        postgres_conn.commit()
        logger.info("✅ Transaction committed successfully")
        
        # Final summary
        logger.info("=" * 60)
        logger.info("MIGRATION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total records processed: {total_records}")
        logger.info(f"Successful migrations: {successful_migrations}")
        logger.info(f"Failed migrations: {failed_migrations}")
        logger.info(f"Skipped client records (user not found): {skipped_client}")
        logger.info(f"Foreign key violations: {foreign_key_violations}")
        logger.info(f"Success rate: {(successful_migrations/total_records)*100:.2f}%")
        logger.info("=" * 60)
        
        if failed_migrations > 0:
            logger.warning(f"⚠️  {failed_migrations} records failed to migrate. Check logs for details.")
            
        if foreign_key_violations > 0:
            logger.warning(f"⚠️  {foreign_key_violations} records had foreign key violations (missing caseId or uploadByUserId).")
            
        logger.info("Foreign key constraints are maintained - only records with valid relationships were migrated")
        
    except Exception as e:
        logger.error(f"Critical migration error: {e}")
        if postgres_conn:
            postgres_conn.rollback()
            logger.info("Transaction rolled back due to error")
        raise
    finally:
        if mysql_conn:
            mysql_conn.close()
            logger.info("MySQL connection closed")
        if postgres_conn:
            postgres_conn.close()
            logger.info("PostgreSQL connection closed")

if __name__ == "__main__":
    try:
        print("🚀 Starting Migration: tbl_cases_files_new → CaseFiles")
        print("=" * 80)
        migrate_data()
        print("=" * 80)
        print("✅ Migration completed! Check the log file for detailed information.")
    except KeyboardInterrupt:
        print("\n⚠️  Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)
