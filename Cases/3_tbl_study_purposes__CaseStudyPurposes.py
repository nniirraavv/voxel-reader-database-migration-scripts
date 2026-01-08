#!/usr/bin/env python3
"""
Migration script: tbl_study_purposes (MySQL) -> CaseStudyPurposes (PostgreSQL)
"""

import logging
import sys
import os
from datetime import datetime

# Add parent directory to path to import db_connections
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connections import get_mysql_connection, get_postgres_connection

# Create cases_logs directory if it doesn't exist
log_dir = 'cases_logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging
log_filename = os.path.join(log_dir, '3_tbl_study_purposes__CaseStudyPurposes.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def convert_tinyint_to_boolean(value):
    """Convert tinyint value to boolean"""
    if value is None:
        return False
    return bool(value)

def convert_blob_to_text(blob_data):
    """Convert blob data to text"""
    if blob_data is None:
        return None
    if isinstance(blob_data, bytes):
        return blob_data.decode('utf-8', errors='ignore')
    return str(blob_data)

def get_updated_at(update_time):
    """Get updatedAt value - use update_time if available, otherwise current time"""
    if update_time is not None:
        return update_time
    return datetime.now()

def check_duplicate_case_ids():
    """Check for duplicate case IDs in source table"""
    try:
        mysql_conn = get_mysql_connection()
        mysql_cursor = mysql_conn.cursor()
        
        mysql_cursor.execute("""
            SELECT cases_id, COUNT(*) as count 
            FROM tbl_study_purposes 
            WHERE cases_id IS NOT NULL
            GROUP BY cases_id 
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        """)
        
        duplicates = mysql_cursor.fetchall()
        
        if duplicates:
            logger.warning(f"Found {len(duplicates)} case IDs with duplicates:")
            for case_id, count in duplicates[:10]:  # Show first 10
                logger.warning(f"  Case ID {case_id}: {count} records")
            if len(duplicates) > 10:
                logger.warning(f"  ... and {len(duplicates) - 10} more")
        
        mysql_conn.close()
        return len(duplicates)
        
    except Exception as e:
        logger.error(f"Error checking duplicates: {str(e)}")
        return 0

def get_valid_doctor_ids(mysql_cursor):
    """Get all valid doctor IDs from tbl_users table"""
    try:
        mysql_cursor.execute("""
            SELECT user_id 
            FROM tbl_users 
            WHERE user_id IS NOT NULL
            ORDER BY user_id
        """)
        valid_doctor_ids = set(row[0] if isinstance(row, tuple) else row['user_id'] for row in mysql_cursor.fetchall())
        logger.info(f"Found {len(valid_doctor_ids)} valid doctor IDs in tbl_users")
        if valid_doctor_ids:
            sample_ids = sorted(list(valid_doctor_ids))[:10]
            logger.info(f"Sample valid doctor IDs: {sample_ids}")
        return valid_doctor_ids
    except Exception as e:
        logger.error(f"Error fetching valid doctor IDs: {e}")
        return set()

def migrate_study_purposes():
    """Main migration function"""
    mysql_conn = None
    postgres_conn = None
    
    try:
        # Check for duplicates first
        duplicate_count = check_duplicate_case_ids()
        if duplicate_count > 0:
            logger.info(f"Will handle {duplicate_count} duplicate case IDs during migration")
        # Establish connections
        logger.info("Establishing database connections...")
        mysql_conn = get_mysql_connection()
        postgres_conn = get_postgres_connection()
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        postgres_cursor = postgres_conn.cursor()
        # Get valid doctor IDs from tbl_users
        valid_doctor_ids = get_valid_doctor_ids(mysql_cursor)
        if not valid_doctor_ids:
            logger.error("No valid doctor IDs found in tbl_users table!")
            return 0, 0
        # Set autocommit to handle individual transactions
        postgres_conn.autocommit = True
        logger.info("Database connections established successfully")
        # Get total count for progress tracking
        mysql_cursor.execute("SELECT COUNT(*) as total FROM tbl_study_purposes")
        total_records = mysql_cursor.fetchone()['total']
        logger.info(f"Total records to migrate: {total_records}")
        # Fetch all records from source table
        mysql_cursor.execute("""
            SELECT 
                study_purposes_id,
                cases_id,
                doctor_id,
                airway,
                general,
                impaction,
                implant,
                orthodontic,
                pathology,
                sinus,
                pain,
                doctors_notes,
                cases_comments,
                status,
                add_ip,
                add_time,
                added_by,
                update_time,
                update_ip,
                updated_by
            FROM tbl_study_purposes
            ORDER BY study_purposes_id
        """)
        records = mysql_cursor.fetchall()
        logger.info(f"Fetched {len(records)} records from source table")
        # Prepare insert statement for target table (include cspId)
        insert_query = """
            INSERT INTO "CaseStudyPurposes" (
                "cspId",
                "caseId",
                "airwayFlag",
                "generalFlag", 
                "impactionFlag",
                "implantFlag",
                "orthodonticFlag",
                "pathologyFlag",
                "sinusFlag",
                "painFlag",
                "doctorsNotes",
                "caseComments",
                "updatedAt"
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # Process and insert records
        successful_migrations = 0
        failed_migrations = 0
        skipped_null_case_ids = 0
        skipped_invalid_doctor_count = 0
        for i, record in enumerate(records, 1):
            try:
                # Skip records with NULL case_id
                if record['cases_id'] is None:
                    skipped_null_case_ids += 1
                    logger.warning(f"Skipping record {record['study_purposes_id']} - NULL case_id")
                    continue
                # Skip if doctor_id is not in tbl_users
                doctor_id = record.get('doctor_id')
                if doctor_id is not None and doctor_id not in valid_doctor_ids:
                    skipped_invalid_doctor_count += 1
                    logger.warning(f"Skipping record {record['study_purposes_id']}: doctor_id {doctor_id} not found in tbl_users")
                    continue
                # Map fields according to the mapping table
                csp_id = int(record['study_purposes_id'])
                case_id = int(record['cases_id'])
                airway_flag = convert_tinyint_to_boolean(record['airway'])
                general_flag = convert_tinyint_to_boolean(record['general'])
                impaction_flag = convert_tinyint_to_boolean(record['impaction'])
                implant_flag = convert_tinyint_to_boolean(record['implant'])
                orthodontic_flag = convert_tinyint_to_boolean(record['orthodontic'])
                pathology_flag = convert_tinyint_to_boolean(record['pathology'])
                sinus_flag = convert_tinyint_to_boolean(record['sinus'])
                pain_flag = convert_tinyint_to_boolean(record['pain'])
                doctors_notes = convert_blob_to_text(record['doctors_notes'])
                case_comments = convert_blob_to_text(record['cases_comments'])
                updated_at = get_updated_at(record['update_time'])
                # Always try to insert, do not update on duplicate
                try:
                    postgres_cursor.execute(insert_query, (
                        csp_id,
                        case_id,
                        airway_flag,
                        general_flag,
                        impaction_flag,
                        implant_flag,
                        orthodontic_flag,
                        pathology_flag,
                        sinus_flag,
                        pain_flag,
                        doctors_notes,
                        case_comments,
                        updated_at
                    ))
                    successful_migrations += 1
                except Exception as insert_error:
                        failed_migrations += 1
                        logger.error(f"Failed to migrate record {record['study_purposes_id']}: {str(insert_error)}")
                # Log progress every 100 records
                if i % 100 == 0:
                    logger.info(f"Progress: {i}/{total_records} records processed (Success: {successful_migrations}, Failed: {failed_migrations})")
            except Exception as e:
                failed_migrations += 1
                logger.error(f"Failed to process record {record['study_purposes_id']}: {str(e)}")
                continue
        logger.info("Migration processing completed")
        # Log final statistics
        logger.info("=" * 60)
        logger.info("MIGRATION COMPLETED")
        logger.info(f"Total records processed: {total_records}")
        logger.info(f"Successful new insertions: {successful_migrations}")
        logger.info(f"Skipped (NULL case_id): {skipped_null_case_ids}")
        logger.info(f"Skipped (invalid doctor_id): {skipped_invalid_doctor_count}")
        logger.info(f"Failed migrations: {failed_migrations}")
        logger.info(f"Total successful operations: {successful_migrations}")
        logger.info(f"Success rate: {((successful_migrations)/(total_records - skipped_null_case_ids))*100:.2f}%")
        logger.info("=" * 60)
        return successful_migrations, failed_migrations
    except Exception as e:
        logger.error(f"Critical error during migration: {str(e)}")
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
    """Verify the migration by comparing record counts"""
    try:
        logger.info("Starting migration verification...")
        
        mysql_conn = get_mysql_connection()
        postgres_conn = get_postgres_connection()
        
        mysql_cursor = mysql_conn.cursor()
        postgres_cursor = postgres_conn.cursor()
        
        # Get count from source table (excluding NULL case_ids)
        mysql_cursor.execute("SELECT COUNT(*) FROM tbl_study_purposes WHERE cases_id IS NOT NULL")
        source_count = mysql_cursor.fetchone()[0]
        
        # Get unique case_ids from source
        mysql_cursor.execute("SELECT COUNT(DISTINCT cases_id) FROM tbl_study_purposes WHERE cases_id IS NOT NULL")
        unique_source_cases = mysql_cursor.fetchone()[0]
        
        # Get count from target table
        postgres_cursor.execute('SELECT COUNT(*) FROM "CaseStudyPurposes"')
        target_count = postgres_cursor.fetchone()[0]
        
        logger.info(f"Source table count (non-NULL case_id): {source_count}")
        logger.info(f"Source unique case_ids: {unique_source_cases}")
        logger.info(f"Target table count: {target_count}")
        
        if unique_source_cases == target_count:
            logger.info("✅ Verification PASSED: Unique case counts match")
        else:
            logger.warning(f"⚠️ Verification WARNING: Counts don't match (difference: {abs(unique_source_cases - target_count)})")
        
        mysql_conn.close()
        postgres_conn.close()
        
    except Exception as e:
        logger.error(f"Error during verification: {str(e)}")

if __name__ == "__main__":
    try:
        logger.info("Starting migration: tbl_study_purposes -> CaseStudyPurposes")
        logger.info("=" * 60)
        
        # Run migration
        successful, failed = migrate_study_purposes()
        
        # Verify migration
        verify_migration()
        
        logger.info("Migration process completed successfully")
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        sys.exit(1)
