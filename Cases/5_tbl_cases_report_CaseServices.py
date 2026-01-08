#!/usr/bin/env python3
"""
MySQL to PostgreSQL CaseServices Data Migration Script

This script migrates data from MySQL tbl_cases_report table to PostgreSQL CaseServices table.
"""

import mysql.connector
import psycopg2
from datetime import datetime
import logging
from decimal import Decimal
import sys
import os

# Add parent directory to path to import db_connections
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connections import get_mysql_connection, get_postgres_connection

# Create cases_logs directory if it doesn't exist
log_dir = 'cases_logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging
log_filename = os.path.join(log_dir, '5_tbl_cases_report_CaseServices.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def connect_mysql():
    """Connect to MySQL database"""
    try:
        connection = get_mysql_connection()
        logger.info("Successfully connected to MySQL database")
        return connection
    except mysql.connector.Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        return None

def connect_postgres():
    """Connect to PostgreSQL database"""
    try:
        connection = get_postgres_connection()
        logger.info("Successfully connected to PostgreSQL database")
        return connection
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return None

def create_case_mapping(postgres_cursor, mysql_cursor):
    """Create mapping from old case IDs to new case IDs using voxelCaseId"""
    try:
        # Get all voxelCaseId to cId mappings from PostgreSQL
        postgres_cursor.execute('SELECT "cId", "voxelCaseId" FROM "Cases" WHERE "voxelCaseId" IS NOT NULL')
        postgres_results = postgres_cursor.fetchall()
        voxel_to_new_id = {}
        
        for row in postgres_results:
            cid, voxel_case_id = row
            # Ensure consistent data types (convert to int)
            try:
                voxel_case_id_int = int(voxel_case_id) if voxel_case_id is not None else None
                if voxel_case_id_int is not None:
                    voxel_to_new_id[voxel_case_id_int] = cid
            except (ValueError, TypeError):
                logger.warning(f"Invalid voxelCaseId value in PostgreSQL: {voxel_case_id}")
                continue
        
        logger.info(f"Found {len(voxel_to_new_id)} valid voxelCaseId mappings in PostgreSQL")
        
        # Get all cases_id to voxel_cases_id mappings from MySQL
        mysql_cursor.execute('SELECT cases_id, voxel_cases_id FROM tbl_cases WHERE voxel_cases_id IS NOT NULL')
        mysql_results = mysql_cursor.fetchall()
        old_to_voxel_id = {}
        
        for row in mysql_results:
            cases_id = row['cases_id']
            voxel_cases_id = row['voxel_cases_id']
            # Ensure consistent data types (convert to int)
            try:
                cases_id_int = int(cases_id) if cases_id is not None else None
                voxel_cases_id_int = int(voxel_cases_id) if voxel_cases_id is not None else None
                if cases_id_int is not None and voxel_cases_id_int is not None:
                    old_to_voxel_id[cases_id_int] = voxel_cases_id_int
            except (ValueError, TypeError):
                logger.warning(f"Invalid case ID values in MySQL: cases_id={cases_id}, voxel_cases_id={voxel_cases_id}")
                continue
        
        logger.info(f"Found {len(old_to_voxel_id)} valid case mappings in MySQL")
        
        # Create final mapping: old cases_id -> new cId
        case_mapping = {}
        missing_voxel_ids = []
        
        for old_id, voxel_id in old_to_voxel_id.items():
            if voxel_id in voxel_to_new_id:
                case_mapping[old_id] = voxel_to_new_id[voxel_id]
            else:
                missing_voxel_ids.append((old_id, voxel_id))
        
        logger.info(f"Created case ID mapping for {len(case_mapping)} cases")
        
        if missing_voxel_ids:
            logger.warning(f"Found {len(missing_voxel_ids)} cases with voxelCaseId not found in PostgreSQL:")
            for old_id, voxel_id in missing_voxel_ids[:10]:  # Show first 10
                logger.warning(f"  MySQL cases_id {old_id} -> voxelCaseId {voxel_id} (not found in PostgreSQL)")
            if len(missing_voxel_ids) > 10:
                logger.warning(f"  ... and {len(missing_voxel_ids) - 10} more")
        
        return case_mapping
    except Exception as e:
        logger.error(f"Error creating case mapping: {e}")
        return {}

def create_service_mapping(postgres_cursor, mysql_cursor):
    """Create mapping from old service IDs to new service IDs using service names"""
    try:
        # Get all title to sId mappings from PostgreSQL
        postgres_cursor.execute('SELECT "sId", "title" FROM "MasterServices" WHERE "isDeleted" = false')
        postgres_services = {row[1].lower().strip(): row[0] for row in postgres_cursor.fetchall()}
        
        logger.info(f"Found {len(postgres_services)} active services in PostgreSQL")
        
        # Get all services_id to services_name mappings from MySQL
        mysql_cursor.execute('SELECT services_id, services_name FROM tbl_add_services WHERE services_name IS NOT NULL AND services_name != \'\'')
        mysql_results = mysql_cursor.fetchall()
        mysql_services = {}
        
        for row in mysql_results:
            services_id = row['services_id']
            services_name = row['services_name']
            # Ensure consistent data types
            try:
                services_id_int = int(services_id) if services_id is not None else None
                if services_id_int is not None and services_name:
                    mysql_services[services_id_int] = services_name.strip()
            except (ValueError, TypeError):
                logger.warning(f"Invalid service ID value in MySQL: {services_id}")
                continue
        
        logger.info(f"Found {len(mysql_services)} valid services in MySQL")
        
        # Create final mapping: old service_id -> new sId
        service_mapping = {}
        unmatched_services = []
        
        for old_id, service_name in mysql_services.items():
            if service_name:
                # Try exact match first
                normalized_name = service_name.lower().strip()
                if normalized_name in postgres_services:
                    service_mapping[old_id] = postgres_services[normalized_name]
                else:
                    # Try fuzzy matching for common variations
                    matched_id = find_service_by_fuzzy_match(normalized_name, postgres_services)
                    if matched_id:
                        service_mapping[old_id] = matched_id
                    else:
                        unmatched_services.append((old_id, service_name))
        
        logger.info(f"Created service ID mapping for {len(service_mapping)} services")
        
        if unmatched_services:
            logger.warning(f"Found {len(unmatched_services)} services that could not be matched:")
            for old_id, service_name in unmatched_services[:10]:  # Show first 10
                logger.warning(f"  Service ID {old_id}: '{service_name}' (no match found)")
            if len(unmatched_services) > 10:
                logger.warning(f"  ... and {len(unmatched_services) - 10} more")
        
        return service_mapping
    except Exception as e:
        logger.error(f"Error creating service mapping: {e}")
        return {}

def find_service_by_fuzzy_match(service_name, postgres_services):
    """Find service by fuzzy matching for common variations"""
    # Common service name mappings
    name_mappings = {
        'radiology report': 'cone beam ct  interpretation -radiology report',
        'comparative scan radiology report add on': 'cone beam ct  interpretation -comparative scan radiology report add on',
        'panoramic radiograph interpretation': 'panoramic radiograph/ periapical/ bitewing radiograph interpretation report',
        # Addeed By URVIL
        'mri radiology report' : "mri radiology report",
        "diagnostic image portfolio" : "diagnostic image portfolio",
    }
    
    # Check direct mapping
    if service_name in name_mappings:
        mapped_name = name_mappings[service_name]
        if mapped_name in postgres_services:
            return postgres_services[mapped_name]
    
    # Check if service name contains key words from PostgreSQL services
    for pg_name, pg_id in postgres_services.items():
        # Check for partial matches (at least 3 words in common)
        service_words = set(service_name.split())
        pg_words = set(pg_name.split())
        common_words = service_words.intersection(pg_words)
        
        if len(common_words) >= 3:
            return pg_id
    
    return None

def get_service_id_by_name(services_name, postgres_cursor, service_name_cache):
    """Get service ID by service name, with caching and fuzzy matching"""
    if services_name in service_name_cache:
        return service_name_cache[services_name]
    
    try:
        normalized_name = services_name.lower().strip()
        
        # Try exact match first
        postgres_cursor.execute('SELECT "sId" FROM "MasterServices" WHERE LOWER("title") = %s AND "isDeleted" = false', (normalized_name,))
        result = postgres_cursor.fetchone()
        if result:
            service_id = result[0]
            service_name_cache[services_name] = service_id
            return service_id
        
        # Try fuzzy matching
        postgres_cursor.execute('SELECT "sId", "title" FROM "MasterServices" WHERE "isDeleted" = false')
        all_services = {row[1].lower().strip(): row[0] for row in postgres_cursor.fetchall()}
        
        matched_id = find_service_by_fuzzy_match(normalized_name, all_services)
        if matched_id:
            service_name_cache[services_name] = matched_id
            return matched_id
        
        logger.warning(f"Service not found by name: {services_name}")
        service_name_cache[services_name] = None
        return None
        
    except Exception as e:
        logger.error(f"Error getting service ID by name '{services_name}': {e}")
        service_name_cache[services_name] = None
        return None

def convert_datetime(mysql_datetime):
    """Convert MySQL datetime to PostgreSQL timestamp"""
    if mysql_datetime is None:
        return None
    if isinstance(mysql_datetime, str):
        try:
            return datetime.strptime(mysql_datetime, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
    return mysql_datetime

def should_skip_record(status):
    """Determine if record should be skipped based on status (equivalent to isDeleted)"""
    # Skip records where status indicates deletion
    # Adjust this logic based on your status values
    if status is None:
        return False
    
    # Common deletion indicators
    delete_indicators = [0, '0', 'deleted', 'inactive', 'removed']
    return status in delete_indicators

def debug_mapping_analysis():
    """Analyze case and service mappings to identify potential issues"""
    mysql_conn = None
    postgres_conn = None
    
    try:
        # Connect to databases
        mysql_conn = connect_mysql()
        postgres_conn = connect_postgres()
        
        if not mysql_conn or not postgres_conn:
            logger.error("Failed to establish database connections for debug analysis")
            return False
        
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        postgres_cursor = postgres_conn.cursor()
        
        logger.info("=" * 60)
        logger.info("DEBUG MAPPING ANALYSIS")
        logger.info("=" * 60)
        
        # Create mappings (same as in migration)
        case_mapping = create_case_mapping(postgres_cursor, mysql_cursor)
        service_mapping = create_service_mapping(postgres_cursor, mysql_cursor)
        
        # Analyze case IDs in tbl_cases_report
        mysql_cursor.execute("SELECT DISTINCT cases_id FROM tbl_cases_report ORDER BY cases_id")
        report_case_ids = [row['cases_id'] for row in mysql_cursor.fetchall()]
        
        missing_case_mappings = []
        for case_id in report_case_ids:
            try:
                case_id_int = int(case_id) if case_id is not None else None
                if case_id_int is not None and case_id_int not in case_mapping:
                    missing_case_mappings.append(case_id_int)
            except (ValueError, TypeError):
                missing_case_mappings.append(f"INVALID: {case_id}")
        
        logger.info(f"Case IDs analysis:")
        logger.info(f"  Total unique case IDs in tbl_cases_report: {len(report_case_ids)}")
        logger.info(f"  Case IDs that will be mapped: {len(report_case_ids) - len(missing_case_mappings)}")
        logger.info(f"  Case IDs that will be SKIPPED: {len(missing_case_mappings)}")
        
        if missing_case_mappings:
            logger.warning(f"Case IDs that will be skipped (first 20): {missing_case_mappings[:20]}")
            
            # Check how many records will be skipped
            placeholders = ','.join(['%s'] * min(len(missing_case_mappings), 100))  # Limit to avoid SQL issues
            query = f"SELECT COUNT(*) FROM tbl_cases_report WHERE cases_id IN ({placeholders})"
            mysql_cursor.execute(query, missing_case_mappings[:100])
            skipped_record_count = mysql_cursor.fetchone()['COUNT(*)']
            logger.warning(f"  Total records that will be skipped due to missing case mappings: {skipped_record_count}")
        
        # Analyze service IDs
        mysql_cursor.execute("SELECT DISTINCT add_services_id, services_name FROM tbl_cases_report WHERE add_services_id IS NOT NULL")
        report_services = mysql_cursor.fetchall()
        
        missing_service_mappings = []
        for row in report_services:
            service_id = row['add_services_id']
            service_name = row['services_name']
            try:
                service_id_int = int(service_id) if service_id is not None else None
                if service_id_int is not None and service_id_int not in service_mapping:
                    missing_service_mappings.append((service_id_int, service_name))
            except (ValueError, TypeError):
                missing_service_mappings.append((f"INVALID: {service_id}", service_name))
        
        logger.info(f"Service IDs analysis:")
        logger.info(f"  Total unique service IDs in tbl_cases_report: {len(report_services)}")
        logger.info(f"  Service IDs that will be mapped: {len(report_services) - len(missing_service_mappings)}")
        logger.info(f"  Service IDs that will be SKIPPED: {len(missing_service_mappings)}")
        
        if missing_service_mappings:
            logger.warning(f"Service IDs that will be skipped (first 10):")
            for service_id, service_name in missing_service_mappings[:10]:
                logger.warning(f"  Service ID {service_id}: '{service_name}'")
        
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"Debug analysis failed: {e}")
        return False
        
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()

def remove_caseservices_constraints(postgres_conn):
    """Remove all NOT NULL and foreign key constraints from CaseServices table."""
    try:
        cursor = postgres_conn.cursor()
        # Remove foreign key constraints
        fk_query = '''
            SELECT tc.constraint_name
            FROM information_schema.table_constraints tc
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_name = 'CaseServices'
        '''
        cursor.execute(fk_query)
        fks = cursor.fetchall()
        for (constraint,) in fks:
            try:
                drop_fk = f'ALTER TABLE "CaseServices" DROP CONSTRAINT IF EXISTS "{constraint}"'
                cursor.execute(drop_fk)
                logger.info(f"Dropped foreign key constraint: {constraint}")
            except Exception as e:
                logger.warning(f"Could not drop foreign key constraint {constraint}: {e}")
        # Remove NOT NULL constraints
        not_null_query = '''
            SELECT column_name
            FROM information_schema.columns
            WHERE is_nullable = 'NO'
              AND table_name = 'CaseServices'
              AND column_name NOT IN ('csId')
        '''
        cursor.execute(not_null_query)
        not_nulls = cursor.fetchall()
        for (column,) in not_nulls:
            try:
                alter_null = f'ALTER TABLE "CaseServices" ALTER COLUMN "{column}" DROP NOT NULL'
                cursor.execute(alter_null)
                logger.info(f"Dropped NOT NULL constraint: CaseServices.{column}")
            except Exception as e:
                logger.warning(f"Could not drop NOT NULL constraint on CaseServices.{column}: {e}")
        postgres_conn.commit()
        cursor.close()
        logger.info("All NOT NULL and foreign key constraints removed from CaseServices.")
    except Exception as e:
        logger.error(f"Failed to remove constraints from CaseServices: {e}")
        postgres_conn.rollback()
        raise

def get_valid_doctor_ids(mysql_conn):
    """Get all valid doctor IDs from tbl_users table"""
    try:
        cursor = mysql_conn.cursor()
        cursor.execute("""
            SELECT user_id 
            FROM tbl_users 
            WHERE user_id IS NOT NULL
            ORDER BY user_id
        """)
        valid_doctor_ids = set(row[0] for row in cursor.fetchall())
        logger.info(f"Found {len(valid_doctor_ids)} valid doctor IDs in tbl_users")
        if valid_doctor_ids:
            sample_ids = sorted(list(valid_doctor_ids))[:10]
            logger.info(f"Sample valid doctor IDs: {sample_ids}")
        cursor.close()
        return valid_doctor_ids
    except Exception as e:
        logger.error(f"Error fetching valid doctor IDs: {e}")
        return set()

def migrate_case_services():
    """Main migration function"""
    mysql_conn = None
    postgres_conn = None
    
    try:
        # Connect to databases
        mysql_conn = connect_mysql()
        postgres_conn = connect_postgres()
        
        if not mysql_conn or not postgres_conn:
            logger.error("Failed to establish database connections")
            return False
        
        # Fetch valid doctor IDs from tbl_users
        valid_doctor_ids = get_valid_doctor_ids(mysql_conn)
        if not valid_doctor_ids:
            logger.error("No valid doctor IDs found in tbl_users table!")
            return False
        
        # --- REMOVED: remove_caseservices_constraints and disabling triggers ---
        
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        postgres_cursor = postgres_conn.cursor()
        
        # Clear existing data
        logger.info("Clearing existing CaseServices data...")
        postgres_cursor.execute('DELETE FROM "CaseServices"')
        postgres_conn.commit()
        
        # Create ID mappings
        case_mapping = create_case_mapping(postgres_cursor, mysql_cursor)
        service_mapping = create_service_mapping(postgres_cursor, mysql_cursor)
        service_name_cache = {}
        
        # Fetch data from MySQL
        mysql_query = """
        SELECT 
            cases_report_id,
            cases_id,
            doctors_id,
            add_services_id,
            services_name,
            price,
            rush_fee,
            status,
            add_time,
            add_ip,
            update_time
        FROM tbl_cases_report
        ORDER BY cases_report_id
        """
        
        logger.info("Fetching data from MySQL...")
        mysql_cursor.execute(mysql_query)
        rows = mysql_cursor.fetchall()
        logger.info(f"Found {len(rows)} records to migrate")
        
        # Counters for tracking
        successful_migrations = 0
        skipped_records = 0
        failed_records = 0
        skipped_invalid_doctor_count = 0
        
        # Insert data into PostgreSQL
        for row in rows:
            try:
                # Skip records marked as deleted
                if should_skip_record(row['status']):
                    logger.debug(f"Skipping deleted record with ID {row['cases_report_id']}")
                    skipped_records += 1
                    continue
                
                # Check if doctors_id exists in tbl_users
                doctor_id = row.get('doctors_id')
                if doctor_id is None or doctor_id not in valid_doctor_ids:
                    skipped_invalid_doctor_count += 1
                    logger.warning(f"Skipping record {row['cases_report_id']}: doctors_id {doctor_id} not found in tbl_users")
                    continue
                
                # Map case ID - ensure consistent data type
                old_case_id = row['cases_id']
                try:
                    old_case_id_int = int(old_case_id) if old_case_id is not None else None
                except (ValueError, TypeError):
                    logger.warning(f"Invalid case ID format in record {row['cases_report_id']}: {old_case_id}")
                    skipped_records += 1
                    continue
                
                if old_case_id_int is None:
                    logger.warning(f"Null case ID in record {row['cases_report_id']}")
                    skipped_records += 1
                    continue
                
                new_case_id = case_mapping.get(old_case_id_int)
                if not new_case_id:
                    logger.warning(f"Case ID {old_case_id_int} not found in mapping, skipping record {row['cases_report_id']}")
                    skipped_records += 1
                    continue
                
                # Map service ID - try by old service ID first, then by name
                new_service_id = None
                old_service_id = row['add_services_id']
                
                # Ensure consistent data type for service ID
                try:
                    old_service_id_int = int(old_service_id) if old_service_id is not None else None
                except (ValueError, TypeError):
                    old_service_id_int = None
                
                if old_service_id_int and old_service_id_int in service_mapping:
                    new_service_id = service_mapping[old_service_id_int]
                elif row['services_name']:
                    new_service_id = get_service_id_by_name(row['services_name'], postgres_cursor, service_name_cache)
                
                if not new_service_id:
                    logger.warning(f"Service ID {old_service_id} or name '{row['services_name']}' not found, skipping record {row['cases_report_id']}")
                    skipped_records += 1
                    continue
                
                # Prepare data for insertion
                price = Decimal(str(row['price'])) if row['price'] is not None else Decimal('0')
                rush_fee = Decimal(str(row['rush_fee'])) if row['rush_fee'] is not None else Decimal('0')
                
                # Map directly: price -> amount, rush_fee -> rushFee
                amount = price
                rush_fee_amount = rush_fee
                
                # Ensure amount is greater than 0 (PostgreSQL constraint)
                if amount <= 0:
                    amount = Decimal('1.00')  # Minimum amount
                    logger.warning(f"Record {row['cases_report_id']}: Amount was <= 0, setting to minimum 1.00")
                
                # Set hasRush based on whether rush_fee > 0
                has_rush = rush_fee_amount > 0
                
                created_at = convert_datetime(row['add_time'])
                if created_at is None:
                    created_at = datetime.now()
                
                cs_id = row['cases_report_id']  # Map old PK to new PK
                # Insert into PostgreSQL (include csId)
                insert_query = """
                INSERT INTO "CaseServices" (
                    "csId",
                    "caseId",
                    "serviceId", 
                    "hasRush",
                    "amount",
                    "rushFee",
                    "createdAt"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
                postgres_cursor.execute(insert_query, (
                    cs_id,
                    new_case_id,
                    new_service_id,
                    has_rush,
                    amount,
                    rush_fee_amount,
                    created_at
                ))
                
                successful_migrations += 1
                
                if successful_migrations % 100 == 0:
                    logger.info(f"Migrated {successful_migrations} records...")
                    postgres_conn.commit()
                
            except psycopg2.IntegrityError as e:
                logger.error(f"Integrity error migrating record {row['cases_report_id']}: {e}")
                failed_records += 1
                postgres_conn.rollback()
                continue
            except Exception as e:
                logger.error(f"Error migrating record {row['cases_report_id']}: {e}")
                failed_records += 1
                postgres_conn.rollback()
                continue
        
        # Commit all changes
        postgres_conn.commit()
        
        # Log summary
        logger.info("=" * 50)
        logger.info("MIGRATION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total records processed: {len(rows)}")
        logger.info(f"Successfully migrated: {successful_migrations}")
        logger.info(f"Skipped records: {skipped_records}")
        logger.info(f"Skipped (invalid doctor_id): {skipped_invalid_doctor_count}")
        logger.info(f"Failed records: {failed_records}")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False
        
    finally:
        if mysql_conn:
            mysql_conn.close()
            logger.info("MySQL connection closed")
        if postgres_conn:
            postgres_conn.close()
            logger.info("PostgreSQL connection closed")

def verify_migration():
    """Verify the migration by comparing record counts and some sample data"""
    mysql_conn = None
    postgres_conn = None
    
    try:
        # Connect to databases
        mysql_conn = connect_mysql()
        postgres_conn = connect_postgres()
        
        if not mysql_conn or not postgres_conn:
            logger.error("Failed to establish database connections for verification")
            return False
        
        mysql_cursor = mysql_conn.cursor()
        postgres_cursor = postgres_conn.cursor()
        
        # Count records in MySQL (excluding deleted records)
        mysql_cursor.execute("SELECT COUNT(*) FROM tbl_cases_report WHERE status NOT IN (0, '0', 'deleted', 'inactive', 'removed')")
        mysql_count = mysql_cursor.fetchone()[0]
        
        # Count records in PostgreSQL
        postgres_cursor.execute('SELECT COUNT(*) FROM "CaseServices"')
        postgres_count = postgres_cursor.fetchone()[0]
        
        logger.info("=" * 50)
        logger.info("MIGRATION VERIFICATION")
        logger.info("=" * 50)
        logger.info(f"MySQL active records: {mysql_count}")
        logger.info(f"PostgreSQL records: {postgres_count}")
        
        if mysql_count == postgres_count:
            logger.info("✅ Record counts match!")
        else:
            logger.warning(f"⚠️  Record count mismatch. Difference: {abs(mysql_count - postgres_count)}")
        
        # Sample data verification
        postgres_cursor.execute('SELECT "caseId", "serviceId", "amount", "rushFee", "hasRush" FROM "CaseServices" LIMIT 5')
        sample_records = postgres_cursor.fetchall()
        
        logger.info("\nSample migrated records:")
        for record in sample_records:
            logger.info(f"CaseID: {record[0]}, ServiceID: {record[1]}, Amount: {record[2]}, RushFee: {record[3]}, HasRush: {record[4]}")
        
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        return False
        
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()

def update_master_services_table():
    """Update MasterServices table to set all isDeleted values to false"""
    postgres_conn = None
    
    try:
        # Connect to PostgreSQL
        postgres_conn = connect_postgres()
        
        if not postgres_conn:
            logger.error("Failed to establish PostgreSQL connection for MasterServices update")
            return False
        
        postgres_cursor = postgres_conn.cursor()
        
        # Check current state
        postgres_cursor.execute('SELECT COUNT(*) FROM "MasterServices" WHERE "isDeleted" = true')
        deleted_count = postgres_cursor.fetchone()[0]
        
        postgres_cursor.execute('SELECT COUNT(*) FROM "MasterServices"')
        total_count = postgres_cursor.fetchone()[0]
        
        logger.info(f"MasterServices table status - Total: {total_count}, Currently marked as deleted: {deleted_count}")
        
        if deleted_count > 0:
            # Update all isDeleted values to false
            logger.info("Updating MasterServices table to set all isDeleted values to false...")
            postgres_cursor.execute('UPDATE "MasterServices" SET "isDeleted" = false WHERE "isDeleted" = true')
            updated_rows = postgres_cursor.rowcount
            
            postgres_conn.commit()
            logger.info(f"Successfully updated {updated_rows} records in MasterServices table")
        else:
            logger.info("All MasterServices records already have isDeleted = false")
        
        # Verify the update
        postgres_cursor.execute('SELECT COUNT(*) FROM "MasterServices" WHERE "isDeleted" = false')
        active_count = postgres_cursor.fetchone()[0]
        
        postgres_cursor.execute('SELECT "sId", "title" FROM "MasterServices" WHERE "isDeleted" = false ORDER BY "sId"')
        active_services = postgres_cursor.fetchall()
        
        logger.info(f"After update - Active services count: {active_count}")
        logger.info("Available services for migration:")
        for service_id, title in active_services:
            logger.info(f"  ID {service_id}: {title}")
        
        postgres_cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"Error updating MasterServices table: {e}")
        return False
        
    finally:
        if postgres_conn:
            postgres_conn.close()

if __name__ == "__main__":
    logger.info("Starting CaseServices migration...")
    
    # First, update MasterServices table to set all isDeleted = false
    logger.info("Updating MasterServices table...")
    if not update_master_services_table():
        logger.error("Failed to update MasterServices table!")
        sys.exit(1)
    
    # Run debug analysis first
    logger.info("Running pre-migration debug analysis...")
    debug_mapping_analysis()
    
    if migrate_case_services():
        logger.info("Migration completed successfully!")
        
        # Run verification
        logger.info("Running verification...")
        verify_migration()
    else:
        logger.error("Migration failed!")
        sys.exit(1)
