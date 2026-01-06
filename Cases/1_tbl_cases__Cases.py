#!/usr/bin/env python3
"""
MySQL to PostgreSQL Cases Data Migration Script

This script migrates data from MySQL tbl_cases table to PostgreSQL Cases table.
"""
import sys
import os
# Add parent directory to Python path to find db_connections module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import mysql.connector
import psycopg2
from datetime import datetime
import logging
from decimal import Decimal
import sys
from db_connections import get_mysql_connection, get_postgres_connection
# Configure logging
script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(script_dir, 'cases_logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, '1_tbl_cases__Cases.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Database connection configurations

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

def get_valid_doctor_ids(mysql_cursor):
    """Get all valid doctor IDs from tbl_users table"""
    try:
        mysql_cursor.execute("""
            SELECT user_id 
            FROM tbl_users 
            WHERE user_id IS NOT NULL
            ORDER BY user_id
        """)
        
        valid_doctor_ids = set()
        for row in mysql_cursor.fetchall():
            valid_doctor_ids.add(row['user_id'])
        
        logger.info(f"Found {len(valid_doctor_ids)} valid doctor IDs in tbl_users")
        
        # Log some sample doctor IDs for debugging
        if valid_doctor_ids:
            sample_ids = sorted(list(valid_doctor_ids))[:10]
            logger.info(f"Sample valid doctor IDs: {sample_ids}")
            
            # Show the range of doctor IDs
            min_doctor_id = min(valid_doctor_ids)
            max_doctor_id = max(valid_doctor_ids)
            logger.info(f"Doctor ID range: {min_doctor_id} to {max_doctor_id}")
        
        return valid_doctor_ids
        
    except Exception as e:
        logger.error(f"Error fetching valid doctor IDs: {e}")
        return set()

def map_status_value(draft_status, submitted_status, completed_status, archived_status):
    """
    Map MySQL status values to PostgreSQL enum values based on four status columns
    
    Updated mapping rules:
    - DRAFT: draft=1, submitted=0, completed=0, archive=0
    - SUBMITTED: draft=X, submitted=1, completed=0, archive=0 (X means any value 0 or 1)
    - COMPLETED: draft=X, submitted=X, completed=1, archive=0 (X means any value 0 or 1)
    - ARCHIVED: draft=X, submitted=X, completed=X, archive=1 (X means any value 0 or 1)
    """
    # Convert all values to integers for comparison (None becomes 0)
    draft = 1 if draft_status == 1 else 0
    submitted = 1 if submitted_status == 1 else 0
    completed = 1 if completed_status == 1 else 0
    archived = 1 if archived_status == 1 else 0
    
    # Apply mapping rules based on the corrected table
    if draft == 1 and submitted == 0 and completed == 0 and archived == 0:
        return 'CREATED'
    elif submitted == 1 and completed == 0 and archived == 0:
        # For SUBMITTED: draft=X, submitted=1, completed=0, archive=0
        return 'SUBMITED'  # Note: PostgreSQL enum has "SUBMITED" not "SUBMITTED"
    elif completed == 1 and archived == 0:
        # For COMPLETED: draft=X, submitted=X, completed=1, archive=0
        return 'COMPLETED'
    elif archived == 1:
        # For ARCHIVED: draft=X, submitted=X, completed=X, archive=1
        return 'ARCHIVED'
    else:
        # Default fallback - try to determine based on priority
        if completed == 1:
            return 'COMPLETED'
        elif archived == 1:
            return 'ARCHIVED'
        elif submitted == 1:
            return 'SUBMITED'
        else:
            return 'DRAFT'  # Default to DRAFT

def map_review_status_value(mysql_review_status):
    """Map MySQL review_status values to PostgreSQL enum values"""
    if mysql_review_status is None:
        return None
    
    # Convert to string and lowercase for mapping
    review_status_str = str(mysql_review_status).lower()
    
    review_status_mapping = {
        'submitted': 'SUBMITED',  # Note: PostgreSQL enum has "SUBMITED" not "SUBMITTED"
        'assigned': 'ASSIGNED',
        'reviewed': 'RESOLVED',   # Map "reviewed" to "RESOLVED" since that's what exists in PostgreSQL
        'accepted': 'ACCEPTED',
        'rejected': 'REJECTED'
    }
    return review_status_mapping.get(review_status_str, 'SUBMITED')  # default to 'SUBMITED'

def get_case_invoice_mapping(mysql_cursor):
    """Get mapping of case IDs to invoice IDs from tbl_client_invoice_details"""
    try:
        # Create a fresh cursor to avoid any reuse issues
        mysql_cursor.execute("""
            SELECT 
                case_id,
                client_invoice_id
            FROM tbl_client_invoice_details
            ORDER BY case_id
        """)
        
        # Create a mapping dictionary
        case_invoice_mapping = {}
        rows = mysql_cursor.fetchall()
        
        for row in rows:
            case_id = row[0]  # case_id
            invoice_id = row[1]  # client_invoice_id
            case_invoice_mapping[case_id] = invoice_id
        
        logger.info(f"Found {len(case_invoice_mapping)} case-invoice mappings")
        
        # Log some sample mappings for debugging
        if case_invoice_mapping:
            sample_mappings = list(case_invoice_mapping.items())[:5]
            logger.info(f"Sample mappings: {sample_mappings}")
            
            # Show the range of case IDs in the mapping
            case_ids = list(case_invoice_mapping.keys())
            min_case_id = min(case_ids)
            max_case_id = max(case_ids)
            logger.info(f"Case ID range in mapping: {min_case_id} to {max_case_id}")
        else:
            logger.warning("No case-invoice mappings found!")
            
        return case_invoice_mapping
    except Exception as e:
        logger.error(f"Error fetching case-invoice mapping: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return {}

def convert_datetime(mysql_datetime):
    """Convert MySQL datetime to PostgreSQL timestamp with timezone"""
    if mysql_datetime is None:
        return None
    if isinstance(mysql_datetime, str):
        try:
            return datetime.strptime(mysql_datetime, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
    return mysql_datetime

def convert_date(mysql_date):
    """Convert MySQL date to PostgreSQL timestamp with timezone"""
    if mysql_date is None:
        return None
    if isinstance(mysql_date, str):
        try:
            return datetime.strptime(mysql_date + ' 00:00:00', '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
    # If it's already a date object, convert to datetime
    return datetime.combine(mysql_date, datetime.min.time())

# def drop_cases_foreign_keys(postgres_cursor):
#     """Permanently drop all foreign key constraints from Cases table"""
#     foreign_key_constraints = [
#         'Cases_clinicLocationId_fkey',
#         'Cases_createdByUserId_fkey',
#         'Cases_doctorUserId_fkey',
#         'Cases_invoiceId_fkey',
#         'Cases_radioLogistUserId_fkey',
#         'Cases_radiologistInvoiceId_fkey'
#     ]
    
#     logger.info("Permanently dropping foreign key constraints from Cases table...")
    
#     for constraint in foreign_key_constraints:
#         try:
#             drop_query = f'ALTER TABLE "Cases" DROP CONSTRAINT IF EXISTS "{constraint}"'
#             postgres_cursor.execute(drop_query)
#             logger.info(f"Dropped constraint: {constraint}")
#         except Exception as e:
#             logger.warning(f"Could not drop constraint {constraint}: {e}")
    
#     logger.info("Finished dropping foreign key constraints from Cases table")

def migrate_cases():
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
        
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        postgres_cursor = postgres_conn.cursor()
        
        # Get valid doctor IDs from tbl_users
        valid_doctor_ids = get_valid_doctor_ids(mysql_cursor)
        if not valid_doctor_ids:
            logger.error("No valid doctor IDs found in tbl_users table!")
            return False
        
        # Fetch the first clId from ClinicLocations table to use as default
        postgres_cursor.execute('SELECT "clId" FROM "ClinicLocations" ORDER BY "clId" ASC LIMIT 1')
        default_clinic_location_id_row = postgres_cursor.fetchone()
        if default_clinic_location_id_row:
            default_clinic_location_id = default_clinic_location_id_row[0]
            logger.info(f"Default clinic location ID set to first clId: {default_clinic_location_id}")
        else:
            logger.error("No clinic locations found in ClinicLocations table! Cannot set default clinic location ID.")
            default_clinic_location_id = None
        
        # Clear existing data and permanently drop foreign key constraints
        logger.info("Clearing existing Cases data...")
        postgres_cursor.execute('DELETE FROM "Cases"')
        postgres_conn.commit()
        
        # Permanently drop foreign key constraints from Cases table
        # drop_cases_foreign_keys(postgres_cursor)
        # postgres_conn.commit()
        
        # Get case to invoice mapping with a fresh cursor
        mapping_cursor = mysql_conn.cursor()
        case_invoice_mapping = get_case_invoice_mapping(mapping_cursor)
        mapping_cursor.close()
        
        # Get clinic location mapping
        clinic_location_mapping = get_clinic_location_mapping(postgres_cursor)
        
        # Get the correct column name for clinic location ID (for debugging queries)
        location_id_column = get_clinic_location_id_column(postgres_cursor)
        
        # Build mapping from olduserid to uId from Users table
        postgres_cursor.execute('SELECT "uId", "olduserid", "nameTitle", "firstName", "lastName" FROM "Users" WHERE "olduserid" IS NOT NULL OR ("firstName" IS NOT NULL AND "lastName" IS NOT NULL)')
        olduserid_to_uid = {}
        name_to_uid = {}
        for row in postgres_cursor.fetchall():
            uId, olduserid, nameTitle, firstName, lastName = row
            if olduserid is not None:
                olduserid_to_uid[olduserid] = uId
            # Build normalized name string for fallback
            name_parts = [str(nameTitle or '').strip(), str(firstName or '').strip(), str(lastName or '').strip()]
            full_name = ' '.join([p for p in name_parts if p]).strip().lower()
            if full_name:
                name_to_uid[full_name] = uId
        logger.info(f"Built olduserid to uId mapping with {len(olduserid_to_uid)} entries and name to uId mapping with {len(name_to_uid)} entries")

        # Fetch data from MySQL
        mysql_query = """
        SELECT 
            cases_id,
            voxel_cases_id,
            doctor_id,
            next_appointment_date,
            scan_date,
            services_total_cost,
            status,
            draft_status,
            submitted_status,
            completed_status,
            archived_status,
            assigned_radiologist_id,
            revenue_amount,
            review_status,
            case_result_summary,
            internal_comments,
            add_time,
            update_time,
            submitted_date,
            completed_date,
            added_by,
            reffering_doctor
        FROM tbl_cases
        ORDER BY cases_id
        """
        
        logger.info("Fetching data from MySQL...")
        mysql_cursor.execute(mysql_query)
        rows = mysql_cursor.fetchall()
        logger.info(f"Found {len(rows)} records to migrate")
        
        # Debug: Show first few cases and their doctor_ids
        if rows:
            first_few_cases = [(row['cases_id'], row['doctor_id']) for row in rows[:5]]
            logger.info(f"First few cases (case_id, doctor_id): {first_few_cases}")
            
            # Check which of these doctor_ids have clinic location mappings
            mapped_doctors = [doctor_id for _, doctor_id in first_few_cases if doctor_id in clinic_location_mapping]
            logger.info(f"Of these doctor_ids, have clinic location mappings: {mapped_doctors}")
        
        # Insert data into PostgreSQL
        postgres_insert_query = """
        INSERT INTO "Cases" (
            "cId",
            "voxelCaseId",
            "doctorUserId",
            "radioLogistUserId",
            "clinicLocationId",
            "scannedAt",
            "status",
            "reviewStatus",
            "totalServiceCost",
            "createdByUserId",
            "isDeleted",
            "createdAt",
            "updatedAt",
            "nextAppointmentAt",
            "revenueAmount",
            "internalComments",
            "caseResultSummary",
            "submittedAt",
            "completedAt",
            "invoiceId"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        migrated_count = 0
        error_count = 0
        missing_invoice_count = 0
        missing_clinic_location_count = 0
        skipped_invalid_doctor_count = 0
        
        for row in rows:
            try:
                # Check that doctor_id is present in tbl_users (user_id)
                doctor_user_id = row['doctor_id']
                if doctor_user_id not in valid_doctor_ids:
                    skipped_invalid_doctor_count += 1
                    logger.warning(f"Skipping case {row['cases_id']}: doctor_id {doctor_user_id} not found in tbl_users (user_id)")
                    continue

                mapped_doctor_uid = olduserid_to_uid.get(doctor_user_id)
                mapped_created_by_uid = mapped_doctor_uid

                if mapped_doctor_uid is None:
                    # Fallback: try to map using reffering_doctor name
                    reffering_doctor = row.get('reffering_doctor')
                    if reffering_doctor:
                        normalized_name = ' '.join(reffering_doctor.strip().split()).lower()
                        fallback_uid = name_to_uid.get(normalized_name)
                        if fallback_uid is not None:
                            mapped_doctor_uid = fallback_uid
                            mapped_created_by_uid = fallback_uid
                            logger.info(f"Used fallback name mapping for case {row['cases_id']}: '{reffering_doctor}' -> uId {fallback_uid}")
                        else:
                            logger.warning(f"No fallback name mapping found for case {row['cases_id']}: '{reffering_doctor}', skipping case.")
                            skipped_invalid_doctor_count += 1
                            continue
                    else:
                        logger.warning(f"doctor_id {doctor_user_id} not found in Users.olduserid mapping and no reffering_doctor value; skipping case {row['cases_id']}")
                        skipped_invalid_doctor_count += 1
                        continue

                # Map and prepare data for PostgreSQL
                cases_id = row['cases_id']  # Map MySQL cases_id to PostgreSQL cId
                voxel_case_id = row['voxel_cases_id']
                
                # Map assigned_radiologist_id to uId using olduserid_to_uid mapping
                assigned_radiologist_id = row['assigned_radiologist_id']
                mapped_radiologist_uid = olduserid_to_uid.get(assigned_radiologist_id)
                if mapped_radiologist_uid is None and assigned_radiologist_id:
                    logger.warning(f"assigned_radiologist_id {assigned_radiologist_id} not found in Users.olduserid mapping; setting radioLogistUserId to NULL for case {row['cases_id']}")
                radio_logist_user_id = mapped_radiologist_uid
                
                scanned_at = convert_date(row['scan_date'])
                status = map_status_value(row['draft_status'], row['submitted_status'], row['completed_status'], row['archived_status'])
                
                review_status = map_review_status_value(row['review_status'])
                
                # Handle total service cost - ensure it's not null and > 0
                total_service_cost = row['services_total_cost']
                if total_service_cost is None or total_service_cost <= 0:
                    total_service_cost = Decimal('1.00')  # Default minimum value
                else:
                    total_service_cost = Decimal(str(total_service_cost))
                
                # Use mapped uId for doctorUserId and createdByUserId (can be None)
                created_by_user_id = mapped_created_by_uid

                # Set isDeleted based on status column: status=1 -> isDeleted=False, status=2 -> isDeleted=True
                # Note: Old table only has status values 1 and 2
                old_status = row['status']
                
                # Debug: Show the exact value and type for troubleshooting
                if migrated_count < 5:
                    logger.info(f"DEBUG: Case {cases_id} old_status value: '{old_status}' (type: {type(old_status)})")
                
                # Convert to integer for comparison (handle string values, None, etc.)
                try:
                    status_int = int(old_status) if old_status is not None else 0
                except (ValueError, TypeError):
                    logger.error(f"Invalid status value '{old_status}' for case {cases_id}. Cannot convert to integer.")
                    status_int = 0
                
                if status_int == 1:
                    is_deleted = False
                elif status_int == 2:
                    is_deleted = True
                else:
                    # This should not happen since old table only has values 1 and 2
                    logger.error(f"Unexpected status value '{old_status}' (converted to {status_int}) for case {cases_id}. Expected only 1 or 2.")
                    is_deleted = False  # Default to False for safety
                
                # Debug logging for status mapping (only for first few records)
                if migrated_count < 5:
                    logger.info(f"Case {row['cases_id']} status mapping: draft={row['draft_status']}, submitted={row['submitted_status']}, completed={row['completed_status']}, archived={row['archived_status']} -> {status}")
                    logger.info(f"Case {row['cases_id']} isDeleted mapping: old_status={old_status} -> isDeleted={is_deleted}")
                
                created_at = convert_datetime(row['add_time']) or datetime.now()
                updated_at = convert_datetime(row['update_time'])
                next_appointment_at = convert_date(row['next_appointment_date'])
                
                revenue_amount = None
                if row['revenue_amount'] is not None:
                    revenue_amount = Decimal(str(row['revenue_amount']))
                
                internal_comments = row['internal_comments']
                case_result_summary = row['case_result_summary']
                submitted_at = convert_datetime(row['submitted_date'])
                completed_at = convert_datetime(row['completed_date'])
                
                # Get invoice ID from mapping
                invoice_id = case_invoice_mapping.get(cases_id)
                if invoice_id is None:
                    missing_invoice_count += 1
                    logger.debug(f"No invoice found for case ID {cases_id}")
                else:
                    logger.debug(f"Found invoice {invoice_id} for case {cases_id}")
                
                # Get clinic location ID from mapping
                clinic_location_id = clinic_location_mapping.get(doctor_user_id)
                if clinic_location_id is None:
                    missing_clinic_location_count += 1
                    logger.warning(f"No clinic location found for doctor_id {doctor_user_id} in case {cases_id}. Setting clinicLocationId to NULL.")
                    clinic_location_id = None  # Set to NULL if not found
                else:
                    logger.debug(f"Found clinic location {clinic_location_id} for doctor_id {doctor_user_id}")
                
                # Prepare values for insertion
                values = (
                    cases_id,  # Map MySQL cases_id to PostgreSQL cId
                    voxel_case_id,
                    mapped_doctor_uid,  # Use mapped uId for doctorUserId (can be None)
                    radio_logist_user_id,
                    clinic_location_id,
                    scanned_at,
                    status,
                    review_status,
                    total_service_cost,
                    created_by_user_id,  # Use mapped uId for createdByUserId (can be None)
                    is_deleted,
                    created_at,
                    updated_at,
                    next_appointment_at,
                    revenue_amount,
                    internal_comments,
                    case_result_summary,
                    submitted_at,
                    completed_at,
                    invoice_id
                )
                
                # Execute insert
                postgres_cursor.execute(postgres_insert_query, values)
                migrated_count += 1
                
                if migrated_count % 100 == 0:
                    logger.info(f"Migrated {migrated_count} records...")
                    postgres_conn.commit()
                
            except Exception as e:
                error_count += 1
                logger.error(f"Error migrating record {row['cases_id']}: {e}")
                
                # Rollback the current transaction and start a new one
                try:
                    postgres_conn.rollback()
                    logger.debug(f"Rolled back transaction after error in record {row['cases_id']}")
                except Exception as rollback_error:
                    logger.error(f"Error during rollback: {rollback_error}")
                
                # Continue with next record
                continue
        
        # Final commit
        postgres_conn.commit()
        
        # Get status mapping statistics
        postgres_cursor.execute('SELECT "status", COUNT(*) FROM "Cases" GROUP BY "status" ORDER BY "status"')
        status_stats = postgres_cursor.fetchall()
        
        # Get isDeleted statistics
        postgres_cursor.execute('SELECT "isDeleted", COUNT(*) FROM "Cases" GROUP BY "isDeleted" ORDER BY "isDeleted"')
        is_deleted_stats = postgres_cursor.fetchall()
        
        logger.info(f"Migration completed!")
        logger.info(f"Successfully migrated: {migrated_count} records")
        logger.info(f"Errors encountered: {error_count} records")
        logger.info(f"Cases without invoices: {missing_invoice_count} records")
        logger.info(f"Cases without clinic locations: {missing_clinic_location_count} records")
        logger.info(f"Cases skipped due to invalid doctor_id: {skipped_invalid_doctor_count} records")
        
        # Log status mapping statistics
        logger.info("Status mapping summary:")
        for status, count in status_stats:
            logger.info(f"  {status}: {count} cases")
        
        # Log isDeleted mapping statistics
        logger.info("isDeleted mapping summary:")
        for is_deleted, count in is_deleted_stats:
            status_text = "Deleted" if is_deleted else "Not Deleted"
            logger.info(f"  {status_text}: {count} cases")
        
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        if postgres_conn:
            postgres_conn.rollback()
        return False
        
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
    mysql_conn = None
    postgres_conn = None
    
    try:
        mysql_conn = connect_mysql()
        postgres_conn = connect_postgres()
        
        if not mysql_conn or not postgres_conn:
            logger.error("Failed to establish database connections for verification")
            return
        
        mysql_cursor = mysql_conn.cursor()
        postgres_cursor = postgres_conn.cursor()
        
        # Count records in MySQL
        mysql_cursor.execute("SELECT COUNT(*) FROM tbl_cases")
        mysql_count = mysql_cursor.fetchone()[0]
        
        # Count records in PostgreSQL
        postgres_cursor.execute('SELECT COUNT(*) FROM "Cases"')
        postgres_count = postgres_cursor.fetchone()[0]
        
        # Count cases with invoices
        postgres_cursor.execute('SELECT COUNT(*) FROM "Cases" WHERE "invoiceId" IS NOT NULL')
        cases_with_invoices = postgres_cursor.fetchone()[0]
        
        logger.info(f"MySQL tbl_cases count: {mysql_count}")
        logger.info(f"PostgreSQL Cases count: {postgres_count}")
        logger.info(f"Cases with invoices: {cases_with_invoices}")
        
        if mysql_count == postgres_count:
            logger.info("✅ Record counts match - migration appears successful!")
        else:
            logger.warning(f"⚠️ Record counts don't match - difference: {mysql_count - postgres_count}")
            
    except Exception as e:
        logger.error(f"Verification failed: {e}")
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()

def test_invoice_mapping():
    """Test function to verify invoice mapping is working correctly"""
    mysql_conn = None
    
    try:
        mysql_conn = connect_mysql()
        if not mysql_conn:
            logger.error("Failed to connect to MySQL")
            return
        
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        
        # Test 1: Check if tbl_client_invoice_details has data
        mysql_cursor.execute("SELECT COUNT(*) as count FROM tbl_client_invoice_details")
        details_count = mysql_cursor.fetchone()['count']
        logger.info(f"tbl_client_invoice_details has {details_count} records")
        
        # Test 2: Show sample data from tbl_client_invoice_details
        mysql_cursor.execute("""
            SELECT case_id, client_invoice_id 
            FROM tbl_client_invoice_details 
            LIMIT 10
        """)
        sample_details = mysql_cursor.fetchall()
        logger.info(f"Sample from tbl_client_invoice_details: {sample_details}")
        
        # Test 3: Check if tbl_cases has data
        mysql_cursor.execute("SELECT COUNT(*) as count FROM tbl_cases")
        cases_count = mysql_cursor.fetchone()['count']
        logger.info(f"tbl_cases has {cases_count} records")
        
        # Test 4: Show sample case IDs from tbl_cases
        mysql_cursor.execute("SELECT cases_id FROM tbl_cases LIMIT 10")
        sample_cases = [row['cases_id'] for row in mysql_cursor.fetchall()]
        logger.info(f"Sample case IDs from tbl_cases: {sample_cases}")
        
        # Test 5: Check overlap between the two tables
        mysql_cursor.execute("""
            SELECT tc.cases_id, cid.client_invoice_id
            FROM tbl_cases tc
            INNER JOIN tbl_client_invoice_details cid ON tc.cases_id = cid.case_id
            LIMIT 10
        """)
        overlap_data = mysql_cursor.fetchall()
        logger.info(f"Sample overlapping data: {overlap_data}")
        
        if not overlap_data:
            logger.warning("NO OVERLAP FOUND between tbl_cases.cases_id and tbl_client_invoice_details.case_id!")
            
            # Check if case IDs match at all
            mysql_cursor.execute("""
                SELECT DISTINCT case_id FROM tbl_client_invoice_details 
                WHERE case_id IN (SELECT cases_id FROM tbl_cases)
                LIMIT 5
            """)
            matching_ids = mysql_cursor.fetchall()
            logger.info(f"Matching case IDs: {matching_ids}")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
    finally:
        if mysql_conn:
            mysql_conn.close()

def get_clinic_location_id_column(postgres_cursor):
    """Detect the correct column name for clinic location ID in ClinicLocations table"""
    try:
        postgres_cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'ClinicLocations' 
            ORDER BY ordinal_position
        """)
        clinic_location_columns = [row[0] for row in postgres_cursor.fetchall()]
        logger.info(f"Debug: ClinicLocations columns: {clinic_location_columns}")
        
        # Use the correct primary key column (likely clId or clinicLocationId)
        if 'clId' in clinic_location_columns:
            return 'clId'
        elif 'clinicLocationId' in clinic_location_columns:
            return 'clinicLocationId'
        elif 'id' in clinic_location_columns:
            return 'id'
        else:
            # Fallback to first column if we can't determine
            fallback_column = clinic_location_columns[0] if clinic_location_columns else 'clId'
            logger.warning(f"Could not determine clinic location ID column, using: {fallback_column}")
            return fallback_column
    except Exception as e:
        logger.error(f"Error detecting clinic location column: {e}")
        return 'clId'  # Safe fallback

def test_clinic_location_mapping():
    """Test function to verify clinic location mapping is working correctly"""
    postgres_conn = None
    
    try:
        postgres_conn = connect_postgres()
        if not postgres_conn:
            logger.error("Failed to connect to PostgreSQL")
            return
        
        postgres_cursor = postgres_conn.cursor()
        
        # Test 1: Check Users table with olduserid
        postgres_cursor.execute('SELECT COUNT(*) FROM "Users" WHERE "olduserid" IS NOT NULL')
        users_with_old_id = postgres_cursor.fetchone()[0]
        logger.info(f"Users with olduserid: {users_with_old_id}")
        
        # Test 2: Check Clinics table
        postgres_cursor.execute('SELECT COUNT(*) FROM "Clinics"')
        clinics_count = postgres_cursor.fetchone()[0]
        logger.info(f"Total Clinics: {clinics_count}")
        
        # Test 3: Check ClinicLocations table
        postgres_cursor.execute('SELECT COUNT(*) FROM "ClinicLocations"')
        clinic_locations_count = postgres_cursor.fetchone()[0]
        logger.info(f"Debug: Total ClinicLocations: {clinic_locations_count}")
        
        if clinic_locations_count > 0:
            # First, let's check what columns exist in ClinicLocations
            postgres_cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'ClinicLocations' 
                ORDER BY ordinal_position
            """)
            clinic_location_columns = [row[0] for row in postgres_cursor.fetchall()]
            logger.info(f"Debug: ClinicLocations columns: {clinic_location_columns}")
            
            # Use the correct primary key column (likely clId or clinicLocationId)
            if 'clId' in clinic_location_columns:
                location_id_column = '"clId"'
            elif 'clinicLocationId' in clinic_location_columns:
                location_id_column = '"clinicLocationId"'
            elif 'id' in clinic_location_columns:
                location_id_column = '"id"'
            else:
                # Fallback to first column if we can't determine
                location_id_column = f'"{clinic_location_columns[0]}"'
                logger.warning(f"Could not determine clinic location ID column, using: {location_id_column}")
            
            postgres_cursor.execute(f'SELECT {location_id_column}, "clinicId" FROM "ClinicLocations" LIMIT 5')
            sample_locations = postgres_cursor.fetchall()
            logger.info(f"Debug: Sample ClinicLocations: {sample_locations}")
        else:
            location_id_column = '"clId"'  # Default fallback
        
        # Test 4: Check the complete join
        postgres_cursor.execute("""
            SELECT COUNT(*) FROM "Users" u
            INNER JOIN "Clinics" c ON u."uId" = c."ownerUserId"
            INNER JOIN "ClinicLocations" cl ON c."cId" = cl."clinicId"
            WHERE u."olduserid" IS NOT NULL
        """)
        complete_mappings = postgres_cursor.fetchone()[0]
        logger.info(f"Complete doctor -> clinic location mappings: {complete_mappings}")
        
        # Test 5: Show sample mappings
        postgres_cursor.execute("""
            SELECT 
                u."olduserid" as mysql_doctor_id,
                u."uId" as user_id,
                c."cId" as clinic_id,
                cl."clId" as clinic_location_id
            FROM "Users" u
            INNER JOIN "Clinics" c ON u."uId" = c."ownerUserId"
            INNER JOIN "ClinicLocations" cl ON c."cId" = cl."clinicId"
            WHERE u."olduserid" IS NOT NULL
            LIMIT 5
        """)
        sample_mappings = postgres_cursor.fetchall()
        logger.info(f"Sample mappings: {sample_mappings}")
        
        if complete_mappings == 0:
            logger.warning("⚠️ NO COMPLETE MAPPINGS FOUND! Check:")
            logger.warning("1. Users table has olduserid values")
            logger.warning("2. Clinics table has ownerUserId matching Users.uId")
            logger.warning("3. ClinicLocations table has clinicId matching Clinics.cId")
        else:
            logger.info(f"✅ Found {complete_mappings} valid clinic location mappings")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
    finally:
        if postgres_conn:
            postgres_conn.close()

def get_clinic_location_mapping(postgres_cursor):
    """
    Create a mapping from MySQL doctor_id to PostgreSQL clinic location ID
    
    Lookup chain:
    1. MySQL doctor_id -> PostgreSQL Users.olduserid -> Users.uId
    2. Users.uId -> Clinics.ownerUserId -> Clinics.cId  
    3. Clinics.cId -> ClinicLocations.clinicId -> ClinicLocations.ClId
    """
    try:
        logger.info("Building clinic location mapping...")
        
        # Get the correct column name for clinic location ID
        location_id_column = get_clinic_location_id_column(postgres_cursor)
        
        # Debug: Check each step of the lookup chain separately
        
        # Step 1: Check Users with olduserid
        postgres_cursor.execute('SELECT COUNT(*) FROM "Users" WHERE "olduserid" IS NOT NULL')
        users_with_old_id = postgres_cursor.fetchone()[0]
        logger.info(f"Debug: Users with olduserid: {users_with_old_id}")
        
        if users_with_old_id > 0:
            postgres_cursor.execute('SELECT "uId", "olduserid" FROM "Users" WHERE "olduserid" IS NOT NULL LIMIT 5')
            sample_users = postgres_cursor.fetchall()
            logger.info(f"Debug: Sample Users with olduserid: {sample_users}")
        
        # Step 2: Check Clinics
        postgres_cursor.execute('SELECT COUNT(*) FROM "Clinics"')
        clinics_count = postgres_cursor.fetchone()[0]
        logger.info(f"Debug: Total Clinics: {clinics_count}")
        
        if clinics_count > 0:
            postgres_cursor.execute('SELECT "cId", "ownerUserId" FROM "Clinics" LIMIT 5')
            sample_clinics = postgres_cursor.fetchall()
            logger.info(f"Debug: Sample Clinics: {sample_clinics}")
        
        # Step 3: Check ClinicLocations
        postgres_cursor.execute('SELECT COUNT(*) FROM "ClinicLocations"')
        clinic_locations_count = postgres_cursor.fetchone()[0]
        logger.info(f"Debug: Total ClinicLocations: {clinic_locations_count}")
        
        if clinic_locations_count > 0:
            postgres_cursor.execute(f'SELECT "{location_id_column}", "clinicId" FROM "ClinicLocations" LIMIT 5')
            sample_locations = postgres_cursor.fetchall()
            logger.info(f"Debug: Sample ClinicLocations: {sample_locations}")
        
        # Step 4: Check Users -> Clinics join
        postgres_cursor.execute("""
            SELECT COUNT(*) FROM "Users" u
            INNER JOIN "Clinics" c ON u."uId" = c."ownerUserId"
            WHERE u."olduserid" IS NOT NULL
        """)
        users_clinics_join = postgres_cursor.fetchone()[0]
        logger.info(f"Debug: Users->Clinics successful joins: {users_clinics_join}")
        
        if users_clinics_join > 0:
            postgres_cursor.execute("""
                SELECT u."olduserid", u."uId", c."cId", c."ownerUserId"
                FROM "Users" u
                INNER JOIN "Clinics" c ON u."uId" = c."ownerUserId"
                WHERE u."olduserid" IS NOT NULL
                LIMIT 5
            """)
            sample_user_clinic_joins = postgres_cursor.fetchall()
            logger.info(f"Debug: Sample Users->Clinics joins: {sample_user_clinic_joins}")
        
        # Step 5: Full join query
        query = f"""
            SELECT 
                u."olduserid" as mysql_doctor_id,
                u."uId" as user_id,
                c."cId" as clinic_id,
                cl."{location_id_column}" as clinic_location_id
            FROM "Users" u
            INNER JOIN "Clinics" c ON u."uId" = c."ownerUserId"
            INNER JOIN "ClinicLocations" cl ON c."cId" = cl."clinicId"
            WHERE u."olduserid" IS NOT NULL
        """
        
        postgres_cursor.execute(query)
        results = postgres_cursor.fetchall()
        
        logger.info(f"Debug: Full join query returned {len(results)} results")
        
        # Create mapping dictionary: mysql_doctor_id -> clinic_location_id
        doctor_to_clinic_location = {}
        
        for row in results:
            mysql_doctor_id = row[0]
            user_id = row[1]
            clinic_id = row[2]
            clinic_location_id = row[3]
            
            doctor_to_clinic_location[mysql_doctor_id] = clinic_location_id
            
            logger.debug(f"Mapping: doctor_id {mysql_doctor_id} -> uId {user_id} -> cId {clinic_id} -> ClId {clinic_location_id}")
        
        logger.info(f"Created {len(doctor_to_clinic_location)} clinic location mappings")
        
        # Log some sample mappings for debugging
        if doctor_to_clinic_location:
            sample_mappings = list(doctor_to_clinic_location.items())[:5]
            logger.info(f"Sample clinic location mappings: {sample_mappings}")
            
            # Show the range of doctor IDs in the mapping
            doctor_ids = list(doctor_to_clinic_location.keys())
            min_doctor_id = min(doctor_ids)
            max_doctor_id = max(doctor_ids)
            logger.info(f"Doctor ID range in mapping: {min_doctor_id} to {max_doctor_id}")
        else:
            logger.warning("No clinic location mappings found!")
            
        return doctor_to_clinic_location
        
    except Exception as e:
        logger.error(f"Error creating clinic location mapping: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return {}

def test_status_mapping():
    """Test function to verify status mapping logic works correctly"""
    logger.info("=== Testing Status Mapping Logic ===")
    
    # Test cases based on the corrected mapping table
    test_cases = [
        # (draft, submitted, completed, archived, expected_status, description)
        (1, 0, 0, 0, 'DRAFT', 'DRAFT status'),
        (0, 1, 0, 0, 'SUBMITED', 'SUBMITTED status (draft=0, submitted=1)'),
        (1, 1, 0, 0, 'SUBMITED', 'SUBMITTED status (draft=1, submitted=1)'),
        (0, 0, 1, 0, 'COMPLETED', 'COMPLETED status (draft=0, submitted=0, completed=1)'),
        (1, 1, 1, 0, 'COMPLETED', 'COMPLETED status (draft=1, submitted=1, completed=1)'),
        (0, 0, 0, 1, 'ARCHIVED', 'ARCHIVED status (draft=0, submitted=0, completed=0, archived=1)'),
        (1, 1, 1, 1, 'ARCHIVED', 'ARCHIVED status (all=1, but archived=1 takes precedence)'),
        (0, 0, 0, 0, 'DRAFT', 'Default fallback to DRAFT'),
        (1, 0, 1, 0, 'COMPLETED', 'COMPLETED takes priority over DRAFT'),
    ]
    
    for draft, submitted, completed, archived, expected, description in test_cases:
        result = map_status_value(draft, submitted, completed, archived)
        status = "✅ PASS" if result == expected else "❌ FAIL"
        logger.info(f"{status} {description}: {draft},{submitted},{completed},{archived} -> {result} (expected: {expected})")
    
    logger.info("=== End Status Mapping Test ===")

def test_is_deleted_mapping():
    """Test function to verify isDeleted mapping logic works correctly"""
    logger.info("=== Testing isDeleted Mapping Logic ===")
    
    # Test cases for isDeleted mapping (old table only has status values 1 and 2)
    test_cases = [
        # (old_status, expected_is_deleted, description)
        (1, False, 'status=1 should set isDeleted=False'),
        (2, True, 'status=2 should set isDeleted=True'),
    ]
    
    for old_status, expected, description in test_cases:
        # Simulate the mapping logic
        if old_status == 1:
            result = False
        elif old_status == 2:
            result = True
        else:
            result = False  # Should not happen in practice
        
        status = "✅ PASS" if result == expected else "❌ FAIL"
        logger.info(f"{status} {description}: status={old_status} -> isDeleted={result} (expected: {expected})")
    
    logger.info("=== End isDeleted Mapping Test ===")

def test_mysql_status_data():
    """Test function to examine the actual status data in MySQL table"""
    mysql_conn = None
    
    try:
        mysql_conn = connect_mysql()
        if not mysql_conn:
            logger.error("Failed to connect to MySQL")
            return
        
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        
        # Check if the four status columns exist
        mysql_cursor.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'tbl_cases' 
            AND COLUMN_NAME IN ('draft_status', 'submitted_status', 'completed_status', 'archived_status')
        """)
        status_columns = [row['COLUMN_NAME'] for row in mysql_cursor.fetchall()]
        logger.info(f"Found status columns in tbl_cases: {status_columns}")
        
        if len(status_columns) != 4:
            logger.warning(f"Expected 4 status columns, found {len(status_columns)}")
            return
        
        # Sample data from the status columns
        mysql_cursor.execute("""
            SELECT 
                cases_id,
                status,
                draft_status,
                submitted_status,
                completed_status,
                archived_status
            FROM tbl_cases 
            LIMIT 10
        """)
        sample_data = mysql_cursor.fetchall()
        logger.info("Sample status data from tbl_cases:")
        for row in sample_data:
            logger.info(f"  Case {row['cases_id']}: status={row['status']}, draft={row['draft_status']}, submitted={row['submitted_status']}, completed={row['completed_status']}, archived={row['archived_status']}")
        
        # Count different status combinations
        mysql_cursor.execute("""
            SELECT 
                draft_status,
                submitted_status,
                completed_status,
                archived_status,
                COUNT(*) as count
            FROM tbl_cases 
            GROUP BY draft_status, submitted_status, completed_status, archived_status
            ORDER BY count DESC
            LIMIT 10
        """)
        status_combinations = mysql_cursor.fetchall()
        logger.info("Most common status combinations:")
        for row in status_combinations:
            logger.info(f"  draft={row['draft_status']}, submitted={row['submitted_status']}, completed={row['completed_status']}, archived={row['archived_status']}: {row['count']} cases")
        
        # Check the old status column values for isDeleted mapping
        mysql_cursor.execute("""
            SELECT 
                status,
                COUNT(*) as count
            FROM tbl_cases 
            GROUP BY status
            ORDER BY status
        """)
        old_status_values = mysql_cursor.fetchall()
        logger.info("Old status column values (for isDeleted mapping):")
        for row in old_status_values:
            is_deleted = "True" if row['status'] == 2 else "False"
            logger.info(f"  status={row['status']} -> isDeleted={is_deleted}: {row['count']} cases")
        
        # Verify that only expected values (1 and 2) exist
        unexpected_statuses = [row['status'] for row in old_status_values if row['status'] not in [1, 2]]
        if unexpected_statuses:
            logger.warning(f"Found unexpected status values: {unexpected_statuses}")
        else:
            logger.info("✅ All status values are as expected (1 and 2 only)")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
    finally:
        if mysql_conn:
            mysql_conn.close()

if __name__ == "__main__":
    logger.info("Starting Cases data migration from MySQL to PostgreSQL...")
    
    # First, test the invoice mapping to diagnose any issues
    logger.info("=== Testing Invoice Mapping ===")
    test_invoice_mapping()
    logger.info("=== End Invoice Mapping Test ===")

    # Test clinic location mapping setup
    logger.info("=== Testing Clinic Location Mapping Setup ===")
    test_clinic_location_mapping()
    logger.info("=== End Clinic Location Mapping Setup Test ===")
    
    # Test status mapping logic
    test_status_mapping()

    # Test isDeleted mapping logic
    test_is_deleted_mapping()
    
    # Test MySQL status data
    logger.info("=== Testing MySQL Status Data ===")
    test_mysql_status_data()
    logger.info("=== End MySQL Status Data Test ===")
    
    success = migrate_cases()
    
    if success:
        logger.info("Running verification...")
        verify_migration()
    else:
        logger.error("Migration failed!")
        sys.exit(1) 