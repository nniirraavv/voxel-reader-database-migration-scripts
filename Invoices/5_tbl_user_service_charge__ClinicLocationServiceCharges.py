#!/usr/bin/env python3
"""
Data Migration Script: tbl_user_service_charge (MySQL) -> ClinicLocationServiceCharges (PostgreSQL)

This script migrates data from the MySQL table 'tbl_user_service_charge' to the PostgreSQL table 'ClinicLocationServiceCharges'.
Includes comprehensive logging to both console and log files.
Foreign key validation is performed to ensure data integrity.

Mapping Rules:
1. services_id=6 -> serviceId=5, otherwise use same value
2. clinicLocationId: user_id -> Users.olduserid -> Users.uId -> Clinics.ownerUserId -> Clinics.cId -> ClinicLocations.clinicId -> ClinicLocations.clId
3. price -> amount, rush_fee -> rushFee
"""

import sys
import os
import logging
import colorama
from colorama import Fore, Style
from datetime import datetime
from decimal import Decimal

# Add parent directory to path to import db_connections
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connections import get_mysql_connection, get_postgres_connection

colorama.init(autoreset=True)

log_dir = 'invoice_logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_filename = os.path.join(log_dir, '5_tbl_user_service_charge__ClinicLocationServiceCharges.log')
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
    logs_dir = os.path.join(script_dir, "invoice_logs")
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(logs_dir, f"5_tbl_user_service_charge__ClinicLocationServiceCharges.log")
    
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

def build_clinic_location_mapping(postgres_cursor):
    """
    Build mapping from user_id to clinicLocationId using the 3-step chain:
    1. user_id -> Users.olduserid -> Users.uId
    2. Users.uId -> Clinics.ownerUserId -> Clinics.cId  
    3. Clinics.cId -> ClinicLocations.clinicId -> ClinicLocations.clId
    """
    try:
        # Build mappings for each step
        # Step 1: Build olduserid -> uId mapping
        postgres_cursor.execute('SELECT "uId", "olduserid" FROM "Users" WHERE "olduserid" IS NOT NULL')
        olduserid_to_uid = {row[1]: row[0] for row in postgres_cursor.fetchall()}
        
        # Step 2: Build uId -> cId mapping from Clinics
        postgres_cursor.execute('SELECT "cId", "ownerUserId" FROM "Clinics" WHERE "ownerUserId" IS NOT NULL')
        uid_to_cid = {row[1]: row[0] for row in postgres_cursor.fetchall()}
        
        # Step 3: Build cId -> clId mapping from ClinicLocations
        postgres_cursor.execute('SELECT "clId", "clinicId" FROM "ClinicLocations" WHERE "clinicId" IS NOT NULL')
        cid_to_clid = {row[1]: row[0] for row in postgres_cursor.fetchall()}
        
        logger.info(f"Built mappings: olduserid->uId: {len(olduserid_to_uid)}, uId->cId: {len(uid_to_cid)}, cId->clId: {len(cid_to_clid)}")
        return olduserid_to_uid, uid_to_cid, cid_to_clid
        
    except Exception as e:
        logger.error(f"Error building clinic location mapping: {e}")
        return {}, {}, {}

def remove_clscid_default(postgres_cursor):
    """Remove the default/sequence from clscId in ClinicLocationServiceCharges table so we can insert explicit values."""
    try:
        postgres_cursor.execute('''
            ALTER TABLE "ClinicLocationServiceCharges" ALTER COLUMN "clscId" DROP DEFAULT
        ''')
        logger.info("Dropped default/sequence from ClinicLocationServiceCharges.clscId to allow explicit inserts.")
    except Exception as e:
        logger.warning(f"Could not drop default/sequence from ClinicLocationServiceCharges.clscId: {e}")

def get_clinic_location_id(user_id, olduserid_to_uid, uid_to_cid, cid_to_clid):
    """
    Get clinic location ID for a given user_id using the 3-step chain
    """
    try:
        # Convert user_id to int to ensure proper comparison
        user_id = int(user_id) if user_id is not None else None
        
        # Step 1: user_id -> olduserid -> uId
        uid = olduserid_to_uid.get(user_id)
        if uid is None:
            logger.debug(f"Step 1 failed: user_id {user_id} not found in olduserid_to_uid mapping")
            return None
            
        # Step 2: uId -> ownerUserId -> cId
        cid = uid_to_cid.get(uid)
        if cid is None:
            logger.debug(f"Step 2 failed: uId {uid} not found in uid_to_cid mapping")
            return None
            
        # Step 3: cId -> clinicId -> clId
        clid = cid_to_clid.get(cid)
        if clid is None:
            logger.debug(f"Step 3 failed: cId {cid} not found in cid_to_clid mapping")
            return None
            
        logger.debug(f"Successfully mapped user_id {user_id} -> uId {uid} -> cId {cid} -> clId {clid}")
        return clid
        
    except Exception as e:
        logger.error(f"Error getting clinic location ID for user_id {user_id}: {e}")
        return None

def get_valid_services(postgres_cursor):
    """
    Get all valid service IDs from MasterServices table
    """
    try:
        postgres_cursor.execute('SELECT "sId" FROM "MasterServices"')
        valid_services = set(row[0] for row in postgres_cursor.fetchall())
        return valid_services
    except Exception as e:
        logger.error(f"Error getting valid services: {e}")
        return set()

def map_service_id(old_service_id):
    """
    Map service ID: if old_service_id=6, return 5, otherwise return same value
    """
    if old_service_id == 6:
        return 5
    return old_service_id

def validate_foreign_keys(postgres_cursor, clinic_location_id, service_id):
    """
    Validate that clinicLocationId and serviceId exist in their respective parent tables.
    
    Args:
        postgres_cursor: PostgreSQL cursor
        clinic_location_id: Clinic Location ID to validate
        service_id: Service ID to validate
    
    Returns:
        tuple: (clinic_location_exists, service_exists)
    """
    try:
        # Check if clinic location exists
        postgres_cursor.execute('SELECT COUNT(*) FROM "ClinicLocations" WHERE "clId" = %s', (clinic_location_id,))
        clinic_location_exists = postgres_cursor.fetchone()[0] > 0
        
        # Check if service exists
        postgres_cursor.execute('SELECT COUNT(*) FROM "MasterServices" WHERE "sId" = %s', (service_id,))
        service_exists = postgres_cursor.fetchone()[0] > 0
        
        return clinic_location_exists, service_exists
        
    except Exception as e:
        logger.error(f"Error validating foreign keys: {e}")
        return False, False

def get_source_data(mysql_cursor):
    """
    Fetch all data from tbl_user_service_charge
    """
    query = """
    SELECT 
        usc_id,
        services_id,
        user_id,
        price,
        rush_fee,
        status,
        position,
        add_ip,
        add_time,
        add_by,
        update_ip,
        update_time,
        update_by
    FROM tbl_user_service_charge
    ORDER BY usc_id
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

        # Truncate the table and reset the sequence for clscId
        logger.info("Truncating ClinicLocationServiceCharges and resetting clscId sequence to 1...")
        postgres_cursor.execute('TRUNCATE TABLE "ClinicLocationServiceCharges" RESTART IDENTITY CASCADE')
        postgres_conn.commit()
        logger.info("Table truncated and sequence reset.")

        # Drop default from clscId to allow explicit inserts
        remove_clscid_default(postgres_cursor)
        postgres_conn.commit()

        # Build clinic location mapping
        logger.info("Building clinic location mapping...")
        olduserid_to_uid, uid_to_cid, cid_to_clid = build_clinic_location_mapping(postgres_cursor)
        
        # Debug: Check if user_id=1023 exists in the mapping
        if 1023 in olduserid_to_uid:
            logger.warning(f"user_id=1023 found in olduserid_to_uid mapping with uId={olduserid_to_uid[1023]}")
        else:
            logger.info(f"user_id=1023 NOT found in olduserid_to_uid mapping - this is correct, should be skipped")
        
        # Show some sample mappings for debugging
        sample_olduserids = list(olduserid_to_uid.keys())[:5]
        logger.info(f"Sample olduserid values in mapping: {sample_olduserids}")

        # Get valid services
        valid_services = get_valid_services(postgres_cursor)

        # Fetch source data
        logger.info("Fetching source data from tbl_user_service_charge...")
        source_data = get_source_data(mysql_cursor)
        logger.info(f"Found {len(source_data)} records to migrate")
        if not source_data:
            logger.warning("No data found to migrate")
            return

        # Migration counters
        total_records = len(source_data)
        successful_migrations = 0
        failed_migrations = 0
        skipped_invalid_mapping = 0
        foreign_key_violations = 0

        logger.info("Starting data migration...")
        logger.info("Note: Foreign key validation will be performed - only records with valid clinicLocationId and serviceId will be migrated")
        
        for i, (usc_id, services_id, user_id, price, rush_fee, status, position, add_ip, add_time, add_by, update_ip, update_time, update_by) in enumerate(source_data, 1):
            try:
                # Debug logging for specific user_id
                if user_id == 1023:
                    logger.info(f"Processing record {usc_id} with user_id=1023")
                
                # Map service ID
                mapped_service_id = map_service_id(services_id)
                
                # Map clinic location ID using the 3-step chain
                clinic_location_id = get_clinic_location_id(user_id, olduserid_to_uid, uid_to_cid, cid_to_clid)
                if clinic_location_id is None:
                    logger.warning(f"Skipping record {usc_id}: user_id {user_id} could not be mapped to a valid clinicLocationId")
                    skipped_invalid_mapping += 1
                    continue
                
                # Debug logging for successful mapping
                if user_id == 1023:
                    logger.info(f"Record {usc_id} with user_id=1023 got clinic_location_id={clinic_location_id}")
                
                # Validate foreign key relationships
                clinic_location_exists, service_exists = validate_foreign_keys(postgres_cursor, clinic_location_id, mapped_service_id)
                
                if not clinic_location_exists:
                    logger.warning(f"Record {usc_id}: Clinic Location ID {clinic_location_id} does not exist in ClinicLocations table - skipping")
                    foreign_key_violations += 1
                    continue
                
                if not service_exists:
                    logger.warning(f"Record {usc_id}: Service ID {mapped_service_id} does not exist in MasterServices table - skipping")
                    foreign_key_violations += 1
                    continue
                
                # Prepare values
                amount = Decimal(str(price)) if price is not None else Decimal('0.00')
                rush_fee_amount = Decimal(str(rush_fee)) if rush_fee is not None else Decimal('0.00')
                created_at = add_time if add_time else datetime.now()
                updated_at = update_time if update_time else None
                
                insert_query = """
                INSERT INTO "ClinicLocationServiceCharges" (
                    "clscId", "clinicLocationId", "serviceId", amount, "rushFee", "createdAt", "updatedAt"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    usc_id,               # clscId (primary key)
                    clinic_location_id,   # clinicLocationId (mapped)
                    mapped_service_id,    # serviceId (mapped)
                    amount,               # amount
                    rush_fee_amount,      # rushFee
                    created_at,           # createdAt
                    updated_at            # updatedAt
                )
                postgres_cursor.execute(insert_query, values)
                successful_migrations += 1
                if successful_migrations % 100 == 0:
                    logger.info(f"Migrated {successful_migrations} records...")
                    postgres_conn.commit()
            except Exception as e:
                failed_migrations += 1
                logger.error(f"Record {usc_id}: Migration failed - {e}")
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
        logger.info(f"Skipped invalid mapping: {skipped_invalid_mapping}")
        logger.info(f"Foreign key violations: {foreign_key_violations}")
        logger.info(f"Success rate: {(successful_migrations/total_records)*100:.2f}%")
        logger.info("=" * 60)
        
        if failed_migrations > 0:
            logger.warning(f"⚠️  {failed_migrations} records failed to migrate. Check logs for details.")
            
        if foreign_key_violations > 0:
            logger.warning(f"⚠️  {foreign_key_violations} records had foreign key violations (missing clinicLocationId or serviceId).")
            
        if skipped_invalid_mapping > 0:
            logger.warning(f"⚠️  {skipped_invalid_mapping} records had invalid user_id mapping (could not find clinicLocationId).")
            
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
        print("🚀 Starting Migration: tbl_user_service_charge → ClinicLocationServiceCharges")
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
