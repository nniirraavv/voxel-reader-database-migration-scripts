#!/usr/bin/env python3
"""
Migration script: tbl_practice (MySQL) -> Clinics & ClinicLocations (PostgreSQL)
Migrates practice data to both Clinics and ClinicLocations tables
Based on mapping sheet specifications

This script:
1. Gets practice data from MySQL tbl_practice
2. Creates Clinics records (one per practice)
3. Creates ClinicLocations records (also one per practice)
4. Links them properly via clinicId foreign key
"""

import mysql.connector
import psycopg2
from datetime import datetime
import logging
from typing import Dict, Any, Optional, List
import sys
import os
import uuid
import argparse

# Add the parent directory to Python path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(script_dir, 'clinic_logs')
os.makedirs(log_dir, exist_ok=True)
from db_connections import get_mysql_connection, get_postgres_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    
    handlers=[
        logging.FileHandler(os.path.join(log_dir, '1_tbl_practice__Clinics_ClinicLocations.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PracticeToClinicsDataMigrator:
    def __init__(self, dry_run=False):
        """Initialize the migrator with database connections"""
        self.mysql_conn = None
        self.postgres_conn = None
        self.dry_run = dry_run
        
        # Migration statistics
        self.stats = {
            'total_mysql_practices': 0,
            'migrated_clinics': 0,
            'migrated_clinic_locations': 0,
            'skipped_duplicates': 0,
            'skipped_no_user': 0,
            'failed': 0
        }
        
        # Build user ID mapping
        self.user_id_mapping = {}  # MySQL user_id -> PostgreSQL uId
        self.available_users = []
        self.default_user_id = None

    def connect_databases(self):
        """Establish connections to both MySQL and PostgreSQL databases"""
        try:
            self.mysql_conn = get_mysql_connection()
            self.postgres_conn = get_postgres_connection()
            logger.info("Database connections established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to databases: {e}")
            raise

    def close_connections(self):
        """Close database connections"""
        if self.mysql_conn:
            self.mysql_conn.close()
        if self.postgres_conn:
            self.postgres_conn.close()
        logger.info("Database connections closed")

    def convert_status(self, mysql_status: str) -> str:
        """Map MySQL status to PostgreSQL enum values"""
        if not mysql_status:
            return 'INACTIVE'
        
        status_upper = mysql_status.upper().strip()
        
        # Map status values according to mapping sheet
        if status_upper == 'ACTIVE':
            return 'ACTIVE'
        else:
            return 'INACTIVE'

    def check_existing_clinic(self, practice_name: str) -> Optional[str]:
        """Check if clinic already exists by title"""
        try:
            cursor = self.postgres_conn.cursor()
            cursor.execute('SELECT "cId" FROM "Clinics" WHERE "title" = %s LIMIT 1', (practice_name,))
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return result[0]
            return None
            
        except Exception as e:
            logger.error(f"Error checking existing clinic: {e}")
            return None

    def check_existing_clinic_location(self, clinic_id: str, location_address: str) -> Optional[str]:
        """Check if clinic location already exists"""
        try:
            cursor = self.postgres_conn.cursor()
            cursor.execute(
                'SELECT "clId" FROM "ClinicLocations" WHERE "clinicId" = %s AND "address" = %s LIMIT 1', 
                (clinic_id, location_address)
            )
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                return result[0]
            return None
            
        except Exception as e:
            logger.error(f"Error checking existing clinic location: {e}")
            return None

    def build_user_mapping(self):
        """Build mapping from MySQL user_id to PostgreSQL uId"""
        try:
            cursor = self.postgres_conn.cursor()
            
            # First, get direct mapping using olduserid field
            cursor.execute('SELECT "uId", "olduserid" FROM "Users" WHERE "olduserid" IS NOT NULL')
            old_user_mappings = cursor.fetchall()
            
            for pg_uid, old_user_id in old_user_mappings:
                self.user_id_mapping[old_user_id] = pg_uid
            
            logger.info(f"Built olduserid mapping: {len(self.user_id_mapping)} mapped users")
            
            # Second, check for any MySQL user_ids that might directly match PostgreSQL uId values
            # Get all PostgreSQL user IDs
            cursor.execute('SELECT "uId" FROM "Users" ORDER BY "uId"')
            all_postgres_users = [row[0] for row in cursor.fetchall()]
            
            # Get MySQL user_ids that we need to map
            mysql_cursor = self.mysql_conn.cursor()
            mysql_cursor.execute('SELECT DISTINCT user_id FROM tbl_practice WHERE user_id IS NOT NULL')
            mysql_user_ids = [row[0] for row in mysql_cursor.fetchall()]
            mysql_cursor.close()
            
            # Check for direct uId matches for unmapped MySQL user_ids
            direct_matches = 0
            for mysql_user_id in mysql_user_ids:
                # Skip if already mapped via olduserid
                if mysql_user_id in self.user_id_mapping:
                    continue
                    
                # Check if this MySQL user_id exists as a PostgreSQL uId
                if mysql_user_id in all_postgres_users:
                    self.user_id_mapping[mysql_user_id] = mysql_user_id
                    direct_matches += 1
                    logger.info(f"Direct uId match found: MySQL user_id {mysql_user_id} -> PostgreSQL uId {mysql_user_id}")
            
            logger.info(f"Found {direct_matches} direct uId matches")
            
            # Get user IDs that already own clinics
            cursor.execute('SELECT DISTINCT "ownerUserId" FROM "Clinics" WHERE "ownerUserId" IS NOT NULL')
            clinic_owners = [row[0] for row in cursor.fetchall()]
            
            # Find users that don't own clinics yet
            self.available_users = [uid for uid in all_postgres_users if uid not in clinic_owners]
            
            # Set up a default user for cases where no mapping exists
            # Since ownerUserId is NOT NULL, we need to provide a default
            if self.available_users:
                self.default_user_id = self.available_users[0]
            elif all_postgres_users:
                # If no available users, use the first user (this might violate unique constraints but we'll handle that)
                self.default_user_id = all_postgres_users[0]
            else:
                # No users exist at all - this is a problem
                logger.error("No users found in PostgreSQL database - cannot assign clinic ownership")
                self.default_user_id = None
            
            cursor.close()
            
            logger.info(f"Total user mapping built: {len(self.user_id_mapping)} mapped users")
            logger.info(f"Available users for clinic assignment: {len(self.available_users)}")
            logger.info(f"Default user ID for unmapped practices: {self.default_user_id}")
            
        except Exception as e:
            logger.error(f"Error building user mapping: {e}")
            # Continue without user mapping if it fails
            self.user_id_mapping = {}
            self.available_users = []
            self.default_user_id = None

    def create_clinic(self, practice_data: Dict[str, Any]) -> Optional[str]:
        """Create a new clinic record"""
        try:
            # Get user ID - MUST NOT BE NULL due to database constraint
            practice_user_id = practice_data.get('user_id')
            owner_user_id = None
            
            # Try to map user from MySQL to PostgreSQL
            if practice_user_id and practice_user_id in self.user_id_mapping:
                mapped_user = self.user_id_mapping[practice_user_id]
                if mapped_user in self.available_users:
                    # Remove from available list so it won't be assigned again
                    self.available_users.remove(mapped_user)
                    owner_user_id = mapped_user
                    logger.info(f"Assigned mapped user {owner_user_id} to practice {practice_data.get('practice_id')}")
            
            # If no mapped user, skip this practice instead of using default
            if owner_user_id is None:
                logger.warning(f"Skipping practice {practice_data.get('practice_id')} - no user mapping found for user_id {practice_user_id}")
                return None
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create clinic: {practice_data.get('practice_name')} with user {owner_user_id}")
                return "dry_run_clinic_id"
            
            cursor = self.postgres_conn.cursor()
            
            # Handle address construction
            address_parts = []
            if practice_data.get('street_line_one'):
                address_parts.append(practice_data['street_line_one'])
            if practice_data.get('street_line_two'):
                address_parts.append(practice_data['street_line_two'])
            if practice_data.get('city'):
                address_parts.append(practice_data['city'])
            if practice_data.get('region'):
                address_parts.append(practice_data['region'])
            if practice_data.get('country'):
                address_parts.append(practice_data['country'])
            
            full_address = ', '.join(address_parts) if address_parts else None
            
            # Map MySQL status to PostgreSQL enum - keep null if no valid status
            mysql_status = practice_data.get('status')
            pg_status = None
            
            if mysql_status is not None:
                if str(mysql_status) == '1':
                    pg_status = 'APPROVED'
                elif str(mysql_status) == '0':
                    pg_status = 'DISABLE'
                # If status is neither 1 nor 0, keep it as None
            
            # Build the query dynamically based on whether status is provided
            if pg_status is not None:
                insert_query = '''
                    INSERT INTO "Clinics" (
                        "ownerUserId", "title", "contactNumber", 
                        "address", "status", "isDeleted", "invoiceType"
                    ) VALUES (%s, %s, %s, %s, %s::"enum_Clinics_status", %s, %s::"enum_Clinics_invoiceType")
                    RETURNING "cId"
                '''
                query_params = (
                    owner_user_id,
                    practice_data.get('practice_name', 'Unknown Practice'),
                    practice_data.get('phonenumber'),
                    full_address,
                    pg_status,
                    False,  # isDeleted
                    'PAY_AS_YOU_GO'  # Default invoice type
                )
            else:
                insert_query = '''
                    INSERT INTO "Clinics" (
                        "ownerUserId", "title", "contactNumber", 
                        "address", "isDeleted", "invoiceType"
                    ) VALUES (%s, %s, %s, %s, %s, %s::"enum_Clinics_invoiceType")
                    RETURNING "cId"
                '''
                query_params = (
                    owner_user_id,
                    practice_data.get('practice_name', 'Unknown Practice'),
                    practice_data.get('phonenumber'),
                    full_address,
                    False,  # isDeleted
                    'PAY_AS_YOU_GO'  # Default invoice type
                )
            
            cursor.execute(insert_query, query_params)
            
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                logger.info(f"Created clinic: {practice_data.get('practice_name')} with ID: {result[0]}")
                return result[0]
            return None
            
        except Exception as e:
            logger.error(f"Error creating clinic for practice {practice_data.get('practice_id')}: {e}")
            # Rollback this specific transaction
            try:
                self.postgres_conn.rollback()
            except:
                pass
            return None

    def create_clinic_location(self, clinic_id: str, practice_data: Dict[str, Any]) -> Optional[str]:
        """Create a new clinic location record"""
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create clinic location for clinic {clinic_id}")
                return "dry_run_location_id"
                
            cursor = self.postgres_conn.cursor()
            
            # Handle address construction (same as clinic)
            address_parts = []
            if practice_data.get('street_line_one'):
                address_parts.append(practice_data['street_line_one'])
            if practice_data.get('street_line_two'):
                address_parts.append(practice_data['street_line_two'])
            if practice_data.get('city'):
                address_parts.append(practice_data['city'])
            if practice_data.get('region'):
                address_parts.append(practice_data['region'])
            if practice_data.get('country'):
                address_parts.append(practice_data['country'])
            
            full_address = ', '.join(address_parts) if address_parts else None
            
            insert_query = '''
                INSERT INTO "ClinicLocations" (
                    "clinicId", "contactNumber", "address", "status", 
                    "isDeleted", "zipcode", "paymentMethod"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::"enum_ClinicLocations_paymentMethod")
                RETURNING "clId"
            '''
            
            cursor.execute(insert_query, (
                clinic_id,
                practice_data.get('phonenumber'),
                full_address,
                True,  # status (boolean - active)
                False,  # isDeleted
                practice_data.get('zipcode', '00000'),
                'PAY_AS_YOU_GO'  # Default payment method
            ))
            
            result = cursor.fetchone()
            cursor.close()
            
            if result:
                logger.info(f"Created clinic location for clinic {clinic_id} with location ID: {result[0]}")
                return result[0]
            return None
            
        except Exception as e:
            logger.error(f"Error creating clinic location for practice {practice_data.get('practice_id')}: {e}")
            self.postgres_conn.rollback()
            return None

    def migrate_practice_record(self, practice_data: Dict[str, Any]) -> bool:
        """Migrate a single practice record to both Clinics and ClinicLocations"""
        try:
            practice_id = practice_data.get('practice_id')
            practice_name = practice_data.get('practice_name', 'Unknown Practice')
            
            logger.info(f"Migrating practice {practice_id}: {practice_name}")
            
            # Step 1: Create or get the Clinic record
            clinic_id = self.create_clinic(practice_data)
            if not clinic_id:
                logger.warning(f"Failed to create clinic for practice {practice_id} - skipping")
                self.stats['skipped_no_user'] += 1
                return False
            
            # Step 2: Create the ClinicLocation record
            location_id = self.create_clinic_location(clinic_id, practice_data)
            if not location_id:
                logger.error(f"Failed to create clinic location for practice {practice_id}")
                self.stats['failed'] += 1
                return False
            
            # Commit the transaction (only if not dry run)
            if not self.dry_run:
                self.postgres_conn.commit()
            
            # Update statistics
            self.stats['migrated_clinics'] += 1
            self.stats['migrated_clinic_locations'] += 1
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would have migrated practice {practice_id} to clinic {clinic_id} and location {location_id}")
            else:
                logger.info(f"Successfully migrated practice {practice_id} to clinic {clinic_id} and location {location_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error migrating practice record {practice_data.get('practice_id')}: {e}")
            if not self.dry_run:
                self.postgres_conn.rollback()
            self.stats['failed'] += 1
            return False

    def fetch_practices_from_mysql(self):
        """Fetch practice data from MySQL tbl_practice table"""
        try:
            cursor = self.mysql_conn.cursor(dictionary=True)
            
            query = """
                SELECT 
                    practice_id,
                    practice_name,
                    street_line_one,
                    street_line_two,
                    city,
                    region,
                    zipcode,
                    country,
                    phonenumber,
                    status,
                    user_id,
                    add_time,
                    update_time
                FROM tbl_practice
                WHERE practice_name IS NOT NULL 
                AND practice_name != ''
                ORDER BY practice_id
            """
            
            cursor.execute(query)
            practices = cursor.fetchall()
            cursor.close()
            
            self.stats['total_mysql_practices'] = len(practices)
            logger.info(f"Fetched {len(practices)} practices from MySQL")
            return practices
            
        except Exception as e:
            logger.error(f"Error fetching practices from MySQL: {e}")
            return []

    def migrate_data(self):
        """Main data migration process"""
        try:
            # Fetch practices from MySQL
            practices = self.fetch_practices_from_mysql()
            
            if not practices:
                logger.warning("No practices found to migrate")
                return
            
            logger.info(f"Starting migration of {len(practices)} practices...")
            
            # Process each practice
            for i, practice_data in enumerate(practices, 1):
                try:
                    self.migrate_practice_record(practice_data)
                    
                    # Log progress every 10 records (since practices are typically fewer)
                    if i % 10 == 0:
                        logger.info(f"Processed {i}/{len(practices)} practices...")
                        
                except Exception as e:
                    logger.error(f"Error processing practice {practice_data.get('practice_id', 'unknown')}: {e}")
                    self.stats['failed'] += 1
                    continue
            
            logger.info("Data migration completed")
            
        except Exception as e:
            logger.error(f"Error during data migration: {e}")
            raise

    def print_final_stats(self):
        """Print final migration statistics"""
        logger.info("=== MIGRATION STATISTICS ===")
        logger.info(f"Total MySQL practices processed: {self.stats['total_mysql_practices']}")
        logger.info(f"Clinics migrated: {self.stats['migrated_clinics']}")
        logger.info(f"Clinic locations migrated: {self.stats['migrated_clinic_locations']}")
        logger.info(f"Skipped duplicates: {self.stats['skipped_duplicates']}")
        logger.info(f"Skipped no user: {self.stats['skipped_no_user']}")
        logger.info(f"Failed: {self.stats['failed']}")
        logger.info("============================")

    def run(self):
        """Main migration process"""
        try:
            logger.info("Starting tbl_practice to Clinics & ClinicLocations migration")
            
            # Establish database connections
            self.connect_databases()
            
            # Build user ID mapping
            self.build_user_mapping()
            
            # Migrate data
            self.migrate_data()
            
            logger.info("Migration completed!")
            self.print_final_stats()
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise
        finally:
            self.close_connections()

    def validate_migration(self):
        """Validate the migration results"""
        try:
            logger.info("Validating migration...")
            
            # Connect to databases
            self.connect_databases()
            
            # Check MySQL vs PostgreSQL counts
            mysql_cursor = self.mysql_conn.cursor()
            mysql_cursor.execute("""
                SELECT COUNT(*) FROM tbl_practice 
                WHERE practice_name IS NOT NULL 
                AND practice_name != ''
            """)
            mysql_count = mysql_cursor.fetchone()[0]
            
            postgres_cursor = self.postgres_conn.cursor()
            postgres_cursor.execute('SELECT COUNT(*) FROM "Clinics"')
            clinics_count = postgres_cursor.fetchone()[0]
            
            postgres_cursor.execute('SELECT COUNT(*) FROM "ClinicLocations"')
            locations_count = postgres_cursor.fetchone()[0]
            
            logger.info(f"MySQL practices: {mysql_count}")
            logger.info(f"PostgreSQL Clinics: {clinics_count}")
            logger.info(f"PostgreSQL ClinicLocations: {locations_count}")
            
            # Sample validation - show clinic and location pairs
            postgres_cursor.execute('''
                SELECT c."cId", c."title" as clinic_name, c."status",
                       cl."clId", cl."address", cl."zipcode"
                FROM "Clinics" c
                JOIN "ClinicLocations" cl ON c."cId" = cl."clinicId"
                ORDER BY c."title"
                LIMIT 5
            ''')
            sample_records = postgres_cursor.fetchall()
            
            logger.info("Sample migrated records:")
            for record in sample_records:
                logger.info(f"  Clinic: {record[1]} ({record[2]}) -> Location: {record[4]} in {record[5]}")
                
            mysql_cursor.close()
            postgres_cursor.close()
                
        except Exception as e:
            logger.error(f"Validation failed: {e}")
        finally:
            self.close_connections()


def main():
    """Main function to run the migration"""
    parser = argparse.ArgumentParser(description='Migrate tbl_practice to Clinics & ClinicLocations')
    parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode (no actual changes)')
    parser.add_argument('--validate-only', action='store_true', help='Only run validation, skip migration')
    args = parser.parse_args()
    
    migrator = PracticeToClinicsDataMigrator(dry_run=args.dry_run)
    
    try:
        if args.validate_only:
            logger.info("Running validation only...")
            migrator.validate_migration()
        else:
            if args.dry_run:
                logger.info("Running in DRY RUN mode - no changes will be made")
            else:
                logger.info("Running in LIVE mode - changes will be made to the database")
            
            # Run the migration
            migrator.run()
            
            # Validate the migration (skip if dry run)
            if not args.dry_run:
                migrator.validate_migration()
        
    except Exception as e:
        logger.error(f"Migration process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()