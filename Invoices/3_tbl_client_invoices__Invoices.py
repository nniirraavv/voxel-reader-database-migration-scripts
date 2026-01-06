#!/usr/bin/env python3
"""
Migration Script: tbl_client_invoices (MySQL) -> Invoices (PostgreSQL)
Author: AI Assistant
Date: Generated automatically
"""

import logging
import sys
import os
import traceback
import argparse
from datetime import datetime

# Add parent directory to path to import db_connections
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connections import get_mysql_connection, get_postgres_connection

# Setup logging directory relative to script location
script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(script_dir, "invoice_logs")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Create log filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = os.path.join(log_dir, f"3_tbl_client_invoices__Invoices.log")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True  # Force reconfiguration if logging was already configured
)

logger = logging.getLogger(__name__)

class InvoiceMigration:
    def __init__(self, dry_run=True, default_clinic_id=None, skip_invalid=False):
        self.mysql_conn = None
        self.postgres_conn = None
        self.mysql_cursor = None
        self.postgres_cursor = None
        self.total_records = 0
        self.migrated_records = 0
        self.failed_records = 0
        self.skipped_records = 0
        self.errors = []
        self.warnings = []
        self.dry_run = dry_run
        self.available_enum_values = []
        self.default_clinic_id = default_clinic_id
        self.skip_invalid = skip_invalid
        self.valid_clinic_ids = set()
        self.user_to_clinic_mapping = {}

    def connect_databases(self):
        """Establish connections to both MySQL and PostgreSQL databases"""
        try:
            logger.info("Connecting to MySQL database...")
            self.mysql_conn = get_mysql_connection()
            self.mysql_cursor = self.mysql_conn.cursor(dictionary=True)
            logger.info("✅ MySQL connection established")

            logger.info("Connecting to PostgreSQL database...")
            self.postgres_conn = get_postgres_connection()
            self.postgres_cursor = self.postgres_conn.cursor()
            logger.info("✅ PostgreSQL connection established")

            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {str(e)}")
            return False

    def load_valid_clinic_ids(self):
        """Create mapping from MySQL user_id to PostgreSQL clinic location ID"""
        try:
            logger.info("Building clinic location mapping...")
            
            # Create the lookup chain: user_id -> Users.olduserid -> Users.uId -> Clinics.ownerUserId -> Clinics.cId -> ClinicLocations.clinicId -> ClinicLocations.clId
            query = """
                SELECT 
                    u."olduserid" as mysql_user_id,
                    u."uId" as user_id,
                    c."cId" as clinic_id,
                    cl."clId" as clinic_location_id
                FROM "Users" u
                INNER JOIN "Clinics" c ON u."uId" = c."ownerUserId"
                INNER JOIN "ClinicLocations" cl ON c."cId" = cl."clinicId"
                WHERE u."olduserid" IS NOT NULL
            """
            
            self.postgres_cursor.execute(query)
            results = self.postgres_cursor.fetchall()
            
            # Create mapping dictionary: mysql_user_id -> clinic_location_id
            self.user_to_clinic_mapping = {}
            
            for row in results:
                mysql_user_id = row[0]
                user_id = row[1]
                clinic_id = row[2]
                clinic_location_id = row[3]
                
                self.user_to_clinic_mapping[mysql_user_id] = clinic_location_id
                
                logger.debug(f"Mapping: user_id {mysql_user_id} -> uId {user_id} -> cId {clinic_id} -> clId {clinic_location_id}")
            
            logger.info(f"Created {len(self.user_to_clinic_mapping)} clinic location mappings")
            
            # Log some sample mappings for debugging
            if self.user_to_clinic_mapping:
                sample_mappings = list(self.user_to_clinic_mapping.items())[:5]
                logger.info(f"Sample clinic location mappings: {sample_mappings}")
                
                # Show the range of user IDs in the mapping
                user_ids = list(self.user_to_clinic_mapping.keys())
                min_user_id = min(user_ids)
                max_user_id = max(user_ids)
                logger.info(f"User ID range in mapping: {min_user_id} to {max_user_id}")
                
                # Set default clinic ID if not provided
                if self.default_clinic_id is None:
                    self.default_clinic_id = None
                    logger.info(f"Using default clinic ID: {self.default_clinic_id}")
            else:
                logger.warning("No clinic location mappings found!")
                
                # Fallback: Get any available clinic location ID
                
                if self.default_clinic_id is None:
                    self.default_clinic_id = 100000
                    logger.warning(f"Using fallback default clinic ID: {self.default_clinic_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating clinic location mapping: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False

    def get_source_data(self):
        """Fetch all records from MySQL tbl_client_invoices"""
        try:
            query = """
            SELECT id, invoice_no, invoice_type, user_id, filename, 
                   invoice_amount, payment_status, tx_id, send_status,
                   created_at, month, year
            FROM tbl_client_invoices
            ORDER BY id
            """
            
            logger.info("Fetching source data from MySQL...")
            self.mysql_cursor.execute(query)
            records = self.mysql_cursor.fetchall()
            self.total_records = len(records)
            logger.info(f"Found {self.total_records} records to migrate")
            
            # Analyze user_id distribution
            user_ids = [r['user_id'] for r in records]
            logger.info(f"User ID range in source: {min(user_ids)} - {max(user_ids)}")
            
            # Check mapping coverage
            mapped_user_ids = [user_id for user_id in user_ids if user_id in self.user_to_clinic_mapping]
            logger.info(f"Users with clinic location mappings: {len(mapped_user_ids)}/{len(set(user_ids))} unique users")
            
            if mapped_user_ids:
                logger.info(f"Sample mapped user_ids: {list(set(mapped_user_ids))[:5]}")
            else:
                logger.warning("No user_ids from source data have clinic location mappings!")
                unique_user_ids = list(set(user_ids))[:5]
                logger.warning(f"Sample unmapped user_ids: {unique_user_ids}")
            
            return records
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch source data: {str(e)}")
            return []

    def check_enum_values(self):
        """Check what enum values are available for invoiceType"""
        try:
            query = """
            SELECT unnest(enum_range(NULL::"enum_Invoices_invoiceType")) as enum_value
            """
            self.postgres_cursor.execute(query)
            self.available_enum_values = [row[0] for row in self.postgres_cursor.fetchall()]
            logger.info(f"Available invoiceType enum values: {self.available_enum_values}")
            return self.available_enum_values
        except Exception as e:
            logger.warning(f"Could not fetch enum values: {str(e)}")
            # Default enum values if we can't fetch them
            self.available_enum_values = ['ADHOC','MONTHLY', 'YEARLY', 'QUARTERLY', 'WEEKLY', 'CUSTOM']
            logger.info(f"Using default enum values: {self.available_enum_values}")
            return self.available_enum_values

    def map_invoice_type(self, invoice_type):
        """Map MySQL invoice_type to PostgreSQL enum values"""
        # Create mapping based on available enum values
        type_mapping = {}
        for enum_val in self.available_enum_values:
            type_mapping[enum_val.upper()] = enum_val
        
        # Add common mappings
        type_mapping.update({
            'REGULAR': self.available_enum_values[0] if self.available_enum_values else 'MONTHLY',
            'STANDARD': self.available_enum_values[0] if self.available_enum_values else 'MONTHLY'
        })
        
        mapped_type = type_mapping.get(invoice_type.upper())
        
        if mapped_type is None:
            # If no mapping found, use the first available enum value
            mapped_type = self.available_enum_values[0] if self.available_enum_values else 'MONTHLY'
            warning_msg = f"Unknown invoice_type '{invoice_type}' mapped to '{mapped_type}'"
            logger.warning(warning_msg)
            self.warnings.append(warning_msg)
        elif mapped_type != invoice_type:
            warning_msg = f"Mapped invoice_type '{invoice_type}' to '{mapped_type}'"
            logger.debug(warning_msg)
        
        return mapped_type

    def resolve_clinic_location_id(self, user_id, record_id):
        """Resolve clinic location ID from user_id. If not found, return None (NULL in DB) and do not skip the record."""
        if user_id in self.user_to_clinic_mapping:
            clinic_location_id = self.user_to_clinic_mapping[user_id]
            logger.debug(f"Record {record_id}: user_id {user_id} found in mapping, clinicLocationId: {clinic_location_id}")
            return clinic_location_id
        else:
            warning_msg = f"No clinic location ID available for record {record_id}; setting clinicLocationId to NULL"
            logger.warning(warning_msg)
            self.warnings.append(warning_msg)
            return None

    def transform_record(self, record):
        """Transform MySQL record to PostgreSQL format with validation, including iId."""
        try:
            # Validate required fields
            if not record.get('invoice_no'):
                error_msg = f"Record {record.get('id')} missing invoice_no"
                logger.error(error_msg)
                self.errors.append(error_msg)
                return None

            if not record.get('invoice_type'):
                error_msg = f"Record {record.get('id')} missing invoice_type"
                logger.error(error_msg)
                self.errors.append(error_msg)
                return None

            # Resolve clinic location ID
            clinic_location_id = self.resolve_clinic_location_id(record['user_id'], record['id'])
            # Do NOT skip the record if clinic_location_id is None; allow NULL

            # Basic field mappings
            transformed = {
                'iId': record['id'],
                'invoiceType': self.map_invoice_type(record['invoice_type']),
                'clinicLocationId': clinic_location_id,
                'monthNumber': record['month'],
                'yearNumber': record['year'],
                'emailedStatus': bool(record['send_status']) if record['send_status'] is not None else False,
                'createdAt': record['created_at'],
                'updatedAt': datetime.now(),
                'isDeleted': False,
                'deletedAt': None,
                'invoiceNo': record['invoice_no'] if record['invoice_no'] else ''
            }

            # Validation checks
            if record['invoice_no'] and len(record['invoice_no']) > 30:
                error_msg = f"Invoice number '{record['invoice_no']}' exceeds 30 character limit for record {record['id']}"
                logger.error(error_msg)
                self.errors.append(error_msg)
                return None

            # Validate month and year
            if not (1 <= transformed['monthNumber'] <= 12):
                error_msg = f"Invalid month {transformed['monthNumber']} for record {record['id']}"
                logger.error(error_msg)
                self.errors.append(error_msg)
                return None

            if transformed['yearNumber'] < 1900 or transformed['yearNumber'] > 2100:
                warning_msg = f"Unusual year {transformed['yearNumber']} for record {record['id']}"
                logger.warning(warning_msg)
                self.warnings.append(warning_msg)

            return transformed
        except Exception as e:
            error_msg = f"Failed to transform record {record.get('id', 'unknown')}: {str(e)}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return None

    def validate_insert_query(self, transformed_record, original_id):
        """Validate the insert query without executing it, including iId."""
        try:
            insert_query = """
            INSERT INTO "Invoices" (
                "iId", "invoiceType", "clinicLocationId", "monthNumber", "yearNumber",
                "emailedStatus", "createdAt", "updatedAt", "isDeleted", 
                "deletedAt", "invoiceNo"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            values = (
                transformed_record['iId'],
                transformed_record['invoiceType'],
                transformed_record['clinicLocationId'],
                transformed_record['monthNumber'],
                transformed_record['yearNumber'],
                transformed_record['emailedStatus'],
                transformed_record['createdAt'],
                transformed_record['updatedAt'],
                transformed_record['isDeleted'],
                transformed_record['deletedAt'],
                transformed_record['invoiceNo']
            )
            logger.debug(f"DRY RUN: Would insert record {original_id} with values: {values}")
            self.migrated_records += 1
            return True
        except Exception as e:
            self.failed_records += 1
            error_msg = f"Failed to validate insert for record {original_id}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            self.errors.append(error_msg)
            return False

    def insert_record(self, transformed_record, original_id):
        """Insert transformed record into PostgreSQL or validate in dry run, including iId."""
        if self.dry_run:
            return self.validate_insert_query(transformed_record, original_id)
        try:
            insert_query = """
            INSERT INTO "Invoices" (
                "iId", "invoiceType", "clinicLocationId", "monthNumber", "yearNumber",
                "emailedStatus", "createdAt", "updatedAt", "isDeleted", 
                "deletedAt", "invoiceNo"
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            values = (
                transformed_record['iId'],
                transformed_record['invoiceType'],
                transformed_record['clinicLocationId'],
                transformed_record['monthNumber'],
                transformed_record['yearNumber'],
                transformed_record['emailedStatus'],
                transformed_record['createdAt'],
                transformed_record['updatedAt'],
                transformed_record['isDeleted'],
                transformed_record['deletedAt'],
                transformed_record['invoiceNo']
            )
            self.postgres_cursor.execute(insert_query, values)
            self.migrated_records += 1
            logger.debug(f"✅ Migrated record {original_id}")
            return True
        except Exception as e:
            self.failed_records += 1
            error_msg = f"Failed to insert record {original_id}: {str(e)}"
            logger.error(f"❌ {error_msg}")
            self.errors.append(error_msg)
            return False

    def run_migration(self):
        """Main migration process"""
        mode = "DRY RUN" if self.dry_run else "LIVE MIGRATION"
        strategy = "SKIP INVALID" if self.skip_invalid else f"USE DEFAULT CLINIC ID ({self.default_clinic_id})"
        logger.info(f"🚀 Starting {mode}: tbl_client_invoices -> Invoices")
        logger.info(f"📋 Strategy: {strategy}")
        if not self.connect_databases():
            return False
        try:
            # Disable triggers on Invoices table before migration
            if not self.dry_run:
                logger.info("Disabling triggers on Invoices table...")
                self.postgres_cursor.execute('ALTER TABLE "Invoices" DISABLE TRIGGER ALL')
                self.postgres_conn.commit()
            # Load valid clinic IDs
            if not self.load_valid_clinic_ids():
                return False
            # Check available enum values
            self.check_enum_values()
            # Get source data
            source_records = self.get_source_data()
            if not source_records:
                logger.error("❌ No source data found or failed to fetch data")
                return False
            if not self.dry_run:
                logger.info("Starting PostgreSQL transaction...")
            else:
                logger.info("🧪 Running in DRY RUN mode - no data will be inserted")
            # Process each record
            for i, record in enumerate(source_records, 1):
                try:
                    # Transform record
                    transformed = self.transform_record(record)
                    if transformed is None:
                        continue
                    # Insert record (or validate in dry run)
                    self.insert_record(transformed, record['id'])
                    # Progress logging
                    if i % 100 == 0:
                        logger.info(f"Progress: {i}/{self.total_records} records processed")
                except Exception as e:
                    self.failed_records += 1
                    error_msg = f"Error processing record {record.get('id', 'unknown')}: {str(e)}"
                    logger.error(f"❌ {error_msg}")
                    self.errors.append(error_msg)
                    continue
            if not self.dry_run:
                # Commit transaction only for live migration
                self.postgres_conn.commit()
                logger.info("✅ Transaction committed successfully")
                # Re-enable triggers on Invoices table after migration
                logger.info("Re-enabling triggers on Invoices table...")
                self.postgres_cursor.execute('ALTER TABLE "Invoices" ENABLE TRIGGER ALL')
                self.postgres_conn.commit()
            else:
                logger.info("✅ Dry run completed - validation finished")
            return True
        except Exception as e:
            logger.error(f"❌ Migration failed: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if self.postgres_conn and not self.dry_run:
                self.postgres_conn.rollback()
                logger.info("Transaction rolled back")
            return False

    def test_clinic_location_mapping(self):
        """Test function to verify clinic location mapping is working correctly"""
        try:
            logger.info("=== Testing Clinic Location Mapping Setup ===")
            
            if not self.connect_databases():
                return False
            
            # Test 1: Check Users table with olduserid
            self.postgres_cursor.execute('SELECT COUNT(*) FROM "Users" WHERE "olduserid" IS NOT NULL')
            users_with_old_id = self.postgres_cursor.fetchone()[0]
            logger.info(f"Users with olduserid: {users_with_old_id}")
            
            # Test 2: Check Clinics table
            self.postgres_cursor.execute('SELECT COUNT(*) FROM "Clinics"')
            clinics_count = self.postgres_cursor.fetchone()[0]
            logger.info(f"Total Clinics: {clinics_count}")
            
            # Test 3: Check ClinicLocations table
            self.postgres_cursor.execute('SELECT COUNT(*) FROM "ClinicLocations"')
            clinic_locations_count = self.postgres_cursor.fetchone()[0]
            logger.info(f"Total ClinicLocations: {clinic_locations_count}")
            
            # Test 4: Check the complete join
            self.postgres_cursor.execute("""
                SELECT COUNT(*) FROM "Users" u
                INNER JOIN "Clinics" c ON u."uId" = c."ownerUserId"
                INNER JOIN "ClinicLocations" cl ON c."cId" = cl."clinicId"
                WHERE u."olduserid" IS NOT NULL
            """)
            complete_mappings = self.postgres_cursor.fetchone()[0]
            logger.info(f"Complete user -> clinic location mappings: {complete_mappings}")
            
            # Test 5: Show sample mappings
            self.postgres_cursor.execute("""
                SELECT 
                    u."olduserid" as mysql_user_id,
                    u."uId" as user_id,
                    c."cId" as clinic_id,
                    cl."clId" as clinic_location_id
                FROM "Users" u
                INNER JOIN "Clinics" c ON u."uId" = c."ownerUserId"
                INNER JOIN "ClinicLocations" cl ON c."cId" = cl."clinicId"
                WHERE u."olduserid" IS NOT NULL
                LIMIT 5
            """)
            sample_mappings = self.postgres_cursor.fetchall()
            logger.info(f"Sample mappings: {sample_mappings}")
            
            if complete_mappings == 0:
                logger.warning("⚠️ NO COMPLETE MAPPINGS FOUND! Check:")
                logger.warning("1. Users table has olduserid values")
                logger.warning("2. Clinics table has ownerUserId matching Users.uId")
                logger.warning("3. ClinicLocations table has clinicId matching Clinics.cId")
                return False
            else:
                logger.info(f"✅ Found {complete_mappings} valid clinic location mappings")
                return True
                
        except Exception as e:
            logger.error(f"Test failed: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
        finally:
            logger.info("=== End Clinic Location Mapping Setup Test ===")

    def print_summary(self):
        """Print migration summary"""
        mode = "DRY RUN" if self.dry_run else "LIVE MIGRATION"
        logger.info("\n" + "="*60)
        logger.info(f"{mode} SUMMARY")
        logger.info("="*60)
        logger.info(f"Total records in source: {self.total_records}")
        logger.info(f"Successfully processed: {self.migrated_records}")
        logger.info(f"Skipped records: {self.skipped_records}")
        logger.info(f"Failed records: {self.failed_records}")
        
        processed_total = self.migrated_records + self.skipped_records
        if processed_total > 0:
            logger.info(f"Success rate: {(self.migrated_records/processed_total*100):.2f}%")
        
        logger.info(f"Total warnings: {len(self.warnings)}")
        logger.info(f"Total errors: {len(self.errors)}")
        
        if self.warnings:
            logger.info(f"\nFirst 5 warnings:")
            for i, warning in enumerate(self.warnings[:5], 1):
                logger.info(f"{i}. {warning}")
            if len(self.warnings) > 5:
                logger.info(f"... and {len(self.warnings) - 5} more warnings")
        
        if self.errors:
            logger.info(f"\nFirst 5 errors:")
            for i, error in enumerate(self.errors[:5], 1):
                logger.info(f"{i}. {error}")
            if len(self.errors) > 5:
                logger.info(f"... and {len(self.errors) - 5} more errors")
        
        if self.dry_run:
            logger.info(f"\n💡 This was a DRY RUN - no data was actually inserted.")
            logger.info(f"💡 To perform the actual migration, run with --live flag")
        
        logger.info("="*60)

    def close_connections(self):
        """Close database connections"""
        try:
            if self.mysql_cursor:
                self.mysql_cursor.close()
            if self.mysql_conn:
                self.mysql_conn.close()
                logger.info("MySQL connection closed")
                
            if self.postgres_cursor:
                self.postgres_cursor.close()
            if self.postgres_conn:
                self.postgres_conn.close()
                logger.info("PostgreSQL connection closed")
                
        except Exception as e:
            logger.error(f"Error closing connections: {str(e)}")

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Migrate data from MySQL tbl_client_invoices to PostgreSQL Invoices')
    parser.add_argument('--dry', action='store_true', help='Perform a dry run (default is live)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    parser.add_argument('--default-clinic-id', type=int, help='Default clinic location ID to use for invalid user_ids')
    parser.add_argument('--skip-invalid', action='store_true', help='Skip records with invalid clinic location IDs instead of using default')
    parser.add_argument('--test-mapping', action='store_true', help='Test clinic location mapping setup and exit')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    migration = InvoiceMigration(
        dry_run=args.dry, 
        default_clinic_id=args.default_clinic_id,
        skip_invalid=args.skip_invalid
    )
    
    try:
        # Handle test mapping mode
        if args.test_mapping:
            logger.info("🧪 Testing clinic location mapping setup...")
            success = migration.test_clinic_location_mapping()
            if success:
                logger.info("🎉 Clinic location mapping test completed successfully!")
                return 0
            else:
                logger.error("💥 Clinic location mapping test failed!")
                return 1
        
        # Normal migration mode
        # Determine if this is a dry run
        dry_run = args.dry
        
        success = migration.run_migration()
        migration.print_summary()
        
        if success:
            if dry_run:
                logger.info("🎉 Dry run completed successfully!")
                logger.info("💡 Review the logs and run with --live to perform actual migration")
            else:
                logger.info("🎉 Migration completed successfully!")
            return 0
        else:
            logger.error("💥 Migration failed!")
            return 1
            
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return 1
    finally:
        migration.close_connections()

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
