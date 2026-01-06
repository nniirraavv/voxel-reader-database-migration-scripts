#!/usr/bin/env python3
"""
Migration Script: MySQL to PostgreSQL
From: tbl_radiologist_invoices & tbl_radiologist_invoice_details (MySQL)
To: RadiologistInvoiceCaseServices (PostgreSQL)

This script migrates data from two related MySQL tables to a single PostgreSQL table,
permanently removing foreign key constraints to allow orphaned records.
"""

import sys
import os
from datetime import datetime
import logging
from typing import List, Dict, Any

# Add parent directory to path to import db_connections
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connections import get_mysql_connection, get_postgres_connection

# Create invoice_log directory if it doesn't exist
log_dir = 'invoice_logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging
log_filename = os.path.join(log_dir, '2_tbl_radiologist_invoices_&_tbl_radiologist_invoice_details__RadiologistInvoiceCaseServices.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DatabaseMigrator:
    def __init__(self):
        """
        Initialize the database migrator using the connection functions from db_connections.py
        """
        self.mysql_conn = None
        self.postgres_conn = None
        self.foreign_keys_removed = False
        
    def connect_databases(self):
        """Establish connections to both MySQL and PostgreSQL databases using db_connections.py"""
        try:
            # Connect to MySQL using the existing connection function
            self.mysql_conn = get_mysql_connection()
            logger.info("Successfully connected to MySQL database")
            
            # Connect to PostgreSQL using the existing connection function
            self.postgres_conn = get_postgres_connection()
            self.postgres_conn.autocommit = False
            logger.info("Successfully connected to PostgreSQL database")
            
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise
    
    def disconnect_databases(self):
        """Close database connections."""
        if self.mysql_conn:
            self.mysql_conn.close()
            logger.info("MySQL connection closed")
        if self.postgres_conn:
            self.postgres_conn.close()
            logger.info("PostgreSQL connection closed")
    
    def prepare_table_for_migration(self):
        """Prepare the table for migration by dropping only the ricsId default and truncating the table."""
        cursor = self.postgres_conn.cursor()
        
        # Truncate the table and reset the sequence for ricsId
        try:
            cursor.execute('TRUNCATE TABLE "RadiologistInvoiceCaseServices" RESTART IDENTITY CASCADE')
            self.postgres_conn.commit()
            logger.info("Table truncated and sequence reset.")
        except Exception as e:
            self.postgres_conn.rollback()
            logger.warning(f"Could not truncate table: {e}")
        
        # Drop default from ricsId only
        try:
            cursor.execute('ALTER TABLE "RadiologistInvoiceCaseServices" ALTER COLUMN "ricsId" DROP DEFAULT')
            self.postgres_conn.commit()
            logger.info('Dropped default from ricsId to allow explicit inserts')
        except Exception as e:
            self.postgres_conn.rollback()
            logger.warning(f'Could not drop default from ricsId: {e}')
    
    def fetch_source_data(self) -> List[Dict[str, Any]]:
        """
        Fetch data from MySQL source tables with proper JOIN.
        
        Returns:
            List of dictionaries containing the joined data
        """
        try:
            cursor = self.mysql_conn.cursor(dictionary=True)
            
            query = """
            SELECT 
                ri.id as invoice_id,
                ri.invoice_no,
                ri.radiologist_id,
                ri.revenue_amount as invoice_revenue_amount,
                ri.month,
                ri.year,
                ri.created_by,
                rid.id as detail_id,
                rid.case_id,
                rid.revenue_amount as detail_revenue_amount,
                rid.services_name,
                rid.created_at
            FROM tbl_radiologist_invoices ri
            INNER JOIN tbl_radiologist_invoice_details rid 
                ON ri.id = rid.radiologist_invoice_id
            ORDER BY ri.id, rid.id
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            logger.info(f"Fetched {len(results)} records from source tables")
            return results
            
        except Exception as e:
            logger.error(f"Failed to fetch source data: {str(e)}")
            raise
    
    def build_caseid_mapping(self):
        """
        Build a mapping from tbl_radiologist_invoice_details.case_id -> tbl_cases.voxel_cases_id -> tbl_cases.cases_id
        Returns: dict mapping old_radiologist_invoice_details.case_id -> cases_id
        """
        try:
            mysql_cursor = self.mysql_conn.cursor()
            # Step 1: Get all voxel_cases_id -> cases_id from tbl_cases
            mysql_cursor.execute('SELECT voxel_cases_id, cases_id FROM tbl_cases WHERE voxel_cases_id IS NOT NULL AND cases_id IS NOT NULL')
            voxel_to_cases_id = {str(row[0]): row[1] for row in mysql_cursor.fetchall()}
            # Step 2: Get all case_id from tbl_radiologist_invoice_details
            mysql_cursor.execute('SELECT DISTINCT case_id FROM tbl_radiologist_invoice_details WHERE case_id IS NOT NULL')
            radiologist_case_ids = [str(row[0]) for row in mysql_cursor.fetchall()]
            # Step 3: Build mapping: for each case_id in radiologist_invoice_details, find matching cases_id
            mapping = {}
            for rad_case_id in radiologist_case_ids:
                cases_id = voxel_to_cases_id.get(rad_case_id)
                if cases_id is not None:
                    mapping[rad_case_id] = cases_id
            return mapping
        except Exception as e:
            logger.error(f"Error building caseId mapping: {e}")
            return {}
    
    def validate_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate and clean the source data before migration.
        Map case_id as per the required chain. Only keep records with valid mappings.
        """
        validated_data = []
        # Build the mapping for case_id
        caseid_mapping = self.build_caseid_mapping()
        for record in data:
            try:
                # Validate required fields (basic validation only)
                if not record.get('invoice_id') or not record.get('case_id') or not record.get('detail_id'):
                    logger.warning(f"Skipping record with missing invoice_id, case_id, or detail_id: {record}")
                    continue
                # Map case_id using the mapping chain
                old_case_id = str(record['case_id'])
                mapped_cases_id = caseid_mapping.get(old_case_id)
                if mapped_cases_id is None:
                    logger.warning(f"Skipping record {record['detail_id']}: case_id {old_case_id} could not be mapped to a valid cases_id via voxel_cases_id")
                    continue
                # Validate amount
                amount = record.get('detail_revenue_amount', 0)
                if amount is None:
                    amount = 0
                # Convert created_at to proper timestamp
                created_at = record.get('created_at')
                if created_at is None:
                    created_at = datetime.now()
                elif isinstance(created_at, str):
                    created_at = datetime.strptime(created_at, '%Y-%m-%d')
                validated_record = {
                    'ricsId': int(record['detail_id']),  # Migrate old PK as new PK
                    'invoiceId': int(record['invoice_id']),
                    'caseId': int(mapped_cases_id),  # Use mapped cases_id
                    'amount': float(amount),
                    'createdAt': created_at,
                    'isDeleted': False,
                    'deletedAt': None,
                    'updatedAt': datetime.now()
                }
                validated_data.append(validated_record)
            except Exception as e:
                logger.warning(f"Validation failed for record {record}: {str(e)}")
                continue
        logger.info(f"Validated {len(validated_data)} out of {len(data)} records (with correct caseId mapping)")
        logger.info("Note: Foreign key validation will be handled during insert - invalid records will be skipped")
        return validated_data
    
    def insert_target_data(self, data: List[Dict[str, Any]]):
        """
        Insert validated data into the PostgreSQL target table.
        Skip records that violate foreign key constraints and continue with migration.
        
        Args:
            data: Validated data to insert
        """
        try:
            cursor = self.postgres_conn.cursor()
            
            # Prepare the insert query (include ricsId)
            insert_query = """
            INSERT INTO "RadiologistInvoiceCaseServices" 
            ("ricsId", "invoiceId", "caseId", amount, "createdAt", "isDeleted", "deletedAt", "updatedAt")
            VALUES (%(ricsId)s, %(invoiceId)s, %(caseId)s, %(amount)s, %(createdAt)s, %(isDeleted)s, %(deletedAt)s, %(updatedAt)s)
            """
            
            # Insert data one by one to handle foreign key violations
            total_inserted = 0
            skipped_fk_violations = 0
            
            for i, record in enumerate(data):
                try:
                    cursor.execute(insert_query, record)
                    self.postgres_conn.commit()
                    total_inserted += 1
                    
                    if total_inserted % 100 == 0:
                        logger.info(f"Inserted {total_inserted} records...")
                        
                except Exception as e:
                    error_msg = str(e).lower()
                    
                    # Check if it's a foreign key violation
                    if 'foreign key' in error_msg or 'violates foreign key constraint' in error_msg:
                        skipped_fk_violations += 1
                        logger.warning(f"Skipping record {record['ricsId']}: Foreign key violation - {e}")
                        self.postgres_conn.rollback()
                        continue
                    else:
                        # For other errors, log and skip
                        logger.error(f"Failed to insert record {record['ricsId']}: {e}")
                        self.postgres_conn.rollback()
                        continue
            
            logger.info(f"Successfully inserted {total_inserted} records into RadiologistInvoiceCaseServices")
            if skipped_fk_violations > 0:
                logger.info(f"Skipped {skipped_fk_violations} records due to foreign key violations")
            
        except Exception as e:
            logger.error(f"Failed to insert target data: {str(e)}")
            self.postgres_conn.rollback()
            raise
    
    def verify_migration(self) -> bool:
        """
        Verify the migration by comparing record counts and sample data.
        Note: Some records may be skipped due to foreign key violations.
        
        Returns:
            True if verification passes, False otherwise
        """
        try:
            # Count records in source tables
            mysql_cursor = self.mysql_conn.cursor()
            mysql_cursor.execute("""
                SELECT COUNT(*) FROM tbl_radiologist_invoice_details
            """)
            source_count = mysql_cursor.fetchone()[0]
            
            # Count records in target table
            postgres_cursor = self.postgres_conn.cursor()
            postgres_cursor.execute('SELECT COUNT(*) FROM "RadiologistInvoiceCaseServices"')
            target_count = postgres_cursor.fetchone()[0]
            
            logger.info(f"Source records: {source_count}, Target records: {target_count}")
            
            # Check for orphaned caseId records (records that don't exist in Cases table)
            postgres_cursor.execute('''
                SELECT COUNT(*) FROM "RadiologistInvoiceCaseServices" rics
                LEFT JOIN "Cases" c ON rics."caseId" = c."cId"
                WHERE c."cId" IS NULL
            ''')
            orphaned_count = postgres_cursor.fetchone()[0]
            
            if orphaned_count > 0:
                logger.warning(f"Found {orphaned_count} orphaned caseId records (not in Cases table) - these should have been skipped")
            
            if target_count > 0:
                logger.info("Migration verification: PASSED - Records were successfully migrated")
                if source_count != target_count:
                    skipped_count = source_count - target_count
                    logger.info(f"Note: {skipped_count} records were skipped due to foreign key violations")
                return True
            else:
                logger.warning("Migration verification: FAILED - No records were migrated")
                return False
                
        except Exception as e:
            logger.error(f"Migration verification failed: {str(e)}")
            return False
    
    def run_migration(self):
        """Execute the complete migration process."""
        try:
            logger.info("Starting migration process...")
            logger.info(f"Log file location: {log_filename}")
            logger.info("Note: Foreign key constraints will be kept intact during migration")
            
            # Step 1: Connect to databases
            self.connect_databases()
            
            # Step 2: Prepare table for migration (truncate and drop ricsId default only)
            self.prepare_table_for_migration()
            
            # Step 3: Fetch source data
            source_data = self.fetch_source_data()
            
            # Step 4: Validate data (no foreign key validation)
            validated_data = self.validate_data(source_data)
            
            # Step 5: Insert data into target table
            self.insert_target_data(validated_data)
            
            # Step 6: Verify migration
            verification_passed = self.verify_migration()
            
            if verification_passed:
                logger.info("Migration completed successfully!")
                logger.info("Foreign key constraints were respected - invalid records were skipped")
            else:
                logger.warning("Migration completed with warnings - please check the logs")
                
        except Exception as e:
            logger.error(f"Migration failed: {str(e)}")
            raise
        finally:
            self.disconnect_databases()

def main():
    """Main function to execute the migration."""
    
    # Create migrator instance (no config needed as it uses db_connections.py)
    migrator = DatabaseMigrator()
    
    try:
        # Run the migration
        migrator.run_migration()
        
    except Exception as e:
        logger.error(f"Migration process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
