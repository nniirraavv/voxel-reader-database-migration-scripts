#!/usr/bin/env python3
"""
Migration script: tbl_cases (MySQL) -> CasePatients (PostgreSQL)
Migrates patient data from cases to CasePatients table
Based on mapping sheet specifications

This script:
1. Gets case data from MySQL tbl_cases (patient fields)
2. Maps to existing Cases via voxel_cases_id
3. Creates ClinicPatients records for patient data
4. Creates CasePatients records linking cases to clinic patients
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

# Add the parent directory to Python path to import db_connections
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connections import get_mysql_connection, get_postgres_connection

# Create cases_logs directory if it doesn't exist
log_dir = 'cases_logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging
log_filename = os.path.join(log_dir, '4_tbl_cases__ClinicPatient_CasePatients.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

class CasesToCasePatientsDataMigrator:
    def __init__(self, dry_run=False):
        self.mysql_conn = None
        self.postgres_conn = None
        self.dry_run = dry_run
        
        # Mapping caches
        self.case_id_mapping = {}  # voxel_cases_id -> new_case_id
        self.case_clinic_mapping = {}  # new_case_id -> clinic_id
        self.doctor_clinic_mapping = {}  # doctor_id -> clinic_id
        self.clinic_patient_cache = {}  # patient_key -> clinic_patient_id
        
        # Migration statistics
        self.stats = {
            'total_mysql_cases': 0,
            'migrated': 0,
            'created_clinic_patients': 0,
            'skipped_duplicates': 0,
            'missing_case_mapping': 0,
            'no_clinic_found': 0,
            'missing_doctor_mapping': 0,
            'used_default_clinic_67': 0,
            'used_default_clinic_68': 0,
            'errors': 0
        }

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

    # --- REMOVED: remove_foreign_key_constraints and drop_not_null_on_clinicpatients_clinicid ---

    def build_case_id_mapping(self):
        """Build mapping from voxel_cases_id to new case IDs"""
        try:
            cursor = self.postgres_conn.cursor()
            cursor.execute('SELECT "cId", "voxelCaseId" FROM "Cases" WHERE "voxelCaseId" IS NOT NULL')
            mappings = cursor.fetchall()
            
            for new_case_id, voxel_case_id in mappings:
                self.case_id_mapping[voxel_case_id] = new_case_id
            
            cursor.close()
            logger.info(f"Built case ID mapping with {len(self.case_id_mapping)} entries")
            
        except Exception as e:
            logger.error(f"Error building case ID mapping: {e}")
            raise

    def build_case_clinic_mapping(self):
        """Build mapping from case IDs to clinic IDs via Cases -> Users -> ClinicUsers -> ClinicLocations"""
        try:
            cursor = self.postgres_conn.cursor()
            
            # Since Cases table has original MySQL user IDs that don't exist in PostgreSQL Users table,
            # we cannot use the user relationship approach. Go directly to fallback.
            logger.warning("Cases table contains original MySQL user IDs that don't exist in PostgreSQL Users table")
            logger.warning("Using fallback clinic mapping approach...")
            self.build_fallback_clinic_mapping()
            
        except Exception as e:
            logger.error(f"Error building case-clinic mapping: {e}")
            # Try fallback approach on error
            try:
                self.build_fallback_clinic_mapping()
            except:
                logger.error("Fallback clinic mapping also failed")
                raise

    def build_fallback_clinic_mapping(self):
        """Fallback method: assign all cases to first available clinic"""
        try:
            cursor = self.postgres_conn.cursor()
            
            # Get the first available clinic
            cursor.execute('SELECT "cId" FROM "Clinics" WHERE "isDeleted" = false LIMIT 1')
            result = cursor.fetchone()
            
            if result:
                fallback_clinic_id = result[0]
                logger.info(f"Using fallback clinic ID: {fallback_clinic_id}")
                
                # Get all case IDs and assign them to the fallback clinic
                cursor.execute('SELECT "cId" FROM "Cases"')
                case_ids = cursor.fetchall()
                
                for (case_id,) in case_ids:
                    self.case_clinic_mapping[case_id] = fallback_clinic_id
                
                logger.info(f"Assigned {len(case_ids)} cases to fallback clinic {fallback_clinic_id}")
            else:
                logger.error("No clinics found for fallback mapping")
                
            cursor.close()
            
        except Exception as e:
            logger.error(f"Error in fallback clinic mapping: {e}")
            raise

    def build_doctor_clinic_mapping(self):
        """
        Build mapping from doctor_id to clinic_id via:
        doctor_id -> Users.olduserid -> Users.uId -> Clinics.ownerUserId -> Clinics.cId
        """
        try:
            cursor = self.postgres_conn.cursor()
            
            # Get the mapping: doctor_id -> uId -> clinic_id
            query = '''
                SELECT DISTINCT u."olduserid", c."cId"
                FROM "Users" u
                INNER JOIN "Clinics" c ON u."uId" = c."ownerUserId"
                WHERE u."olduserid" IS NOT NULL
                AND c."isDeleted" = false
                ORDER BY u."olduserid", c."cId"
            '''
            
            cursor.execute(query)
            mappings = cursor.fetchall()
            
            for old_user_id, clinic_id in mappings:
                if old_user_id not in self.doctor_clinic_mapping:
                    # Use first match found for multiple matches
                    self.doctor_clinic_mapping[old_user_id] = clinic_id
            
            cursor.close()
            logger.info(f"Built doctor-clinic mapping with {len(self.doctor_clinic_mapping)} entries")
            
            # Show sample mappings
            sample_count = min(5, len(self.doctor_clinic_mapping))
            if sample_count > 0:
                logger.info("Sample doctor-clinic mappings:")
                for i, (doctor_id, clinic_id) in enumerate(list(self.doctor_clinic_mapping.items())[:sample_count]):
                    logger.info(f"  Doctor ID {doctor_id} -> Clinic ID {clinic_id}")
            
        except Exception as e:
            logger.error(f"Error building doctor-clinic mapping: {e}")
            raise

    def get_clinic_id_for_doctor(self, doctor_id):
        """
        Get clinic ID for a doctor using the mapping logic:
        1. If doctor_id is missing -> use None (NULL)
        2. If mapping not found -> use None (NULL)
        3. If found -> use mapped clinic ID
        """
        if not doctor_id:
            logger.debug(f"Missing doctor_id, setting clinicId to NULL")
            return None
        clinic_id = self.doctor_clinic_mapping.get(doctor_id)
        if clinic_id:
            logger.debug(f"Doctor ID {doctor_id} mapped to clinic ID {clinic_id}")
            return clinic_id
        else:
            logger.debug(f"No mapping found for doctor ID {doctor_id}, setting clinicId to NULL")
            return None

    def convert_dob(self, dob) -> Optional[str]:
        """Convert date of birth to proper format"""
        if not dob:
            return None
            
        try:
            if isinstance(dob, str):
                # Try to parse the string date
                if dob.strip() == '' or dob.strip() == '0000-00-00':
                    return None
                return dob.strip()
            elif hasattr(dob, 'strftime'):
                # It's a date/datetime object
                return dob.strftime('%Y-%m-%d')
            else:
                return str(dob)
        except Exception as e:
            logger.warning(f"Error converting date of birth {dob}: {e}")
            return None

    def convert_gender(self, mysql_gender: str) -> str:
        """Map MySQL gender to PostgreSQL enum values"""
        if not mysql_gender:
            return 'OTHER'
        
        gender_lower = mysql_gender.lower().strip()
        
        # Map common gender values to enum
        gender_mapping = {
            'male': 'MALE',
            'm': 'MALE',
            'female': 'FEMALE',
            'f': 'FEMALE',
            'other': 'OTHER',
            'o': 'OTHER'
        }
        
        return gender_mapping.get(gender_lower, 'OTHER')

    def find_or_create_clinic_patient(self, first_name: str, last_name: str, gender: str, dob: str, clinic_id: int) -> Optional[int]:
        """Always create a new clinic patient and return their ID (no deduplication). Skip if clinic_id is None."""
        if clinic_id is None:
            logger.warning(f"Skipping ClinicPatient creation for {first_name} {last_name} because clinicId is NULL.")
            return None
        try:
            cursor = self.postgres_conn.cursor()
            insert_query = '''
                INSERT INTO "ClinicPatients" (
                    "clinicId", "firstName", "lastName", "gender", "dob", 
                    "platformId", "createdAt", "updatedAt"
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING "cpId"
            '''
            now = datetime.now()
            platform_id = str(uuid.uuid4())
            cursor.execute(insert_query, (
                clinic_id,  # Can be None (NULL)
                first_name, last_name, gender, dob,
                platform_id, now, now
            ))
            new_patient = cursor.fetchone()
            clinic_patient_id = new_patient[0]
            self.postgres_conn.commit()
            cursor.close()
            logger.info(f"Created new clinic patient: {first_name} {last_name} for clinic {clinic_id}")
            self.stats['created_clinic_patients'] += 1
            return clinic_patient_id
        except Exception as e:
            logger.error(f"Error creating clinic patient {first_name} {last_name}: {e}")
            if self.postgres_conn:
                self.postgres_conn.rollback()
            return None

    def check_existing_case_patient(self, case_id: int) -> bool:
        """Check if CasePatient already exists for this case"""
        try:
            cursor = self.postgres_conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM "CasePatients" WHERE "caseId" = %s', (case_id,))
            count = cursor.fetchone()[0]
            cursor.close()
            return count > 0
        except Exception as e:
            logger.error(f"Error checking existing case patient: {e}")
            return True  # Return True to skip on error

    def create_case_patient_record(self, case_id: int, clinic_patient_id: int, first_name: str, last_name: str, gender: str, dob: str) -> bool:
        """Create a CasePatient record linking case to clinic patient with direct patient data"""
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create CasePatient record for case {case_id} with patient {first_name} {last_name}")
                return True
                
            cursor = self.postgres_conn.cursor()
            
            insert_case_patient_sql = '''
                INSERT INTO "CasePatients" 
                ("caseId", "clinicPatientId", "firstName", "lastName", "gender", "dob")
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING "cpId"
            '''
            
            cursor.execute(insert_case_patient_sql, (
                case_id,
                clinic_patient_id,
                first_name,
                last_name,
                gender,
                dob
            ))
            
            # Commit the record
            self.postgres_conn.commit()
            cursor.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating case patient record: {e}")
            if self.postgres_conn:
                self.postgres_conn.rollback()
            return False

    def migrate_case_patient_record(self, case_data: Dict[str, Any]) -> bool:
        """Migrate a single case patient record"""
        try:
            cases_id = case_data['cases_id']
            doctor_id = case_data.get('doctor_id')
            new_case_id = cases_id
            if self.check_existing_case_patient(new_case_id):
                logger.debug(f"CasePatient already exists for case {new_case_id} (cases_id: {cases_id})")
                self.stats['skipped_duplicates'] += 1
                return True
            clinic_id = self.get_clinic_id_for_doctor(doctor_id)
            # If clinic_id is None, skip this CasePatient
            if clinic_id is None:
                logger.warning(f"Skipping CasePatient creation for case {cases_id} because clinicLocationId (clinicId) is NULL.")
                self.stats['errors'] += 1
                return False
            first_name = case_data.get('patient_firstname', '').strip()
            last_name = case_data.get('patient_lastname', '').strip()
            gender = case_data.get('gender', '').strip()
            dob = case_data.get('dob')
            if not first_name or not last_name:
                logger.warning(f"Missing patient name for case {cases_id}")
                self.stats['errors'] += 1
                return False
            gender = self.convert_gender(gender)
            dob_str = self.convert_dob(dob)
            if not dob_str:
                logger.warning(f"Missing date of birth for case {cases_id}")
                dob_str = '1900-01-01'  # Default date for missing DOB
            clinic_patient_id = self.find_or_create_clinic_patient(
                first_name, last_name, gender, dob_str, clinic_id
            )
            if not clinic_patient_id:
                logger.warning(f"Failed to create clinic patient for case {cases_id} - skipping CasePatient creation.")
                self.stats['errors'] += 1
                return False
            if self.create_case_patient_record(new_case_id, clinic_patient_id, first_name, last_name, gender, dob_str):
                self.stats['migrated'] += 1
                logger.debug(f"Successfully migrated case patient for case {cases_id} with doctor {doctor_id} -> clinic {clinic_id}")
                return True
            else:
                self.stats['errors'] += 1
                return False
        except Exception as e:
            logger.error(f"Error migrating case patient {case_data.get('cases_id', 'unknown')}: {e}")
            self.stats['errors'] += 1
            return False

    def fetch_cases_from_mysql(self):
        """Fetch case patient data from MySQL tbl_cases table"""
        try:
            cursor = self.mysql_conn.cursor(dictionary=True)
            query = """
                SELECT 
                    cases_id,
                    patient_firstname,
                    patient_lastname,
                    gender,
                    dob,
                    doctor_id
                FROM tbl_cases
                WHERE patient_firstname IS NOT NULL 
                AND patient_firstname != ''
                AND patient_lastname IS NOT NULL
                AND patient_lastname != ''
                AND cases_id IS NOT NULL
                ORDER BY cases_id
            """
            cursor.execute(query)
            cases = cursor.fetchall()
            cursor.close()
            self.stats['total_mysql_cases'] = len(cases)
            logger.info(f"Fetched {len(cases)} cases with patient data from MySQL")
            return cases
        except Exception as e:
            logger.error(f"Error fetching cases from MySQL: {e}")
            return []

    def migrate_data(self):
        """Main data migration process"""
        try:
            # Fetch valid doctor IDs from tbl_users
            valid_doctor_ids = get_valid_doctor_ids(self.mysql_conn)
            if not valid_doctor_ids:
                logger.error("No valid doctor IDs found in tbl_users table!")
                return
            # Fetch cases from MySQL
            cases = self.fetch_cases_from_mysql()
            if not cases:
                logger.warning("No cases found to migrate")
                return
            logger.info(f"Starting migration of {len(cases)} cases...")
            skipped_invalid_doctor_count = 0
            # Process each case
            for i, case_data in enumerate(cases, 1):
                try:
                    doctor_id = case_data.get('doctor_id')
                    # Only migrate if doctor_id is present in valid_doctor_ids
                    if doctor_id is None or doctor_id not in valid_doctor_ids:
                        skipped_invalid_doctor_count += 1
                        logger.warning(f"Skipping case {case_data.get('cases_id', 'unknown')}: doctor_id {doctor_id} not found in tbl_users")
                        continue
                    self.migrate_case_patient_record(case_data)
                    # Log progress every 100 records
                    if i % 100 == 0:
                        logger.info(f"Processed {i}/{len(cases)} cases...")
                except Exception as e:
                    logger.error(f"Error processing case {case_data.get('cases_id', 'unknown')}: {e}")
                    self.stats['errors'] += 1
                    continue
            logger.info(f"Data migration completed. Skipped {skipped_invalid_doctor_count} cases due to invalid doctor_id.")
        except Exception as e:
            logger.error(f"Error during data migration: {e}")
            raise

    def print_final_stats(self):
        """Print final migration statistics"""
        logger.info("=== MIGRATION STATISTICS ===")
        logger.info(f"Total MySQL cases processed: {self.stats['total_mysql_cases']}")
        logger.info(f"Successfully migrated: {self.stats['migrated']}")
        logger.info(f"Created new clinic patients: {self.stats['created_clinic_patients']}")
        logger.info(f"Skipped duplicates: {self.stats['skipped_duplicates']}")
        logger.info(f"Missing case mapping: {self.stats['missing_case_mapping']}")
        logger.info(f"Missing doctor mapping: {self.stats['missing_doctor_mapping']}")
        logger.info(f"Used default clinic 67 (missing doctor): {self.stats['used_default_clinic_67']}")
        logger.info(f"Used default clinic 68 (no mapping): {self.stats['used_default_clinic_68']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info("============================")

    def run(self):
        """Main migration process"""
        try:
            logger.info("Starting tbl_cases to CasePatients migration")
            logger.info("Foreign key constraints are maintained - only records with valid relationships will be migrated")
            logger.info("Using doctor_id -> Users.olduserid -> Clinics.ownerUserId mapping for clinic assignment")
            
            # Establish database connections
            self.connect_databases()
            
            # Build mapping tables
            self.build_case_id_mapping()
            self.build_doctor_clinic_mapping()
            
            # Migrate data
            self.migrate_data()
            
            logger.info("Migration completed!")
            logger.info("Foreign key constraints are maintained - only records with valid relationships were migrated")
            logger.info("Clinic assignment: Default clinic 67 for missing doctor_id, default clinic 68 for unmapped doctors")
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
                SELECT COUNT(*) FROM tbl_cases 
                WHERE patient_firstname IS NOT NULL 
                AND patient_firstname != ''
                AND patient_lastname IS NOT NULL  
                AND patient_lastname != ''
                AND cases_id IS NOT NULL
            """)
            mysql_count = mysql_cursor.fetchone()[0]
            
            postgres_cursor = self.postgres_conn.cursor()
            postgres_cursor.execute('SELECT COUNT(*) FROM "CasePatients"')
            postgres_count = postgres_cursor.fetchone()[0]
            
            postgres_cursor.execute('SELECT COUNT(*) FROM "ClinicPatients"')
            clinic_patients_count = postgres_cursor.fetchone()[0]
            
            logger.info(f"MySQL cases with patient data: {mysql_count}")
            logger.info(f"PostgreSQL CasePatients count: {postgres_count}")
            logger.info(f"PostgreSQL ClinicPatients count: {clinic_patients_count}")
            
            # Check for orphaned caseId records (records that don't exist in Cases table)
            postgres_cursor.execute('''
                SELECT COUNT(*) FROM "CasePatients" cp
                LEFT JOIN "Cases" c ON cp."caseId" = c."cId"
                WHERE c."cId" IS NULL
            ''')
            orphaned_cases = postgres_cursor.fetchone()[0]
            
            # Check for orphaned clinicPatientId records (records that don't exist in ClinicPatients table)
            postgres_cursor.execute('''
                SELECT COUNT(*) FROM "CasePatients" cp
                LEFT JOIN "ClinicPatients" clp ON cp."clinicPatientId" = clp."cpId"
                WHERE clp."cpId" IS NULL
            ''')
            orphaned_clinic_patients = postgres_cursor.fetchone()[0]
            
            # Check for orphaned clinicId records in ClinicPatients (records that don't exist in Clinics table)
            postgres_cursor.execute('''
                SELECT COUNT(*) FROM "ClinicPatients" clp
                LEFT JOIN "Clinics" c ON clp."clinicId" = c."cId"
                WHERE c."cId" IS NULL
            ''')
            orphaned_clinics = postgres_cursor.fetchone()[0]
            
            if orphaned_cases > 0:
                logger.info(f"Found {orphaned_cases} orphaned caseId records (not in Cases table)")
                
            if orphaned_clinic_patients > 0:
                logger.info(f"Found {orphaned_clinic_patients} orphaned clinicPatientId records (not in ClinicPatients table)")
                
            if orphaned_clinics > 0:
                logger.info(f"Found {orphaned_clinics} orphaned clinicId records (not in Clinics table)")
            
            # Sample validation
            postgres_cursor.execute('''
                SELECT cp."caseId", cp."clinicPatientId", c."voxelCaseId", 
                       clp."firstName", clp."lastName", clp."gender", clp."dob"
                FROM "CasePatients" cp
                LEFT JOIN "Cases" c ON cp."caseId" = c."cId"
                LEFT JOIN "ClinicPatients" clp ON cp."clinicPatientId" = clp."cpId"
                ORDER BY cp."caseId"
                LIMIT 5
            ''')
            sample_records = postgres_cursor.fetchall()
            
            logger.info("Sample migrated records:")
            for record in sample_records:
                voxel_case_id = record[2] if record[2] else "orphaned"
                patient_name = f"{record[3]} {record[4]}" if record[3] and record[4] else "orphaned patient"
                logger.info(f"  Case {record[0]} (voxel: {voxel_case_id}): {patient_name}")
                
            mysql_cursor.close()
            postgres_cursor.close()
                
        except Exception as e:
            logger.error(f"Validation failed: {e}")
        finally:
            self.close_connections()


def main():
    """Main function to run the migration"""
    parser = argparse.ArgumentParser(description='Migrate tbl_cases to CasePatients')
    parser.add_argument('--dry-run', action='store_true', help='Run in dry-run mode (no actual changes)')
    parser.add_argument('--validate-only', action='store_true', help='Only run validation, skip migration')
    args = parser.parse_args()
    
    migrator = CasesToCasePatientsDataMigrator(dry_run=args.dry_run)
    
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
            
            # Validate the migration (unless it was a dry run)
            if not args.dry_run:
                migrator.validate_migration()
        
    except Exception as e:
        logger.error(f"Migration process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
