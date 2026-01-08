#!/usr/bin/env python3
"""
Migration script: tbl_radiologist (MySQL) -> Users (PostgreSQL)
Updates existing radiologist records in Users table with oldUserId from tbl_radiologist
"""

import mysql.connector
import psycopg2
import uuid
from datetime import datetime
import logging
from typing import Dict, Any, Optional
import sys
import os

# Add the parent directory to Python path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connections import get_mysql_connection, get_postgres_connection

# Setup logging directory relative to script location
script_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(script_dir, "user_log")
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Create log filename with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = os.path.join(log_dir, f"2_tbl_radiologist__Users.log")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ],
    force=True  # Force reconfiguration if logging was already configured
)
logger = logging.getLogger(__name__)

class RadiologistToUsersDataUpdater:
    def __init__(self):
        self.mysql_conn = None
        self.postgres_conn = None
        self.stats = {
            'total_mysql_records': 0,
            'total_postgres_radiologists': 0,
            'updated': 0,
            'not_found': 0,
            'already_has_old_id': 0,
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

    def disconnect_databases(self):
        """Close database connections"""
        if self.mysql_conn:
            self.mysql_conn.close()
        if self.postgres_conn:
            self.postgres_conn.close()
        logger.info("Database connections closed")

    def find_radiologist_by_email(self, email: str) -> Optional[int]:
        """Find radiologist in Users table by email and return their uId"""
        try:
            cursor = self.postgres_conn.cursor()
            logger.info(f"Searching for radiologist in Users table with email: {email}")
            cursor.execute("""
                SELECT "uId", "olduserid" FROM "Users" 
                WHERE LOWER(email) = LOWER(%s) AND "userType" = 'RADIOLOGIST'
            """, (email,))
            result = cursor.fetchone()
            cursor.close()
            if result:
                u_id, old_user_id = result
                if old_user_id is not None:
                    logger.info(f"Radiologist {email} already has oldUserId: {old_user_id}")
                    self.stats['already_has_old_id'] += 1
                    return None
                logger.info(f"Radiologist {email} found in Users table with uId: {u_id}")
                return u_id
            else:
                logger.warning(f"Radiologist with email {email} not found in Users table (case-insensitive match)")
                self.stats['not_found'] += 1
                return None
        except Exception as e:
            logger.error(f"Error finding radiologist by email {email}: {e}")
            return None

    def find_radiologist_by_email_or_name(self, email: str, fname: str, lname: str) -> Optional[tuple]:
        """Find radiologist in Users table by email, or by first and last name (case-insensitive), and return their uId and current oldUserId"""
        try:
            cursor = self.postgres_conn.cursor()
            # Strip whitespace
            email_clean = email.strip() if email else ''
            fname_clean = fname.strip() if fname else ''
            lname_clean = lname.strip() if lname else ''
            # Try email match first
            logger.info(f"Searching for radiologist in Users table with email: '{email_clean}' (original: '{email}')")
            cursor.execute('''
                SELECT "uId", "olduserid" FROM "Users" 
                WHERE LOWER(email) = LOWER(%s) AND "userType" = 'RADIOLOGIST'
            ''', (email_clean,))
            result = cursor.fetchone()
            if result:
                cursor.close()
                u_id, old_user_id = result
                logger.info(f"Radiologist {email_clean} found in Users table with uId: {u_id}, current oldUserId: {old_user_id}")
                return (u_id, old_user_id)
            # Try name match if email fails
            logger.info(f"Email match failed for '{email_clean}'. Trying first name: '{fname_clean}', last name: '{lname_clean}' (original: '{fname}', '{lname}')")
            cursor.execute('''
                SELECT "uId", "olduserid" FROM "Users"
                WHERE LOWER(TRIM("firstName")) = LOWER(%s) AND LOWER(TRIM("lastName")) = LOWER(%s) AND "userType" = 'RADIOLOGIST'
            ''', (fname_clean, lname_clean))
            result = cursor.fetchone()
            cursor.close()
            if result:
                u_id, old_user_id = result
                logger.info(f"Radiologist {fname_clean} {lname_clean} found in Users table with uId: {u_id}, current oldUserId: {old_user_id}")
                return (u_id, old_user_id)
            else:
                logger.warning(f"Radiologist with email '{email_clean}' or name '{fname_clean} {lname_clean}' not found in Users table")
                self.stats['not_found'] += 1
                return None
        except Exception as e:
            logger.error(f"Error finding radiologist by email or name: {e}")
            return None

    def update_radiologist_old_id(self, u_id: int, old_user_id: int, email: str) -> bool:
        """Update the oldUserId for a radiologist in Users table"""
        try:
            cursor = self.postgres_conn.cursor()
            logger.info(f"Attempting to update oldUserId for email: '{email}', uId: {u_id}, oldUserId: {old_user_id}")
            update_query = """
                UPDATE "Users" 
                SET "olduserid" = %s, "updatedAt" = %s
                WHERE "uId" = %s AND "userType" = 'RADIOLOGIST'
            """
            cursor.execute(update_query, (old_user_id, datetime.now(), u_id))
            rows_affected = cursor.rowcount
            cursor.close()
            logger.info(f"Rows affected by update: {rows_affected}")
            if rows_affected > 0:
                logger.info(f"Successfully updated radiologist {email} (uId: {u_id}) with oldUserId: {old_user_id}")
                self.stats['updated'] += 1
                return True
            else:
                logger.error(f"No rows updated for radiologist {email} (uId: {u_id}) - check if userType or uId mismatch")
                self.stats['errors'] += 1
                return False
        except Exception as e:
            logger.error(f"Error updating radiologist {email} with oldUserId {old_user_id}: {e}")
            self.stats['errors'] += 1
            return False

    def fetch_radiologists_from_mysql(self):
        """Fetch all radiologists from MySQL tbl_radiologist table"""
        try:
            cursor = self.mysql_conn.cursor(dictionary=True)
            
            query = """
                SELECT 
                    radiologist_id,
                    email,
                    fname,
                    lname
                FROM tbl_radiologist
                ORDER BY radiologist_id
            """
            
            cursor.execute(query)
            radiologists = cursor.fetchall()
            cursor.close()
            
            logger.info(f"Fetched {len(radiologists)} radiologists from MySQL")
            return radiologists
            
        except Exception as e:
            logger.error(f"Error fetching radiologists from MySQL: {e}")
            return []

    def get_postgres_radiologist_count(self):
        """Get count of radiologists in PostgreSQL Users table"""
        try:
            cursor = self.postgres_conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM "Users" WHERE "userType" = \'RADIOLOGIST\'')
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logger.error(f"Error getting PostgreSQL radiologist count: {e}")
            return 0

    def run_update(self):
        """Execute the complete update process"""
        try:
            logger.info("Starting tbl_radiologist oldUserId update process")
            # Connect to databases
            self.connect_databases()
            # Get counts for statistics
            self.stats['total_postgres_radiologists'] = self.get_postgres_radiologist_count()
            logger.info(f"Found {self.stats['total_postgres_radiologists']} radiologists in PostgreSQL Users table")
            # Fetch radiologists from MySQL
            radiologists = self.fetch_radiologists_from_mysql()
            self.stats['total_mysql_records'] = len(radiologists)
            if not radiologists:
                logger.warning("No radiologists found in MySQL database")
                return
            # Process each radiologist
            for radiologist in radiologists:
                email = radiologist['email']
                old_user_id = radiologist['radiologist_id']
                fname = radiologist.get('fname', '')
                lname = radiologist.get('lname', '')
                # Find the radiologist in PostgreSQL Users table by email or name
                match = self.find_radiologist_by_email_or_name(email, fname, lname)
                if match:
                    u_id, prev_old_user_id = match
                    logger.info(f"Updating oldUserId for uId: {u_id} from {prev_old_user_id} to {old_user_id}")
                    self.update_radiologist_old_id(u_id, old_user_id, email)
                    # Commit after each successful update
                    self.postgres_conn.commit()
            # Final commit
            self.postgres_conn.commit()
            # Print update statistics
            logger.info("Update process completed!")
            logger.info(f"Total MySQL records: {self.stats['total_mysql_records']}")
            logger.info(f"Total PostgreSQL radiologists: {self.stats['total_postgres_radiologists']}")
            logger.info(f"Successfully updated: {self.stats['updated']}")
            logger.info(f"Not found in PostgreSQL: {self.stats['not_found']}")
            logger.info(f"Errors: {self.stats['errors']}")
        except Exception as e:
            logger.error(f"Update process failed: {e}")
            if self.postgres_conn:
                self.postgres_conn.rollback()
            raise
        finally:
            self.disconnect_databases()

    def validate_update(self):
        """Validate the update by checking how many radiologists now have oldUserId"""
        try:
            logger.info("Validating update...")
            
            # Connect to databases
            self.connect_databases()
            
            # Count radiologists with oldUserId in PostgreSQL
            postgres_cursor = self.postgres_conn.cursor()
            postgres_cursor.execute('''
                SELECT COUNT(*) FROM "Users" 
                WHERE "userType" = 'RADIOLOGIST' AND "olduserid" IS NOT NULL
            ''')
            postgres_with_old_id = postgres_cursor.fetchone()[0]
            
            # Count total radiologists in PostgreSQL
            postgres_cursor.execute('SELECT COUNT(*) FROM "Users" WHERE "userType" = \'RADIOLOGIST\'')
            postgres_total = postgres_cursor.fetchone()[0]
            postgres_cursor.close()
            
            # Count radiologists in MySQL
            mysql_cursor = self.mysql_conn.cursor()
            mysql_cursor.execute("SELECT COUNT(*) FROM tbl_radiologist")
            mysql_count = mysql_cursor.fetchone()[0]
            mysql_cursor.close()
            
            logger.info(f"MySQL tbl_radiologist count: {mysql_count}")
            logger.info(f"PostgreSQL Users (RADIOLOGIST) total: {postgres_total}")
            logger.info(f"PostgreSQL Users (RADIOLOGIST) with oldUserId: {postgres_with_old_id}")
            
            if postgres_with_old_id == mysql_count:
                logger.info("✅ Validation successful: All radiologists have been updated with oldUserId")
            elif postgres_with_old_id > 0:
                logger.warning(f"⚠️ Partial update: {postgres_with_old_id}/{mysql_count} radiologists updated")
            else:
                logger.error("❌ Validation failed: No radiologists were updated")
                
        except Exception as e:
            logger.error(f"Validation failed: {e}")
        finally:
            self.disconnect_databases()

def main():
    """Main function to run the update"""
    updater = RadiologistToUsersDataUpdater()
    
    try:
        # Run the update
        updater.run_update()
        
        # Validate the update
        updater.validate_update()
        
    except Exception as e:
        logger.error(f"Update process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
