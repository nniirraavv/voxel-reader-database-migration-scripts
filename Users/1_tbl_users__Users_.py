import uuid
import logging
import os
import sys
from datetime import datetime

# Add parent directory to path to import db_connections
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connections import get_mysql_connection, get_postgres_connection
import psycopg2.extras

DEFAULT_USER_TYPE = "CLINIC_USERS"  # Updated to use the new enum value

# Setup logging
def setup_logging():
    """Setup logging configuration"""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create logs directory relative to script location
    log_dir = os.path.join(script_dir, "user_log")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"1_tbl_users__Users.log")  # Added timestamp to filename
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()  # Also log to console
        ],
        force=True  # Force reconfiguration if logging was already configured
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_filename}")
    return logger

def fetch_mysql_users():
    logger = logging.getLogger(__name__)
    logger.info("Starting to fetch users from MySQL database")
    
    conn = get_mysql_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT 
                user_id, voxel_doctors_id, title, fname, lname, email, 
                contact, contact_no, status, add_time, update_time 
            FROM tbl_users
        """)
        users = cursor.fetchall()
        logger.info(f"Successfully fetched {len(users)} users from MySQL")
        return users
    except Exception as e:
        logger.error(f"Error fetching users from MySQL: {str(e)}")
        raise
    finally:
        conn.close()
        logger.debug("MySQL connection closed")

def transform_users(mysql_users):
    logger = logging.getLogger(__name__)
    logger.info(f"Starting transformation of {len(mysql_users)} users")
    
    transformed = []
    invalid_users = []

    for user in mysql_users:
        email = user.get("email")
        first_name = user.get("fname") or "Unknown"
        last_name = user.get("lname") or ""
        old_user_id = user.get("user_id")
        
        if not email or not first_name:
            invalid_users.append({
                'old_user_id': old_user_id,
                'email': email,
                'first_name': first_name,
                'reason': 'Missing email or first name'
            })
            logger.warning(f"Skipping invalid user ID {old_user_id}: missing email or first name")
            continue

        sub = str(uuid.uuid4())
        name_title = user.get("title") or None
        status = bool(user.get("status", 0))  # convert tinyint to bool
        is_deleted = False
        created_at = user.get("add_time") or datetime.now()
        updated_at = user.get("update_time") or datetime.now()
        mobile = str(user.get("contact") or user.get("contact_no") or "")

        transformed.append((
            sub,
            DEFAULT_USER_TYPE,
            email,
            name_title,
            first_name,
            last_name,
            status,
            is_deleted,
            created_at,
            updated_at,
            mobile,
            old_user_id
        ))
        
        logger.debug(f"Transformed user {old_user_id}: {email} ({first_name} {last_name})")

    logger.info(f"Transformation completed: {len(transformed)} valid users, {len(invalid_users)} invalid users")
    
    if invalid_users:
        logger.warning("Invalid users summary:")
        for invalid in invalid_users:
            logger.warning(f"  - ID {invalid['old_user_id']}: {invalid['email']} - {invalid['reason']}")

    return transformed

def insert_postgres_users(user_data_list):
    logger = logging.getLogger(__name__)
    logger.info(f"Starting update of olduserid for {len(user_data_list)} users in PostgreSQL (no new inserts)")
    
    conn = get_postgres_connection()
    updated_count = 0
    updated_users = []
    
    try:
        with conn.cursor() as cursor:
            for user in user_data_list:
                email = user[2]  # email is 3rd in tuple
                first_name = user[4]  # firstName is 5th in tuple
                last_name = user[5]  # lastName is 6th in tuple
                old_user_id = user[11]  # oldUserId is 12th in tuple
                
                # Prepare lowercased, trimmed values
                email_lc = email.strip().lower()
                first_name_lc = first_name.strip().lower()
                last_name_lc = last_name.strip().lower()

                # Check for existing user using case-insensitive, trimmed match
                cursor.execute('''
                    SELECT "uId", sub, "olduserid" FROM "Users" 
                    WHERE LOWER(TRIM(email)) = %s AND LOWER(TRIM("firstName")) = %s AND LOWER(TRIM("lastName")) = %s
                ''', (email_lc, first_name_lc, last_name_lc))
                
                existing_user = cursor.fetchone()
                if existing_user:
                    existing_uid = existing_user[0]
                    existing_sub = existing_user[1]
                    existing_olduserid = existing_user[2]
                    
                    # Update the olduserid for the existing user
                    cursor.execute('''
                        UPDATE "Users" 
                        SET "olduserid" = %s, "updatedAt" = %s
                        WHERE LOWER(TRIM(email)) = %s AND LOWER(TRIM("firstName")) = %s AND LOWER(TRIM("lastName")) = %s
                    ''', (old_user_id, datetime.now(), email_lc, first_name_lc, last_name_lc))
                    
                    updated_users.append({
                        'old_user_id': old_user_id,
                        'email': email,
                        'first_name': first_name,
                        'last_name': last_name,
                        'existing_uid': existing_uid,
                        'existing_sub': existing_sub,
                        'previous_olduserid': existing_olduserid
                    })
                    logger.info(f"✅ Updated existing user: {email} ({first_name} {last_name}) - uId: {existing_uid}, set olduserid: {old_user_id}")
                    updated_count += 1
                # No else: do nothing if user does not exist

        conn.commit()
        logger.info(f"✅ Transaction committed successfully")
        logger.info(f"📊 Update summary: {updated_count} users updated (no new users inserted)")
        
        # Log detailed summary
        if updated_users:
            logger.info("=== UPDATED USERS SUMMARY ===")
            for user in updated_users:
                prev_old_id = user['previous_olduserid'] or 'NULL'
                logger.info(f"uId {user['existing_uid']} | {user['email']} ({user['first_name']} {user['last_name']}) - olduserid: {prev_old_id} -> {user['old_user_id']}")
                
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error during update, transaction rolled back: {str(e)}")
        logger.error(f"Failed after processing {updated_count} users")
        raise
    finally:
        conn.close()
        logger.info("PostgreSQL connection closed")

if __name__ == "__main__":
    # Initialize logging
    logger = setup_logging()
    
    try:
        logger.info("🔄 Starting full user data migration from MySQL → PostgreSQL")
        start_time = datetime.now()

        mysql_users = fetch_mysql_users()
        logger.info(f"🧑‍💻 Users fetched from MySQL: {len(mysql_users)}")

        user_data_to_insert = transform_users(mysql_users)
        logger.info(f"🚀 Transformed {len(user_data_to_insert)} users for insertion")

        insert_postgres_users(user_data_to_insert)
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"⏱️ Migration completed in {duration.total_seconds():.2f} seconds")
        logger.info("🎉 User migration process completed successfully!")
        
    except Exception as e:
        logger.error(f"💥 Migration failed with error: {str(e)}")
        logger.error("❌ User migration process failed!")
        raise
