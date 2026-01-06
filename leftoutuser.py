#!/usr/bin/env python3
"""
Script to find users from the old database that are missing in the new database.
This helps identify which users need to be added to the new database before migration.
"""

import mysql.connector
import psycopg2
import logging
from datetime import datetime
import os
import sys

# Add the parent directory to Python path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connections import get_mysql_connection, get_postgres_connection

# Setup logging
def setup_logging():
    """Setup logging configuration"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = os.path.join(log_dir, f"missing_users_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ],
        force=True
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_filename}")
    return logger

def get_old_database_users():
    """Get all user IDs from the old MySQL database"""
    logger = logging.getLogger(__name__)
    logger.info("Fetching users from old MySQL database...")
    
    try:
        mysql_conn = get_mysql_connection()
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        
        # Get users from tbl_users
        mysql_cursor.execute("""
            SELECT user_id, fname, lname, email, title, status, add_time
            FROM tbl_users 
            WHERE user_id IS NOT NULL
            ORDER BY user_id
        """)
        tbl_users = mysql_cursor.fetchall()
        logger.info(f"Found {len(tbl_users)} users in tbl_users")
        
        # Get radiologists from tbl_radiologist
        mysql_cursor.execute("""
            SELECT radiologist_id, fname, lname, email, status, add_time
            FROM tbl_radiologist 
            WHERE radiologist_id IS NOT NULL
            ORDER BY radiologist_id
        """)
        tbl_radiologists = mysql_cursor.fetchall()
        logger.info(f"Found {len(tbl_radiologists)} radiologists in tbl_radiologist")
        
        # Combine all users from old database
        old_users = {}
        
        # Add tbl_users
        for user in tbl_users:
            user_id = user['user_id']
            old_users[user_id] = {
                'id': user_id,
                'fname': user['fname'],
                'lname': user['lname'],
                'email': user['email'],
                'title': user['title'],
                'status': user['status'],
                'add_time': user['add_time'],
                'source_table': 'tbl_users',
                'user_type': 'CLINIC_USERS'
            }
        
        # Add tbl_radiologists
        for radiologist in tbl_radiologists:
            radiologist_id = radiologist['radiologist_id']
            old_users[radiologist_id] = {
                'id': radiologist_id,
                'fname': radiologist['fname'],
                'lname': radiologist['lname'],
                'email': radiologist['email'],
                'title': None,
                'status': radiologist['status'],
                'add_time': radiologist['add_time'],
                'source_table': 'tbl_radiologist',
                'user_type': 'RADIOLOGIST'
            }
        
        mysql_cursor.close()
        mysql_conn.close()
        
        logger.info(f"Total unique users in old database: {len(old_users)}")
        return old_users
        
    except Exception as e:
        logger.error(f"Error fetching users from MySQL: {str(e)}")
        raise

def get_new_database_users():
    """Get all user IDs from the new PostgreSQL database"""
    logger = logging.getLogger(__name__)
    logger.info("Fetching users from new PostgreSQL database...")
    
    try:
        postgres_conn = get_postgres_connection()
        postgres_cursor = postgres_conn.cursor()
        
        # Get all users with their olduserid if available
        postgres_cursor.execute("""
            SELECT "uId", "olduserid", "firstName", "lastName", "email", "userType", "createdAt"
            FROM "Users" 
            ORDER BY "uId"
        """)
        
        new_users = {}
        for row in postgres_cursor.fetchall():
            u_id, old_user_id, first_name, last_name, email, user_type, created_at = row
            new_users[u_id] = {
                'uId': u_id,
                'olduserid': old_user_id,
                'firstName': first_name,
                'lastName': last_name,
                'email': email,
                'userType': user_type,
                'createdAt': created_at
            }
        
        postgres_cursor.close()
        postgres_conn.close()
        
        logger.info(f"Total users in new database: {len(new_users)}")
        
        # Count users with olduserid
        users_with_old_id = sum(1 for user in new_users.values() if user['olduserid'] is not None)
        logger.info(f"Users with olduserid: {users_with_old_id}")
        
        return new_users
        
    except Exception as e:
        logger.error(f"Error fetching users from PostgreSQL: {str(e)}")
        raise

def find_missing_users(old_users, new_users):
    """Find users from old database that are missing in new database"""
    logger = logging.getLogger(__name__)
    logger.info("Comparing users between old and new databases...")
    
    # Get all old user IDs
    old_user_ids = set(old_users.keys())
    
    # Get all new user IDs (both uId and olduserid)
    new_user_ids = set()
    new_olduserids = set()
    
    for user in new_users.values():
        new_user_ids.add(user['uId'])
        if user['olduserid'] is not None:
            new_olduserids.add(user['olduserid'])
    
    logger.info(f"Old database user IDs: {len(old_user_ids)}")
    logger.info(f"New database uIds: {len(new_user_ids)}")
    logger.info(f"New database olduserids: {len(new_olduserids)}")
    
    # Find missing users (users in old database but not in new database)
    missing_user_ids = old_user_ids - new_olduserids
    
    logger.info(f"Missing users: {len(missing_user_ids)}")
    
    # Get details of missing users
    missing_users = []
    for user_id in missing_user_ids:
        missing_users.append(old_users[user_id])
    
    return missing_users

def generate_missing_users_report(missing_users):
    """Generate a detailed report of missing users"""
    logger = logging.getLogger(__name__)
    
    if not missing_users:
        logger.info("✅ No missing users found! All users from old database exist in new database.")
        return
    
    logger.info(f"❌ Found {len(missing_users)} missing users:")
    logger.info("=" * 80)
    
    # Group by source table
    missing_by_table = {}
    for user in missing_users:
        source_table = user['source_table']
        if source_table not in missing_by_table:
            missing_by_table[source_table] = []
        missing_by_table[source_table].append(user)
    
    for source_table, users in missing_by_table.items():
        logger.info(f"\n📋 Missing users from {source_table} ({len(users)} users):")
        logger.info("-" * 60)
        
        for user in users:
            logger.info(f"ID: {user['id']:>6} | {user['fname']} {user['lname']} | {user['email']} | Type: {user['user_type']}")
    
    # Create a summary file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_file = os.path.join(script_dir, f"missing_users_summary_{timestamp}.txt")
    
    with open(summary_file, 'w') as f:
        f.write(f"Missing Users Report - Generated on {datetime.now()}\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Total missing users: {len(missing_users)}\n\n")
        
        for source_table, users in missing_by_table.items():
            f.write(f"Missing users from {source_table} ({len(users)} users):\n")
            f.write("-" * 60 + "\n")
            
            for user in users:
                f.write(f"ID: {user['id']:>6} | {user['fname']} {user['lname']} | {user['email']} | Type: {user['user_type']}\n")
            f.write("\n")
    
    logger.info(f"\n📄 Detailed report saved to: {summary_file}")
    
    # Also create a CSV file for easy import
    csv_file = os.path.join(script_dir, f"missing_users_{timestamp}.csv")
    with open(csv_file, 'w') as f:
        f.write("ID,FirstName,LastName,Email,Title,Status,AddTime,SourceTable,UserType\n")
        for user in missing_users:
            f.write(f"{user['id']},{user['fname']},{user['lname']},{user['email']},{user['title'] or ''},{user['status']},{user['add_time']},{user['source_table']},{user['user_type']}\n")
    
    logger.info(f"📊 CSV export saved to: {csv_file}")

def main():
    """Main function to find missing users"""
    logger = setup_logging()
    
    try:
        logger.info("🔍 Starting missing users analysis...")
        
        # Get users from old database
        old_users = get_old_database_users()
        
        # Get users from new database
        new_users = get_new_database_users()
        
        # Find missing users
        missing_users = find_missing_users(old_users, new_users)
        
        # Generate report
        generate_missing_users_report(missing_users)
        
        logger.info("✅ Missing users analysis completed!")
        
    except Exception as e:
        logger.error(f"❌ Analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
