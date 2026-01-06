# setup_postgres.py

import subprocess
import sys
from psycopg2 import connect, sql, errors
from db_connections import get_postgres_connection
import os
import shutil

# CONFIGS
NEW_DB_NAME = "website_db_order2"
BACKUP_FILE = "/home/eternal/Desktop/voxel_order_1/Backup/dev_voxel_app_db.backup"
TEMP_BACKUP_FILE = "/tmp/dev_voxel_app_db.backup"
POSTGRES_BIN_PATH = "/usr/lib/postgresql/16/bin"  # Change this if using a different version

def prepare_backup_file():
    """Copy backup file to a location accessible by postgres user"""
    print(f"Preparing backup file for postgres access...")
    try:
        # Copy to /tmp
        shutil.copy2(BACKUP_FILE, TEMP_BACKUP_FILE)
        
        # Change ownership to postgres user
        subprocess.run(
            ["sudo", "chown", "postgres:postgres", TEMP_BACKUP_FILE],
            check=True
        )
        print(f"✅ Backup file prepared at {TEMP_BACKUP_FILE}")
    except Exception as e:
        print(f"❌ Error preparing backup file: {e}")
        sys.exit(1)

def cleanup_backup_file():
    """Remove temporary backup file"""
    try:
        if os.path.exists(TEMP_BACKUP_FILE):
            os.remove(TEMP_BACKUP_FILE)
            print(f"✅ Temporary backup file cleaned up")
    except Exception as e:
        print(f"⚠️  Warning: Could not clean up temporary file: {e}")

def create_database():
    print(f"Creating database '{NEW_DB_NAME}'...")
    try:
        subprocess.run(
            ["sudo", "-u", "postgres", f"{POSTGRES_BIN_PATH}/createdb", NEW_DB_NAME],
            check=True
        )
        print(f"✅ Database '{NEW_DB_NAME}' created successfully.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error creating database: {e}")
        sys.exit(1)


def restore_backup():
    print(f"Restoring backup from '{TEMP_BACKUP_FILE}'...")
    try:
        subprocess.run(
            [
                "sudo", "-u", "postgres",
                f"{POSTGRES_BIN_PATH}/pg_restore",
                "-v",
                "-d", NEW_DB_NAME,
                TEMP_BACKUP_FILE
            ],
            check=True
        )
        print(f"✅ Backup restored successfully into '{NEW_DB_NAME}'.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error restoring backup: {e}")
        sys.exit(1)


def execute_post_restore_queries():
    print(f"Connecting to '{NEW_DB_NAME}' and running cleanup queries...")
    try:
        os.environ["POSTGRES_DATABASE"] = NEW_DB_NAME
        conn = get_postgres_connection()
        cur = conn.cursor()

        # DELETE queries
        delete_queries = [
            'DELETE FROM "InvoiceCaseServices";',
            'DELETE FROM "PaymentPlatformAccessTokens";',
            'DELETE FROM "PaymentTransactions";',
            'DELETE FROM "CaseFiles";',
            'DELETE FROM "CaseServices";',
            'DELETE FROM "CaseStudyPurposes";',
            'DELETE FROM "CasePatients";',
            'DELETE FROM "Cases";',
            'DELETE FROM "RadiologistInvoiceCaseServices";',
            'DELETE FROM "Invoices";',
            'DELETE FROM "RadiologistInvoices";'
        ]
        for query in delete_queries:
            cur.execute(query)

        # ALTER TABLE if column not exists
        cur.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns 
                    WHERE table_name='Users' AND column_name='olduserid'
                ) THEN
                    ALTER TABLE "Users" ADD COLUMN olduserid INTEGER;
                END IF;
            END
            $$;
        """)

        # Enum add safely
        try:
            cur.execute("ALTER TYPE \"enum_Users_userType\" ADD VALUE 'CLINIC_USERS';")
        except errors.DuplicateObject:
            print("Enum value 'CLINIC_USERS' already exists.")

        try:
            cur.execute("ALTER TYPE \"enum_Invoices_invoiceType\" ADD VALUE 'ADHOC';")
        except errors.DuplicateObject:
            print("Enum value 'ADHOC' already exists.")

        conn.commit()
        cur.close()
        conn.close()
        print("✅ Post-restore SQL steps completed successfully.")

    except Exception as e:
        print(f"❌ Error running post-restore steps: {e}")
        sys.exit(1)
    finally:
        os.environ["POSTGRES_DATABASE"] = "postgres"


def delete_database():
    print(f"Dropping database '{NEW_DB_NAME}'...")
    try:
        os.environ["POSTGRES_DATABASE"] = "postgres"
        conn = get_postgres_connection()
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = '{NEW_DB_NAME}' AND pid <> pg_backend_pid();
        """)
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(NEW_DB_NAME)))

        cur.close()
        conn.close()
        print(f"✅ Database '{NEW_DB_NAME}' dropped successfully.")

    except Exception as e:
        print(f"❌ Error dropping database: {e}")
        sys.exit(1)
    finally:
        os.environ["POSTGRES_DATABASE"] = "postgres"


if __name__ == "__main__":
    print("1 -> Create & setup database")
    print("2 -> Delete database")
    choice = input("Enter your choice: ")

    if choice == "1":
        prepare_backup_file()
        try:
            create_database()
            restore_backup()
            execute_post_restore_queries()
        finally:
            cleanup_backup_file()
    elif choice == "2":
        delete_database()
    else:
        print("❌ Invalid choice. Please enter 1 or 2.")