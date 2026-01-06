from dotenv import load_dotenv
import colorama
from colorama import Fore, Style
import mysql.connector
import psycopg2
import os

colorama.init(autoreset=True) 

load_dotenv()

# ---------------- MySQL Connection ---------------- #
def get_mysql_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE")
    )

# ---------------- PostgreSQL Connection ---------------- #
def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DATABASE")
    )

# ---------------- Main Test ---------------- #
if __name__ == "__main__":
    # MySQL
    print(Fore.CYAN + "Connecting to MySQL...")
    mysql_conn = get_mysql_connection()
    print(Fore.GREEN + "✅ MySQL Connection Established")
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute("SHOW TABLES")
    tables = mysql_cursor.fetchall()
    print(Fore.CYAN + f"MySQL Tables: {len(tables)}")
    for table in tables:
        print(Fore.LIGHTBLUE_EX + f" - {table[0]}")
    print()
    
    # PostgreSQL
    print(Fore.MAGENTA + "Connecting to PostgreSQL...")
    postgres_conn = get_postgres_connection()
    print(Fore.GREEN + "✅ PostgreSQL Connection Established")
    postgres_cursor = postgres_conn.cursor()
    postgres_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = postgres_cursor.fetchall()
    print(Fore.MAGENTA + f"PostgreSQL Tables: {len(tables)}")
    for table in tables:
        print(Fore.LIGHTYELLOW_EX + f" - {table[0]}")
