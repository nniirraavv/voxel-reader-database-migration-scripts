# MySQL to PostgreSQL Data Migration Guide

This repository contains migration scripts to transfer data from the legacy MySQL database to the new PostgreSQL database. The migration is organized into modules for different entity types (Users, Cases, Invoices, Clinics, etc.).


## Prerequisites

Before starting the migration, ensure you have:

- **Python 3.10+** installed
- **MySQL** database server running with the source database
- **PostgreSQL** database server running with the target database
- **pip** package manager
- Access to both database servers with appropriate credentials

## Environment Setup

### 1. Create Python Virtual Environment

Navigate to the project root directory and create a virtual environment:

```bash
cd /to/directory/voxel_order_1
python3 -m venv Env
```

### 2. Activate Virtual Environment

**On Linux/macOS:**
```bash
source Env/bin/activate
```

**On Windows:**
```bash
Env\Scripts\activate
```

### 3. Install Dependencies

Install all required Python packages:

```bash
pip install -r requirements.txt
```

The main dependencies include:
- `mysql-connector-python` - MySQL database connector
- `psycopg2` / `psycopg2-binary` - PostgreSQL database connector
- `python-dotenv` - Environment variable management
- `colorama` - Colored terminal output
- `typing-extensions` - Type hints support

## Database Setup

### MySQL Database (Source)

Ensure your MySQL database is loaded and accessible. If you need to load the MySQL database from a backup:

```bash
# Clean the SQL file first (remove GTID and fix collation issues)
sed -i '/@@GLOBAL.GTID_PURGED=/d' Backup/website_db.sql
sed -i 's/utf8mb4_unicode_520_ci/utf8mb4_unicode_ci/g' Backup/website_db.sql
sed -i 's/utf8mb4_0900_ai_ci/utf8mb4_unicode_ci/g' Backup/website_db.sql

# Load the database
mysql -u root -p voxel_backup < Backup/website_db.sql
```

### PostgreSQL Database (Target)

Ensure your PostgreSQL database is set up. If you need to load the PostgreSQL database from a backup:

```bash
# Load PostgreSQL backup
sudo -u postgres /usr/lib/postgresql/16/bin/pg_restore -v -d your_postgres_db Backup/dev_voxel_app_db.backup
```

**Note:** Replace `your_postgres_db` with your actual PostgreSQL database name.

## Configuration

### 1. Create Environment File

Create a `.env` file in the project root directory with the following variables:

```env
# MySQL Connection (Source Database)
MYSQL_HOST=localhost
MYSQL_USER=your_mysql_user
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=your_mysql_database

# PostgreSQL Connection (Target Database)
POSTGRES_HOST=localhost
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DATABASE=your_postgres_database
```

**Important:** Replace the placeholder values with your actual database credentials.

### 2. Verify Database Connections

Test the database connections before running migrations:

```bash
python db_connections.py
```

This script will:
- Connect to MySQL and list all tables
- Connect to PostgreSQL and list all tables
- Display connection status with colored output

## Pre-Migration Setup (MANDATORY)

**⚠️ IMPORTANT:** These steps must be performed on the PostgreSQL database **BEFORE** running any migration scripts. Execute these SQL commands in your PostgreSQL database.

### Step 1: Clear Existing Data

Clear existing data from tables (delete child records first to respect foreign key constraints):

```sql
-- Clear existing tables:
-- 1. Delete child records first (they reference others)
DELETE FROM "InvoiceCaseServices";
DELETE FROM "PaymentPlatformAccessTokens";
DELETE FROM "PaymentTransactions";
DELETE FROM "CaseFiles";
DELETE FROM "CaseServices";
DELETE FROM "CaseStudyPurposes";
DELETE FROM "CasePatients";
DELETE FROM "Cases";  -- FK to RadiologistInvoices
DELETE FROM "RadiologistInvoiceCaseServices";
DELETE FROM "Invoices";
DELETE FROM "RadiologistInvoices";
```

### Step 2: Schema Modifications for Users

Add required columns and enum values for the Users table:

```sql
-- Phase 1: For Users
ALTER TABLE "Users" ADD COLUMN olduserid INTEGER;
ALTER TYPE "enum_Users_userType" ADD VALUE 'CLINIC_USERS';
```

### Step 3: Schema Modifications for Invoices

Add required enum value for Invoices:

```sql
-- Phase 2: For Invoices
ALTER TYPE "enum_Invoices_invoiceType" ADD VALUE IF NOT EXISTS 'ADHOC';
```

### Step 4: Data Standardization

Update name titles to standardized format:

```sql
UPDATE "Users" SET "nameTitle"='Mr.' where "nameTitle"='Mr';
UPDATE "Users" SET "nameTitle"='Dr.' where "nameTitle"='Dr';
UPDATE "Users" SET "nameTitle"='Mrs.' where "nameTitle"='Mrs';
UPDATE "Users" SET "nameTitle"='Ms.' where "nameTitle"='Ms';
```

### Step 5: Insert Required System Users

Insert mandatory CMS users:

```sql
INSERT INTO public."Users" ("userType","email","sub","nameTitle","firstName","status","olduserid") 
VALUES ('CMS','nirav.shah@eternalsoftsolutions.com','34983428-e031-7001-00e9-ff56c1b7a274','Mr.','Admin',TRUE, 1002);

INSERT INTO public."Users" ("userType","email","sub","nameTitle","firstName","status","olduserid")
VALUES ('CMS','info@voxelreaders.com','444814a8-70e1-7007-6e80-6601cd63c766','Mr','CMS',TRUE,1001);
```

### Step 6: Update Existing User Mapping

Update specific user mapping:

```sql
UPDATE public."Users" set "olduserid"=1191 where "uId"=124;
```

### Step 7: Update Master Services

Ensure MasterServices are not marked as deleted:

```sql
UPDATE "MasterServices" SET "isDeleted"=false;
```

**Note:** Make sure to replace `your_postgres_db` with your actual PostgreSQL database name.

## Migration Process

The migration is organized into separate modules, each handling specific entity types. Each module contains numbered scripts that run in sequential order.

### Migration Architecture

- **Main Orchestrator:** `migrate_all.py` - Runs all migration modules in the correct order
- **Module Structure:** Each module (Users, Cases, Invoices, etc.) has:
  - Numbered migration scripts (`1_*.py`, `2_*.py`, etc.)
  - A `migrate.py` orchestrator that runs scripts in order
  - A logs directory for migration logs

## Migration Modules

### 1. Users Module (`Users/`)

**Total Tables: 2**

- `1_tbl_users__Users_.py` - Migrates user data from `tbl_users` to `Users` table
- `2_tbl_radiologist__Users.py` - Migrates radiologist data from `tbl_radiologist` to `Users` table

**Logs Location:** `Users/user_log/`

### 2. Invoices Module (`Invoices/`)

**Total Tables: 5**

- `1_tbl_radiologist_invoices_&_tbl_radiologist_invoice_details__RadiologistInvoices.py` - Migrates radiologist invoices
- `2_tbl_radiologist_invoices_&_tbl_radiologist_invoice_details__RadiologistInvoiceCaseServices.py` - Migrates radiologist invoice case services
- `3_tbl_client_invoices__Invoices.py` - Migrates client invoices
- `4_tbl_client_invoice_details_&_tbl_client_invoice_reports__InvoiceCaseServices.py` - Migrates invoice case services
- `5_tbl_user_service_charge__ClinicLocationServiceCharges.py` - Migrates service charges

**Logs Location:** `Invoices/invoice_logs/`

### 3. Cases Module (`Cases/`)

**Total Tables: 5**

- `1_tbl_cases__Cases.py` - Migrates cases data
- `2_tbl_cases_files_new__CaseFiles.py` - Migrates case files
- `3_tbl_study_purposes__CaseStudyPurposes.py` - Migrates study purposes
- `4_tbl_cases__ClinicPatient_CasePatients.py` - Migrates case patients
- `5_tbl_cases_report_CaseServices.py` - Migrates case services

**Logs Location:** `Cases/cases_logs/`

### 4. Clinics Module (`Clinics/`)

**Total Tables: 1**

- `1_tbl_practice__Clinics_ClinicLocations.py` - Migrates clinic locations

**Logs Location:** `Clinics/clinic_logs/`

### 5. Payments Module (`Payments/`)

**Status:** Currently contains `1.py` but is not yet integrated into the main migration script.

## Running Migrations

### Option 1: Run All Migrations (Recommended)

Run the complete migration process using the main orchestrator:

```bash
python migrate_all.py
```

This will execute migrations in the following specific order:
1. **Users/migrate.py** - Runs all Users scripts (2 tables)
2. **Clinics/1_tbl_practice__Clinics_ClinicLocations.py** - Creates Clinics and ClinicLocations
3. **Invoices/1_tbl_radiologist_invoices_&_tbl_radiologist_invoice_details__RadiologistInvoices.py**
4. **Cases/1_tbl_cases__Cases.py**
5. **Invoices/2_tbl_radiologist_invoices_&_tbl_radiologist_invoice_details__RadiologistInvoiceCaseServices.py**
6. **Invoices/3_tbl_client_invoices__Invoices.py**
7. **Invoices/4_tbl_client_invoice_details_&_tbl_client_invoice_reports__InvoiceCaseServices.py**
8. **Invoices/5_tbl_user_service_charge__ClinicLocationServiceCharges.py**
9. **Cases/2_tbl_cases_files_new__CaseFiles.py**
10. **Cases/3_tbl_study_purposes__CaseStudyPurposes.py**
11. **Cases/4_tbl_cases__ClinicPatient_CasePatients.py**
12. **Cases/5_tbl_cases_report_CaseServices.py**

**Note:** The order is critical due to foreign key dependencies. The script will stop if any step fails.

### Option 2: Run Individual Modules

You can run individual migration modules if needed:

**Run Users migration:**
```bash
cd Users
python migrate.py
cd ..
```

**Run Invoices migration:**
```bash
cd Invoices
python migrate.py
cd ..
```

**Run Cases migration:**
```bash
cd Cases
python migrate.py
cd ..
```

**Run Clinics migration:**
```bash
cd Clinics
python 1_tbl_practice__Clinics_ClinicLocations.py
cd ..
```

### Option 3: Run Individual Scripts

If you need to run a specific migration script:

```bash
python Cases/1_tbl_cases__Cases.py
```

## Migration Execution Flow

When you run `migrate_all.py`, the following happens in this exact order:

1. **Users Module:**
   - Executes `Users/migrate.py` which runs:
     - `1_tbl_users__Users_.py`
     - `2_tbl_radiologist__Users.py`
   - Logs are written to `Users/user_log/`

2. **Clinics Module:**
   - Executes `1_tbl_practice__Clinics_ClinicLocations.py`
   - Creates Clinics and ClinicLocations tables
   - Logs are written to `Clinics/clinic_logs/`

3. **Interleaved Invoices and Cases:**
   - Executes Invoices script 1
   - Executes Cases script 1
   - Executes Invoices scripts 2-5
   - Executes Cases scripts 2-5
   - Logs are written to respective `*_logs/` directories

Each script:
- Connects to both MySQL (source) and PostgreSQL (target) databases
- Reads data from MySQL tables
- Transforms and maps data according to the new schema
- Inserts data into PostgreSQL tables
- Logs all operations and any errors encountered

## Logging

All migration scripts generate detailed logs:

- **Location:** Each module has its own `*_logs/` directory
- **Format:** Timestamped logs with INFO, WARNING, and ERROR levels
- **Content:** Includes connection status, record counts, errors, and completion status

Example log locations:
- `Users/user_log/1_tbl_users__Users.log`
- `Invoices/invoice_logs/3_tbl_client_invoices__Invoices.log`
- `Cases/cases_logs/1_tbl_cases__Cases.log`

## Important Notes

1. **Migration Order:** The order of migrations is critical due to foreign key dependencies. Always run `migrate_all.py` to ensure correct order.

2. **Data Backup:** Always backup both source and target databases before running migrations.

3. **Incremental Runs:** The scripts are designed to handle re-runs, but always verify data integrity after each migration.

4. **User Mapping:** The migration uses `olduserid` column in the Users table to map relationships between old and new databases.

5. **Case Mapping:** 
   - Old database uses `voxel_cases_id` for display and `cases_id` for relationships
   - New database uses `cId` as primary key and `voxelCaseId` for display

6. **Invoice Mapping:** Only invoices from 2021 onwards are migrated from the old database.
