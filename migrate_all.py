import os
import subprocess
import sys

def run_script(script_path, script_name):
    """Run a single migration script"""
    print(f"\n🚀 Running {script_name}...")
    result = subprocess.run([sys.executable, script_path])
    if result.returncode != 0:
        print(f"❌ {script_name} exited with error code {result.returncode}")
        return False
    else:
        print(f"✅ {script_name} completed successfully.")
        return True

def main():
    """
    Migration execution order (specific interleaved sequence):
    
    1. Users/migrate.py (runs all Users scripts)
    2. Clinics/1_tbl_practice__Clinics_ClinicLocations.py
    3. Invoices/1_tbl_radiologist_invoices_&_tbl_radiologist_invoice_details__RadiologistInvoices.py
    4. Cases/1_tbl_cases__Cases.py
    5. Invoices/2_tbl_radiologist_invoices_&_tbl_radiologist_invoice_details__RadiologistInvoiceCaseServices.py
    6. Invoices/3_tbl_client_invoices__Invoices.py
    7. Invoices/4_tbl_client_invoice_details_&_tbl_client_invoice_reports__InvoiceCaseServices.py
    8. Invoices/5_tbl_user_service_charge__ClinicLocationServiceCharges.py
    9. Cases/2_tbl_cases_files_new__CaseFiles.py
    10. Cases/3_tbl_study_purposes__CaseStudyPurposes.py
    11. Cases/4_tbl_cases__ClinicPatient_CasePatients.py
    12. Cases/5_tbl_cases_report_CaseServices.py
    """
    
    # Get the project root directory
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    # Define the exact execution order
    migration_scripts = [
        # Step 1: Users - Run all scripts via migrate.py
        "Users/migrate.py",
        
        # Step 2: Clinics - Script 1
        "Clinics/1_tbl_practice__Clinics_ClinicLocations.py",
        
        # Step 3: Invoices - Script 1
        "Invoices/1_tbl_radiologist_invoices_&_tbl_radiologist_invoice_details__RadiologistInvoices.py",
        
        # Step 4: Cases - Script 1
        "Cases/1_tbl_cases__Cases.py",
        
        # Step 5: Invoices - Script 2
        "Invoices/2_tbl_radiologist_invoices_&_tbl_radiologist_invoice_details__RadiologistInvoiceCaseServices.py",
        
        # Step 6: Invoices - Script 3
        "Invoices/3_tbl_client_invoices__Invoices.py",
        
        # Step 7: Invoices - Script 4
        "Invoices/4_tbl_client_invoice_details_&_tbl_client_invoice_reports__InvoiceCaseServices.py",
        
        # Step 8: Invoices - Script 5
        "Invoices/5_tbl_user_service_charge__ClinicLocationServiceCharges.py",
        
        # Step 9: Cases - Script 2
        "Cases/2_tbl_cases_files_new__CaseFiles.py",
        
        # Step 10: Cases - Script 3
        "Cases/3_tbl_study_purposes__CaseStudyPurposes.py",
        
        # Step 11: Cases - Script 4
        "Cases/4_tbl_cases__ClinicPatient_CasePatients.py",
        
        # Step 12: Cases - Script 5
        "Cases/5_tbl_cases_report_CaseServices.py",
    ]
    
    print("\n" + "="*70)
    print("DATA MIGRATION - EXECUTING SCRIPTS IN SPECIFIED ORDER")
    print("="*70)
    
    # Execute scripts in the specified order
    for i, script_path in enumerate(migration_scripts, 1):
        full_path = os.path.join(project_root, script_path)
        
        if not os.path.exists(full_path):
            print(f"\n❌ ERROR: Script not found: {full_path}")
            print(f"Migration stopped at step {i}")
            sys.exit(1)
        
        print(f"\n{'='*70}")
        print(f"STEP {i}/{len(migration_scripts)}: {script_path}")
        print(f"{'='*70}")
        
        success = run_script(full_path, script_path)
        if not success:
            print(f"\n❌ Migration failed at step {i}: {script_path}")
            print("Stopping migration process.")
            sys.exit(1)
    
    print("\n" + "="*70)
    print("✅ MIGRATION COMPLETE - All scripts executed successfully!")
    print("="*70)

if __name__ == "__main__":
    main()