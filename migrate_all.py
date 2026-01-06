from Users.migrate import get_numbered_scripts as get_users_scripts, run_scripts as run_users_scripts
from Invoices.migrate import get_numbered_scripts as get_invoices_scripts, run_scripts as run_invoices_scripts
from Cases.migrate import get_numbered_scripts as get_cases_scripts, run_scripts as run_cases_scripts

def main():
    # USERS:- Total 2 tables
    user_scripts = get_users_scripts()
    run_users_scripts(user_scripts)

    # INVOICES:- Total 4 tables
    invoice_scripts = get_invoices_scripts()
    run_invoices_scripts(invoice_scripts)

    # CASES:- Total 6 tables
    case_scripts = get_cases_scripts()
    run_cases_scripts(case_scripts)

    # PAYMENTS:- Total  tables

if __name__ == "__main__":
    main()