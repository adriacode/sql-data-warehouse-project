"""
===============================================================================
Script: proc_load_bronze.py
===============================================================================
Purpose:
    This script loads data into the 'bronze' schema from external CSV files
    in a PostgreSQL Data Warehouse environment.

    It performs the following actions:
    - Truncates bronze tables before loading data.
    - Loads data from CSV files into bronze tables using PostgreSQL COPY
      via psycopg2 (STDIN).

Parameters:
    None.
    This script does not accept parameters and does not return any values.

Usage Example:
    python proc_load_bronze.py

Notes:
    - Designed for the Bronze layer, which is fully reloadable.
    - Tables in this layer are truncated before each load.
    - CSV file paths are configured inside the script.
===============================================================================
"""
import psycopg2
from datetime import datetime
import os
import sys 

# =========================
# Configurações
# =========================

DB_CONFIG = {
    "host": "localhost",
    "database": "DataWarehouse",
    "user": "postgres",
    "password": "123janeiroA@"
}

BASE_PATH = r"C:/Users/AdriaFreitas/OneDrive - IBM/Desktop/sql-data-warehouse-project/datasets"

TABLES = [ 
    ("bronze.crm_cust_info", "source_crm/cust_info.csv"),
    ("bronze.crm_prd_info", "source_crm/prd_info.csv"),
    ("bronze.crm_sales_details", "source_crm/sales_details.csv"),
    ("bronze.erp_loc_a101", "source_erp/loc_a101.csv"),
    ("bronze.erp_cust_az12", "source_erp/cust_az12.csv"),
    ("bronze.erp_px_cat_g1v2", "source_erp/px_cat_g1v2.csv")
]


# =========================
# Funções auxiliares
# =========================

def log(msg):
    print(msg)

def load_table(cursor, table_name, file_path):
    start_time = datetime.now()

    log(f">> Truncating Table {table_name}")
    cursor.execute(f"TRUNCATE TABLE {table_name};")

    log(f"Inserting Data Into: {table_name}")

    with open(file_path, "r", encoding="utf-8") as f:
        cursor.copy_expert(
            f"""
            COPY {table_name}
            FROM STDIN
            WITH (
                FORMAT CSV,
                HEADER, DELIMITER ','
                );
            """,
            f
        )

    end_time = datetime.now()
    duration = (end_time - start_time).seconds

    log(f">> Load Duration: {duration} seconds")
    log(">> --------------")

 
# =========================
# Main
# =========================

def main():
    batch_start_time = datetime.now()

    log("================================================")
    log("Loading Bronze Layer")
    log("================================================")

    try: 
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        log("================================================")
        log("Loading Tables")
        log("================================================")
        
        for table, relative_path in TABLES:
            file_path = os.path.join(BASE_PATH, relative_path)
            load_table(cursor, table, file_path)

        conn.commit()

        batch_end_time = datetime.now()
        total_duration = (batch_end_time - batch_start_time).seconds

        
        log("==========================================")
        log("Loading Bronze Layer is Completed")
        log(f"   - Total Load Duration: {total_duration} seconds")
        log("==========================================")
        
    except Exception as e:
        log("==========================================")
        log("ERROR OCCURRED DURING LOADING BRONZE LAYER")
        log(f"Error Message: {str(e)}")
        log("==========================================")
        sys.exit(1)
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# This block ensures that the ETL process is executed only when this script
# is run directly. If the file is imported as a module in another script,
# the ETL will NOT run automatically, preventing unintended data loads.

if __name__ == "__main__":
    main()

    
