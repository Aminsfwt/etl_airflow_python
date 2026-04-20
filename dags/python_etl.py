# =============================================================================
# ETL Toll Data Pipeline - Apache Airflow DAG
# =============================================================================
# This DAG implements a complete ETL (Extract, Transform, Load) pipeline
# that processes toll plaza data from multiple file formats.
#
# Data Flow:
#   1. Download: Fetch raw toll data archive from remote server
#   2. Extract: Unpack the tar.gz archive
#   3. Transform: Extract specific fields from CSV, TSV, and fixed-width formats
#   4. Consolidate: Merge all extracted data into a single file
#   5. Transform: Apply data transformations (uppercase conversion)
#
# Author: Amin Safout Ali
# Created: April 20, 2026
# =============================================================================

# Airflow core imports for DAG definition
from airflow import DAG
from datetime import datetime, timedelta

# Python operator for executing Python callable functions
from airflow.providers.standard.operators.python import PythonOperator

# Path handling for cross-platform file operations
from pathlib import Path

# =============================================================================
# Directory and File Path Configuration
# =============================================================================
# All paths are relative to the Airflow DAGs directory in the container.
# These paths match the structure expected by the final assignment.

# Raw data directory - contains downloaded source files
RAW_BASE_DIR = Path("/usr/local/airflow/dags/finalassignment/raw")

# Raw input files from various formats:
# - vehicle-data.csv: Contains vehicle information in CSV format
# - tollplaza-data.tsv: Contains toll plaza data in TSV format
# - payment-data.txt: Contains payment data in fixed-width format
vehicle_data_raw = RAW_BASE_DIR / "vehicle-data.csv"
tollplaza_data_raw = RAW_BASE_DIR / "tollplaza-data.tsv"
payment_data_raw = RAW_BASE_DIR / "payment-data.txt"

# Staging directory - intermediate files after extraction
STAGING_BASE_DIR = Path("/usr/local/airflow/dags/finalassignment/staging")

# Staging output files:
# - csv_data.csv: Extracted fields from vehicle-data.csv
# - tsv_data.csv: Extracted fields from tollplaza-data.tsv
# - fixed_width_data.csv: Extracted fields from payment-data.txt
# - extracted_data.csv: Consolidated data from all three sources
vehicle_data_ext = STAGING_BASE_DIR / "csv_data.csv"
tollplaza_data_ext = STAGING_BASE_DIR / "tsv_data.csv"
payment_data_ext = STAGING_BASE_DIR / "fixed_width_data.csv"
extracted_data_ext = STAGING_BASE_DIR / "extracted_data.csv"

# Transformed directory - final output after data transformation
TRANS_BASE_DIR = Path("/usr/local/airflow/dags/finalassignment/transformed")

# Final transformed output file
transformed_data = TRANS_BASE_DIR / "transformed_data.csv"


# =============================================================================
# ETL Task Functions
# =============================================================================

def download_dataset():
    """
    Download the toll data archive from a remote URL.
    
    This function fetches a compressed tar.gz archive containing
    raw toll data from IBM SkillsNetwork cloud storage.
    
    Source URL: IBM Cloud Object Storage (S3 compatible)
    Target File: /usr/local/airflow/dags/finalassignment/tolldata.tgz
    
    Implementation Notes:
    - Uses streaming download to handle large files efficiently
    - Reads in 8KB chunks to manage memory usage
    - Includes 30-second timeout to prevent hanging connections
    """
    import os
    from urllib.request import urlopen
    
    print("Downloading dataset...")
    
    # Remote source URL for the toll data archive
    source = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    
    # Local target path where the archive will be saved
    target_path = "/usr/local/airflow/dags/finalassignment/tolldata.tgz"

    # Stream download with chunked reading for memory efficiency
    with urlopen(source, timeout=30) as response:
        with open(target_path, "wb") as f:
            while True:
                chunk = response.read(8192)  # 8KB chunks
                if not chunk:
                    break
                f.write(chunk)

    print(f"Download complete: {target_path}")

def untar_dataset():
    """
    Extract the downloaded tar.gz archive to the raw data directory.
    
    This function decompresses the toll data archive and extracts
    all contained files to the designated raw data directory.
    
    Expected extracted files:
    - vehicle-data.csv: Vehicle registration data
    - tollplaza-data.tsv: Toll plaza transaction records
    - payment-data.txt: Payment information in fixed-width format
    
    Implementation Notes:
    - Creates the extraction directory if it doesn't exist
    - Uses gzip compression format ("r:gz")
    - Extracts all files recursively
    """
    import os
    import tarfile

    print("Extracting dataset...")
    
    # Path to the downloaded archive
    archive_path = "/usr/local/airflow/dags/finalassignment/tolldata.tgz"
    
    # Destination directory for extracted files
    extract_path = "/usr/local/airflow/dags/finalassignment/raw"
    
    # Ensure extraction directory exists
    os.makedirs(extract_path, exist_ok=True)

    # Extract all files from the gzip-compressed tar archive
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(path=extract_path)

    print(f"Extraction complete: {extract_path}")

def extract_data_from_csv():
    """
    Extract relevant fields from the vehicle-data.csv file.
    
    This function reads the raw CSV file and extracts the first four
    fields (columns) from each record.
    
    Input:  vehicle-data.csv (full record)
    Output: csv_data.csv (first 4 fields: likely rowid, timestamp, vehicle_id, vehicle_type)
    
    Data Format: CSV (Comma-Separated Values)
    Extraction: First 4 comma-delimited fields
    """
    global vehicle_data_raw, vehicle_data_ext
    print("Extracting data from CSV file...")
    
    # Read from raw CSV and write extracted fields to staging
    with open(vehicle_data_raw, 'r') as infile, \
            open(vehicle_data_ext, 'w') as outfile:
        for line in infile:
            fields = line.strip().split(",")
            if len(fields) >= 4:
                # Extract first 4 fields from each CSV record
                field_1 = fields[0]
                field_2 = fields[1]
                field_3 = fields[2]
                field_4 = fields[3]
                outfile.write(field_1 + "," + field_2 + "," + field_3 + "," + field_4 + "\n")

def extract_data_from_tsv():
    global tollplaza_data_raw, tollplaza_data_ext
    print("Extracting data from TSV file...")
    with open(tollplaza_data_raw, 'r') as infile, \
            open(tollplaza_data_ext, 'w') as outfile:
        for line in infile:
            fields = line.strip().split("\t")
            if len(fields) >= 4:
                field_1 = fields[4]
                field_2 = fields[5]
                field_3 = fields[6]
                outfile.write(field_1 + "," + field_2 + "," + field_3 + "\n")

def extract_data_from_fixed_width():
    global payment_data_raw, payment_data_ext
    print("Extracting data from Fixed Width file...")
    with open(payment_data_raw, 'r') as infile, \
            open(payment_data_ext, 'w') as outfile:
        for line in infile:
            fields = line.strip().split(" ")
            if len(fields) >= 4:
                field_1 = fields[5]
                field_2 = fields[6]
                outfile.write(field_1 + "," + field_2 + "\n")

def consolidate_data():
    print("Consolidating data...")
    with open(vehicle_data_ext, "r") as infile1, \
         open(tollplaza_data_ext, "r") as infile2, \
         open(payment_data_ext, "r") as infile3, \
         open(extracted_data_ext, "w") as outfile:
        for line1, line2, line3 in zip(infile1, infile2, infile3):
            consolidated_line = f"{line1.strip()},{line2.strip()},{line3.strip()}\n"
            outfile.write(consolidated_line)

def transform_data():
    global extracted_data_ext, transformed_data
    print("Transforming data...")
    with open(extracted_data_ext, 'r') as infile, \
            open(transformed_data, 'w') as outfile:
        for line in infile:
            fields = line.strip().split(",")
            if len(fields) >= 7:
                fields[3] = fields[3].upper()
                outfile.write(",".join(fields[:7]) + "\n")

# Define the default_args dictionary to specify the default parameters for the DAG and its tasks
default_args = {
    'owner': 'Amin Safout Ali',
    'start_date': datetime(2026, 4, 20),
    'email': 'aminsafoutali@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG with the specified parameters
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule=timedelta(days=1),
)

# download task
download_data = PythonOperator(
    task_id='download_data',
    python_callable=download_dataset,
    dag=dag,
)

# untar task
untar_data = PythonOperator(
    task_id='untar_data',
    python_callable=untar_dataset,
    dag=dag,
)

# extract task for CSV file
extract_csv_task = PythonOperator(
    task_id='extract_data_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

# extract task for TSV file
extract_tsv_task = PythonOperator(
    task_id='extract_data_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)

# extract task for Fixed Width file
extract_fixed_width_task = PythonOperator(
    task_id='extract_data_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

# consolidate task
consolidate_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

# transform task
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

download_data >> untar_data >> [extract_csv_task, extract_tsv_task, extract_fixed_width_task] >> consolidate_task >> transform_task
