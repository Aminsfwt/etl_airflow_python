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
    """
    Extract relevant fields from the tollplaza-data.tsv file.
    
    This function reads the TSV (Tab-Separated Values) file and extracts
    specific fields (columns 5-7, 0-indexed: fields[4], fields[5], fields[6])
    from each record.
    
    Input:  tollplaza-data.tsv (full record with multiple fields)
    Output: tsv_data.csv (fields 5-7: likely toll_amount, tag_number, etc.)
    
    Data Format: TSV (Tab-Separated Values)
    Extraction: Fields at index 4, 5, 6 (skipping first 4 fields)
    """
    global tollplaza_data_raw, tollplaza_data_ext
    print("Extracting data from TSV file...")
    
    # Read from raw TSV and write extracted fields to staging
    with open(tollplaza_data_raw, 'r') as infile, \
            open(tollplaza_data_ext, 'w') as outfile:
        for line in infile:
            fields = line.strip().split("\t")  # Split by tab character
            if len(fields) >= 4:
                # Extract fields at indices 4, 5, 6 (skipping first 4 columns)
                field_1 = fields[4]
                field_2 = fields[5]
                field_3 = fields[6]
                outfile.write(field_1 + "," + field_2 + "," + field_3 + "\n")

def extract_data_from_fixed_width():
    """
    Extract relevant fields from the payment-data.txt file.
    
    This function reads the fixed-width format file (space-delimited) and
    extracts specific fields (columns 6-7, 0-indexed: fields[5], fields[6])
    from each record.
    
    Note: Fixed-width files use positional data rather than delimiters.
    The data appears to be space-separated in this dataset.
    
    Input:  payment-data.txt (full record)
    Output: fixed_width_data.csv (fields at index 5, 6)
    
    Data Format: Fixed-width (space-separated in this case)
    Extraction: Fields at index 5, 6
    """
    global payment_data_raw, payment_data_ext
    print("Extracting data from Fixed Width file...")
    
    # Read from raw fixed-width file and write extracted fields to staging
    with open(payment_data_raw, 'r') as infile, \
            open(payment_data_ext, 'w') as outfile:
        for line in infile:
            fields = line.strip().split(" ")  # Split by space delimiter
            if len(fields) >= 4:
                # Extract fields at indices 5 and 6
                field_1 = fields[5]
                field_2 = fields[6]
                outfile.write(field_1 + "," + field_2 + "\n")

def consolidate_data():
    """
    Consolidate data from all three extracted files into a single file.
    
    This function merges the extracted data from:
    - csv_data.csv (vehicle data - 4 fields)
    - tsv_data.csv (toll plaza data - 3 fields)
    - fixed_width_data.csv (payment data - 2 fields)
    
    Into a single consolidated CSV file with 9 total fields.
    
    Implementation Notes:
    - Uses zip() to iterate through all three files simultaneously
    - Assumes files have equal number of records (aligned data)
    - Strips newline characters before joining to avoid duplicates
    
    Output Format: CSV with 9 columns (4 + 3 + 2)
    """
    print("Consolidating data...")
    
    # Open all three extracted files and the consolidated output
    with open(vehicle_data_ext, "r") as infile1, \
         open(tollplaza_data_ext, "r") as infile2, \
         open(payment_data_ext, "r") as infile3, \
         open(extracted_data_ext, "w") as outfile:
        
        # Iterate through all three files simultaneously
        for line1, line2, line3 in zip(infile1, infile2, infile3):
            # Strip existing newlines and concatenate with comma separator
            consolidated_line = f"{line1.strip()},{line2.strip()},{line3.strip()}\n"
            outfile.write(consolidated_line)

def transform_data():
    """
    Apply data transformations to the consolidated data.
    
    This function performs the following transformations:
    1. Converts field at index 3 to uppercase (likely vehicle type)
    2. Limits output to first 7 fields
    
    This transformation standardizes categorical data (vehicle types)
    to ensure consistency in the final output.
    
    Input:  extracted_data.csv (9 fields)
    Output: transformed_data.csv (7 fields, field 3 uppercase)
    
    Transformation Rules:
    - Field 3: Convert to uppercase (e.g., "car" -> "CAR")
    - Fields: Keep only first 7 fields
    """
    global extracted_data_ext, transformed_data
    print("Transforming data...")
    
    # Read consolidated data and write transformed data
    with open(extracted_data_ext, 'r') as infile, \
            open(transformed_data, 'w') as outfile:
        for line in infile:
            fields = line.strip().split(",")
            if len(fields) >= 7:
                # Transform: Convert field 3 to uppercase
                fields[3] = fields[3].upper()
                # Write only first 7 fields to output
                outfile.write(",".join(fields[:7]) + "\n")

# =============================================================================
# DAG Configuration
# =============================================================================

# Default arguments applied to all tasks in the DAG
# These settings control retry behavior, notifications, and ownership
default_args = {
    'owner': 'Amin Safout Ali',           # DAG owner for accountability
    'start_date': datetime(2026, 4, 20),  # DAG start date (April 20, 2026)
    'email': 'aminsafoutali@gmail.com',   # Email for notifications
    'email_on_failure': True,             # Send email when task fails
    'email_on_retry': True,               # Send email when task retries
    'retries': 1,                         # Number of retry attempts
    'retry_delay': timedelta(minutes=5),  # Wait time between retries (5 min)
}

# Create the DAG instance with configuration parameters
# 
# DAG Parameters:
# - dag_id: Unique identifier for the DAG ("ETL_toll_data")
# - default_args: Dictionary of default task parameters (defined above)
# - description: Human-readable description of the DAG's purpose
# - schedule: Run frequency (timedelta(days=1) = once per day)
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment - ETL Toll Data Pipeline',
    schedule=timedelta(days=1),  # Daily execution at midnight
)

# =============================================================================
# Task Definitions
# =============================================================================
# Each task is a PythonOperator that executes a specific ETL function.
# Tasks are executed in the order defined by the task dependencies below.

# Task 1: Download the raw data archive from remote URL
download_data = PythonOperator(
    task_id='download_data',
    python_callable=download_dataset,
    dag=dag,
)

# Task 2: Extract the downloaded archive to raw directory
untar_data = PythonOperator(
    task_id='untar_data',
    python_callable=untar_dataset,
    dag=dag,
)

# Task 3: Extract fields from CSV format (vehicle data)
extract_csv_task = PythonOperator(
    task_id='extract_data_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

# Task 4: Extract fields from TSV format (toll plaza data)
extract_tsv_task = PythonOperator(
    task_id='extract_data_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)

# Task 5: Extract fields from fixed-width format (payment data)
extract_fixed_width_task = PythonOperator(
    task_id='extract_data_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

# Task 6: Consolidate all extracted data into single file
consolidate_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

# Task 7: Transform data (uppercase, field selection)
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)


# =============================================================================
# Task Dependencies / Flow Control
# =============================================================================
# Define the execution order of tasks using bitwise shift operators:
#
# Flow Diagram:
#   download_data
#        |
#        v
#    untar_data
#        |
#        v
#    +----+----+----+
#    |    |    |    |
#    v    v    v    v
#  extract_csv  extract_tsv  extract_fixed_width
#    |    |    |    |
#    +----+----+----+
#        |
#        v
#   consolidate_data
#        |
#        v
#    transform_data
#
# Execution Order:
# 1. download_data must complete before untar_data starts
# 2. untar_data must complete before all three extract tasks start (parallel)
# 3. All three extract tasks must complete before consolidate_data starts
# 4. consolidate_data must complete before transform_data starts

download_data >> untar_data >> [extract_csv_task, extract_tsv_task, extract_fixed_width_task] >> consolidate_task >> transform_task
