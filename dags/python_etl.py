from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from pathlib import Path

RAW_BASE_DIR = Path("/usr/local/airflow/dags/finalassignment/raw")
vehicle_data_raw = RAW_BASE_DIR / "vehicle-data.csv"
tollplaza_data_raw = RAW_BASE_DIR / "tollplaza-data.tsv"
payment_data_raw = RAW_BASE_DIR / "payment-data.txt"

STAGING_BASE_DIR = Path("/usr/local/airflow/dags/finalassignment/staging")
vehicle_data_ext = STAGING_BASE_DIR / "csv_data.csv"
tollplaza_data_ext = STAGING_BASE_DIR / "tsv_data.csv"
payment_data_ext = STAGING_BASE_DIR / "fixed_width_data.csv"
extracted_data_ext = STAGING_BASE_DIR / "extracted_data.csv"

TRANS_BASE_DIR = Path("/usr/local/airflow/dags/finalassignment/transformed")
transformed_data = TRANS_BASE_DIR / "transformed_data.csv"

# define function to download dataset
def download_dataset():
    import os
    from urllib.request import urlopen
    
    print("Downloading dataset...")
    source = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
    target_path = "/usr/local/airflow/dags/finalassignment/tolldata.tgz"
    #os.makedirs(os.path.dirname(target_path), exist_ok=True)

    with urlopen(source, timeout=30) as response:
        with open(target_path, "wb") as f:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                if chunk:
                    f.write(chunk)

    print(f"Download complete: {target_path}")

# define function to extract dataset
def untar_dataset():
    import os
    import tarfile

    print("Extracting dataset...")
    archive_path = "/usr/local/airflow/dags/finalassignment/tolldata.tgz"
    extract_path = "/usr/local/airflow/dags/finalassignment/raw"
    os.makedirs(extract_path, exist_ok=True)

    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(path=extract_path)

    print(f"Extraction complete: {extract_path}")

def extract_data_from_csv():
    global vehicle_data_raw, vehicle_data_ext
    print("Extracting data from CSV file...")
    with open(vehicle_data_raw, 'r') as infile, \
            open(vehicle_data_ext, 'w') as outfile:
        for line in infile:
            fields = line.strip().split(",")
            if len(fields) >= 4:
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
