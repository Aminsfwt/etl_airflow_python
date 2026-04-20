# ETL Toll Data Pipeline - Apache Airflow

## Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline using Apache Airflow's PythonOperator. The pipeline processes toll plaza data from multiple file formats (CSV, TSV, and fixed-width) and produces a consolidated, transformed output.

> **Course**: ETL and Data Pipelines with Shell, Airflow, and Kafka  
> **Institution**: IBM/Coursera Data Engineering Professional Certificate  
> **Author**: Amin Safout Ali  
> **Date**: April 20, 2026

Project Contents
================

Your Astro project contains the following files and folders:

- dags: Contains the ETL pipeline DAGs:
    - `python_etl.py`: Main ETL toll data pipeline using PythonOperator
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: Additional files (empty by default)
- packages.txt: OS-level packages
- requirements.txt: Python packages
- plugins: Custom plugins (empty by default)
- airflow_settings.yaml: Local Airflow configuration

Deploy Your Project Locally
===========================

Start Airflow on your local machine by running 'astro dev start'.

This command will spin up five Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG Processor: The Airflow component responsible for parsing DAGs
- API Server: The Airflow component responsible for serving the Airflow UI and API
- Triggerer: The Airflow component responsible for triggering deferred tasks

When all five containers are ready the command will open the browser to the Airflow UI at http://localhost:8080/. You should also be able to access your Postgres Database at 'localhost:5432/postgres' with username 'postgres' and password 'postgres'.

Note: If you already have either of the above ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

---

## ETL Pipeline Details

### Overview

This DAG implements a complete ETL pipeline for processing toll plaza data:

1. **Download**: Fetches `tolldata.tgz` from IBM Cloud Object Storage
2. **Extract**: Unpacks the archive to `/usr/local/airflow/dags/finalassignment/raw/`
3. **Transform**: Extracts specific fields from 3 different file formats:
   - `vehicle-data.csv` → extracts fields 0-3
   - `tollplaza-data.tsv` → extracts fields 4-6
   - `payment-data.txt` → extracts fields 5-6
4. **Consolidate**: Merges all extracted data into single file (9 fields)
5. **Transform**: Converts field 3 to uppercase, outputs 7 fields

### DAG Configuration

- **Owner**: Amin Safout Ali
- **Schedule**: Daily at midnight
- **Retries**: 1 attempt with 5-minute delay
- **Notifications**: Email on failure and retry

### Data Flow

```
download_data → untar_data → [extract_csv, extract_tsv, extract_fixed] → consolidate → transform
```

---

## License

This project is part of the IBM Data Engineering Professional Certificate course on Coursera.

---

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
