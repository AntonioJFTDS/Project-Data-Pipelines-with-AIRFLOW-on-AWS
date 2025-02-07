# Project Description

## Overview
This project implements a **Data Engineering pipeline** using **Apache Airflow** and **AWS Redshift**. It processes **music streaming data**, extracting it from **AWS S3**, staging it in **Redshift**, transforming it into **fact and dimension tables**, and performing **data quality checks**.

---

## Key Components

### 1. Airflow DAG (`the_dag.py`)
- Defines an **ETL workflow** for loading and transforming data in **Redshift**.
- Uses **Airflow Operators** for staging, loading, and validating data.
- Runs **hourly** (`0 * * * *`).
- Establishes task dependencies.

### 2. Staging Data (`stage_redshift.py`)
- Uses `StageToRedshiftOperator` to copy **raw JSON data** from **S3** to **Redshift staging tables**.
- **Data sources:**
  - `log_data`: User activity logs (`s3://udacity-dend/log-data/`).
  - `song_data`: Metadata of songs (`s3://udacity-dend/song-data/A/A/A`).

### 3. SQL Queries (`sql_queries.py`)
- Defines **SQL statements** for inserting data into Redshift tables.
- Handles **fact (`songplays`)** and **dimension tables (`users, songs, artists, time`)**.

### 4. Fact Table Loading (`load_fact.py`)
- Uses `LoadFactOperator` to insert data into the **fact table (`songplays`)**.
- Extracts data from **staging tables**.

### 5. Dimension Table Loading (`load_dimension.py`)
- Uses `LoadDimensionOperator` to insert data into **dimension tables** (`users, songs, artists, time`).
- Supports **truncate-insert** (clearing old data before inserting new ones).

### 6. Data Quality Checks (`data_quality.py`)
- Uses `DataQualityOperator` to validate Redshift tables.
- Runs **SQL tests** to ensure tables are populated correctly.

---

## Workflow Execution
1. **Extract:** Load raw **JSON** data from **S3** to **Redshift staging tables**.
2. **Transform:** Process and insert data into **fact and dimension tables**.
3. **Load:** Store structured tables in **Redshift**.
4. **Validate:** Run **data quality checks**.

This pipeline ensures an efficient and automated ETL process for processing music streaming data at scale.
