# Project: Data Pipelines with Airflow
A music streaming company, **Sparkify**, has decided to introduce more automation and monitoring to their data warehouse ETL pipelines. They concluded that **Apache Airflow** is the best tool to achieve this.

They have brought you into the project to create **high-grade data pipelines** that are **dynamic, reusable, and easily monitored**. The pipelines should also support **backfills** and include **data quality checks** to detect discrepancies in the datasets.

The source data resides in **S3** and needs to be processed in **Amazon Redshift**. The datasets consist of **JSON logs** capturing user activity and **JSON metadata** about the songs users listen to.

# Project Specification: Data Pipelines with Airflow

## General Specifications
- The **DAG** can be browsed without issues in the **Airflow UI**.
- The DAG follows the **data flow** provided in the instructions.
- All tasks have dependencies, and the DAG begins with a `start_execution` task and ends with an `end_execution` task.

## DAG Configuration Specifications
- The DAG contains a **default_args** dictionary with the following keys:
  - `owner`
  - `depends_on_past`
  - `start_date`
  - `retries`
  - `retry_delay`
  - `catchup`
  - `default_args` are bound to the DAG.
- The DAG object has **default_args** set.
- The DAG is scheduled to **run once per hour**.

## Staging the Data Specifications
- A task stages data from **S3 to Redshift** (Runs a Redshift COPY statement).
- Instead of using a **static SQL statement**, the task uses **parameters** to dynamically generate the **COPY statement**.
- The operator contains **logging** at different execution steps.
- The **SQL statements** are executed using an **Airflow Hook**.

## Loading Dimensions and Facts Specifications
- **Dimensions** are loaded using the **LoadDimension** operator.
- **Facts** are loaded using the **LoadFact** operator.
- Instead of a static SQL statement, the tasks use **parameters** to generate the **COPY statement dynamically**.
- The DAG allows switching between **append-only** and **delete-load** functionality.

## Data Quality Checks Specifications
- **Data quality checks** are performed using the correct operator.
- The DAG either **fails** or **retries** **n times** when checks fail.
- The operator uses **parameters** to get the tests and expected results (tests are **not hardcoded** in the operator).
