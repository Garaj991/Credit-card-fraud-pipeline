# Credit Card Fraud Detection Pipeline

## Overview
End-to-end data pipeline built on GCP to detect credit card fraud patterns
using BigQuery, DBT, and Airflow.

## Architecture
Raw CSV → BigQuery → DBT Models → Mart Layer → Airflow Orchestration

## Tech Stack
- **BigQuery** — Data warehouse + SQL analysis
- **DBT** — Data modeling, transformations, testing
- **Airflow** — Pipeline orchestration (DAG)
- **Python** — Data processing
- **GCP** — Cloud infrastructure

## Key Findings
- Dataset: 284,807 transactions, 492 fraud cases (0.17% fraud rate)
- Fraud transactions have higher average amount than legitimate ones
- Fraud clusters observed at specific hours of the day

## Project Structure
fraud_pipeline/
├── models/
│   ├── stg_transactions.sql
│   ├── mart_fraud_summary.sql
│   └── schema.yml
├── dags/
│   └── fraud_pipeline_dag.py
└── dbt_project.yml
