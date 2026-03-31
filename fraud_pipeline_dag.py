from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fraud_pipeline',
    default_args=default_args,
    description='Credit Card Fraud Detection Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    check_source = BashOperator(
        task_id='check_source_data',
        bash_command='echo "Checking source data in BigQuery..."',
    )

    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /path/to/fraud_pipeline && dbt run',
    )

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /path/to/fraud_pipeline && dbt test',
    )

    check_source >> run_dbt_models >> run_dbt_tests