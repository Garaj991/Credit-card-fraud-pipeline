from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# ── Config ────────────────────────────────────────────────────────────────────
DBT_PROJECT_DIR = "/home/rajvee/fraud_pipeline"
DBT_BIN         = "/home/rajvee/airflow-env/bin/dbt"
GCP_PROJECT     = "data-pipeline-project-491810"
BQ_DATASET      = "credit_card_pipeline"
# ──────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "rajvee",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    'email': ['rajvee.airflow.test@gmail.com'],
}

# Shared env so every BashOperator can find gcloud ADC credentials
DBT_ENV = {
    "GOOGLE_APPLICATION_CREDENTIALS": "/home/rajvee/.config/gcloud/application_default_credentials.json",
    "DBT_PROJECT_DIR": DBT_PROJECT_DIR,
}

with DAG(
    dag_id="fraud_pipeline",
    default_args=default_args,
    description="Credit Card Fraud Detection Pipeline — dbt on BigQuery",
    schedule_interval='*/3 * * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "bigquery", "fraud"],
) as dag:

    # ── 1. Sanity-check that source data exists in BigQuery ───────────────────
    def check_source_data(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT)
    query = f"SELECT COUNT(*) as row_count FROM `{GCP_PROJECT}.{BQ_DATASET}.transactions`"
    result = list(client.query(query).result())
    row_count = result[0].row_count
    if row_count == 0:
        raise ValueError("No rows in transactions table!")
    print(f" Found {row_count} rows in transactions table")

check_source = PythonOperator(
    task_id="check_source_data",
    python_callable=check_source_data,
)

    # ── 2. Run dbt models (staging → marts) ──────────────────────────────────
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"{DBT_BIN} run --project-dir {DBT_PROJECT_DIR} --profiles-dir /home/rajvee/.dbt",
        env=DBT_ENV,
    )

    # ── 3. Run dbt tests ──────────────────────────────────────────────────────
    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"{DBT_BIN} test --project-dir {DBT_PROJECT_DIR} --profiles-dir /home/rajvee/.dbt",
        env=DBT_ENV,
    )

    # ── 4. Log completion ─────────────────────────────────────────────────────
   
    def log_success(**context):
        run_date = context["ds"]
        execution_date = context["execution_date"]

        html_content = f"""
        <h3> Fraud Pipeline Completed Successfully</h3>
        <table border="1" cellpadding="6">
            <tr><td><b>Run Date</b></td><td>{run_date}</td></tr>
            <tr><td><b>Execution Time</b></td><td>{execution_date}</td></tr>
            <tr><td><b>Project</b></td><td>{GCP_PROJECT}</td></tr>
            <tr><td><b>Dataset</b></td><td>{BQ_DATASET}</td></tr>
            <tr><td><b>Status</b></td><td>validate → staging → marts → tests ✅</td></tr>
        </table>
        """

        send_email(
            to=['rajvee.airflow.test@gmail.com'],
            subject=f' Fraud Pipeline Success — {run_date}',
            html_content=html_content,
        )
        logging.info(f" fraud_pipeline completed for {run_date}")

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=log_success,
        provide_context=True,
    )

    # ── Pipeline order ────────────────────────────────────────────────────────
    check_source >> run_dbt_models >> run_dbt_tests >> notify_success
