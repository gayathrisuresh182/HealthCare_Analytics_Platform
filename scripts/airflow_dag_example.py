"""
Apache Airflow DAG for Healthcare Analytics Pipeline
Install: pip install apache-airflow
Run: airflow webserver (in one terminal) and airflow scheduler (in another)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'healthcare_analytics',
    'depends_on_past': False,
    'email': ['admin@healthcare.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# DAG definition
dag = DAG(
    'healthcare_analytics_pipeline',
    default_args=default_args,
    description='Healthcare Analytics Data Pipeline',
    schedule_interval=timedelta(days=1),  # Run daily at 2 AM
    catchup=False,
    tags=['healthcare', 'dbt', 'data-quality'],
)

# Task 1: Install dbt packages
install_deps = BashOperator(
    task_id='install_dbt_packages',
    bash_command='cd /path/to/HealthCare_Analytics_Platform && dbt deps',
    dag=dag,
)

# Task 2: Run staging models
run_staging = BashOperator(
    task_id='run_staging_models',
    bash_command='cd /path/to/HealthCare_Analytics_Platform && dbt run --select staging',
    dag=dag,
)

# Task 3: Run intermediate models
run_intermediate = BashOperator(
    task_id='run_intermediate_models',
    bash_command='cd /path/to/HealthCare_Analytics_Platform && dbt run --select intermediate',
    dag=dag,
)

# Task 4: Run marts models
run_marts = BashOperator(
    task_id='run_marts_models',
    bash_command='cd /path/to/HealthCare_Analytics_Platform && dbt run --select marts',
    dag=dag,
)

# Task 5: Run dbt tests
run_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command='cd /path/to/HealthCare_Analytics_Platform && dbt test',
    dag=dag,
)

# Task 6: Run Great Expectations
run_gx = BashOperator(
    task_id='run_great_expectations',
    bash_command='cd /path/to/HealthCare_Analytics_Platform && python scripts/gx_run_checkpoint.py',
    dag=dag,
)

# Task 7: Generate documentation
generate_docs = BashOperator(
    task_id='generate_documentation',
    bash_command='cd /path/to/HealthCare_Analytics_Platform && dbt docs generate',
    dag=dag,
)

# Task 8: Build GX docs
build_gx_docs = BashOperator(
    task_id='build_gx_docs',
    bash_command='cd /path/to/HealthCare_Analytics_Platform && python scripts/gx_docs_build.py',
    dag=dag,
)

# Define task dependencies
install_deps >> run_staging >> run_intermediate >> run_marts >> [run_tests, run_gx] >> [generate_docs, build_gx_docs]

