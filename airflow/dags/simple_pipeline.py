# airflow_pipeline.py (Airflow DAG to run the Jupyter notebooks)
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import papermill as pm

# Define default arguments for the DAG
default_args = {
    'owner': 'clourenco',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

# Create the DAG
with DAG('notebook_pipeline', default_args=default_args, schedule_interval=None) as dag:

    # Define Python functions to run the notebooks
    def run_preprocess():
        pm.execute_notebook(
            '../simple-pipeline/preprocess_data.ipynb',  # Relative path to the preprocess notebook
            '../simple-pipeline/output'  # Output notebook location
        )

    def run_analysis():
        pm.execute_notebook(
            '../simple-pipeline/analyze_data.ipynb',  # Relative path to the analysis notebook
            '../simple-pipeline/output'  # Output notebook location
        )

    def run_report():
        pm.execute_notebook(
            '../simple-pipeline/create_report.ipynb',  # Relative path to the report notebook
            '../simple-pipeline/output'  # Output notebook location
        )

    # Define the tasks in the DAG
    preprocess_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=run_preprocess
    )

    analyze_task = PythonOperator(
        task_id='analyze_data',
        python_callable=run_analysis
    )

    report_task = PythonOperator(
        task_id='create_report',
        python_callable=run_report
    )

    # Define task dependencies (create the pipeline)
    preprocess_task >> analyze_task >> report_task
