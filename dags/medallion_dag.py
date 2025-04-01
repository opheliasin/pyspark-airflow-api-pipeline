import datetime
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts import clean_forms_data, build_form_responses, extract_typeform_submissions, extract_typeform_questions

with DAG(
    dag_id="loading_typeform_data",
    start_date=datetime.datetime(2025, 3, 23),
    schedule="@daily",
    catchup=False,
) as dag:

    extract_submissions_task = PythonOperator(
        task_id="extract_submissions_task",
        python_callable=extract_typeform_submissions.main,
        op_kwargs={"execution_date": "{{ execution_date }}"},
    )

    extract_questions_task = PythonOperator(
        task_id="extract_questions_task",
        python_callable=extract_typeform_questions.main,
    )

    clean_task = PythonOperator(
        task_id="clean_task",
        python_callable=clean_forms_data.main,
    )

    build_BI_table_task = PythonOperator(
        task_id="build_BI_table_task",
        python_callable=build_form_responses.main,
    )

    [extract_submissions_task, extract_questions_task] >> clean_task >> build_BI_table_task
