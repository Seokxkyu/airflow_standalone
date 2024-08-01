from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import (
        PythonOperator, 
        PythonVirtualenvOperator,
        BranchPythonOperator,
)


with DAG(
    'movie_summary',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },

    max_active_runs=1,
    max_active_tasks=3,
    description='movie summary',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'summary', 'dataframe'],
) as dag:
        
    def app_type():
        return

    def merge_df():
        return
    
    def delete_duplicate():
        return

    def summary_df():
        return
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    apply_type = PythonVirtualenvOperator(
            task_id="apply_type",
            python_callable=app_type,
            # requirements=["git+https://github.com/Seokxkyu/mov.git@0.3/api"],
            system_site_packages=False
    )

    merge_df = PythonVirtualenvOperator(
            task_id="merge_df",
            python_callable=merge_df,
            # requirements=["git+https://github.com/Seokxkyu/mov.git@0.3/api"],
            system_site_packages=False
    )

    de_dup = PythonVirtualenvOperator(
            task_id="del_dup",
            python_callable=delete_duplicate,
            # requirements=["git+https://github.com/Seokxkyu/mov.git@0.3/api"], 
            system_site_packages=False
    )

    summary_df = PythonVirtualenvOperator(
            task_id="summary_df",
            python_callable=summary_df,
            # requirements=["git+https://github.com/Seokxkyu/mov.git@0.3/api"],
            system_site_packages=False
    )

    start >> apply_type >> merge_df >> de_dup >> end
