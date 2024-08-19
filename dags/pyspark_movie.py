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
    'pyspark_movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },

    max_active_runs=1,
    max_active_tasks=3,
    description='processing movie parquet data',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'data', 'parquet'],
) as dag:


    start=EmptyOpeator(task_id=start)

    re_partition=PythonVirtualenvOperator(
            task_id='re_partition',
            callable=
    )

    join_df=BashOperator(
            task_id='join_df'
            bash_command=""
    )

    agg=PythonVirtualenvOperator(
            task_id='agg',
            callable=
    )

    end=EmptyOperator(task_id=end)

    start >> re_partition >> join_df >> agg >> end
