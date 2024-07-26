from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

def gen_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
    'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='movie',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 26),
    catchup=True,
    tags=['movie', 'data', 'pandas'],
) as dag:
    
    task_get = BashOperator(
        task_id="get.data",
        bash_command="""
            echo "get data"
        """
    )

    task_save = BashOperator(
        task_id="save.data",
        bash_command="""
            echo "save data"
        """,
    )

    
    task_start = gen_emp('start')
    task_end = gen_emp('end', 'all_done')

    task_start >> task_get >> task_save >> task_end
