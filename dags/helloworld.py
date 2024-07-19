from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
with DAG(
    'helloworld',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': True,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hello world DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['helloworld'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    
    t1 = BashOperator(
        task_id='download_data_file',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='clean_data',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
    

    # t1 = DummyOperator(task_id='download_data_file')
    # t2 = DummyOperator(task_id='clean_data')
    t31 = DummyOperator(task_id='peak_hour_identification')
    t32 = DummyOperator(task_id='crowded_station_identification')
    t33 = DummyOperator(task_id='crowded_line_identification')
    t4 = DummyOperator(task_id='visualize')
    t5 = DummyOperator(task_id='seek_insights_and_report')
    task_end = DummyOperator(task_id='end')
    task_start = DummyOperator(task_id='start')
    # task_empty = DummyOperator(task_id='empty')

    task_start >> t1
    t1 >> t2 >> [t31, t32] >> t4 >> t5
    t2 >> t33 >> t5
    t5 >> task_end
    # t1 >> t22 >> task_end
    # t1 >> t33 >> task_end
    # t3 >> task_empty
    # task_empty >> task_end
