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

from pprint import pprint

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
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'data', 'pandas'],
) as dag:
    
    def get_data(ds_nodash):
        from mov.api.call import get_key, save2df
        key = get_key()
        df = save2df(ds_nodash)
        print(df.head(5))

    def branch_func(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        # path = f'{home_dir}/tmp/test_parquet/load_dt{ld}'
        path = os.path.join(home_dir, f'tmp/test_parquet/load_dt={ds_nodash}')
        if os.path.exists(path):    
            return "rm_dir"
        else:
            return "get_data", "echo_task"
    
    def save_data():
        from mov.api.call import get_key
        key = get_key()
        print("*" * 30)
        print(key)
        print("*" * 30)

    branch_op = BranchPythonOperator(
        task_id="branch_op",
        python_callable=branch_func,
    )

    get_data = PythonVirtualenvOperator(
        task_id="get_data",
        python_callable=get_data,
        requirements=["git+https://github.com/Seokxkyu/mov.git@0.2/api"],
        system_site_packages=False,
        trigger_rule='all_done',
        venv_cache_path='/home/kyuseok00/tmp/air_venv/get_data'
    )
    
    save_data = PythonVirtualenvOperator(
        task_id="save_data",
        python_callable=save_data,
        system_site_packages=False,
        trigger_rule='one_success',
        venv_cache_path='/home/kyuseok00/tmp/air_venv/get_data'
    )

    rm_dir = BashOperator(
        task_id="rm_dir",
        bash_command="rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}"
    )


    echo_task = BashOperator(
        task_id="echo_task",
        bash_command="echo 'task'"
    )
    
    task_start = gen_emp('start')
    task_end = gen_emp('end', 'all_done')
    
    join_task = BashOperator(
        task_id='join',
        bash_command="exit 1",
        trigger_rule='one_success'
    )

    task_start >> branch_op
    task_start >> join_task >> save_data

    branch_op >> rm_dir >> get_data
    branch_op >> echo_task >> save_data
    branch_op >> get_data

    get_data >> save_data >> task_end

