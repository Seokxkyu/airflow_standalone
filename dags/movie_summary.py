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
    
    REQUIREMENTS=["git+https://github.com/Seokxkyu/mov.git@0.3/api"]

    def gen_empty(id):
        task = EmptyOperator(task_id=id)
        return task
   
    def gen_empty(*ids):
        tasks = [EmptyOperator(task_id=id) for id in ids]
        return tuple(tasks) # (t, )

    def gen_vpython(**kwarg):
        id = kwarg['id']
        func = kwarg['func']
        op_kwargs = kwarg['op_kwargs']
        
        task = PythonVirtualenvOperator(
            task_id=id, 
            python_callable=func,
            system_site_packages=False,
            requirements=REQUIREMENTS,
            op_kwargs=op_kwargs
        )
        return task
    '''
    def gen_vpython(id, func, **opkwargs):
        id = id
        func = func
        op_kw = opkwargs

        task = PythonVirtualenvOperator(
            task_id=id,
            python_callable=func,
            system_site_packages=False,
            requirements=REQUIREMENTS,
            op_kwargs=op_kw
        )
        return task
    '''
    # def pro_data(ds_nodash, url_param):
        # print("pro data")

    def pro_data(**params):
        print("*" * 30)
        print(params['task_name'])
        print("*" * 30)

    def pro_data2(task_name, **params):
        print("@" * 30)
        print(params['task_name'])
        print("@" * 30)

    def merge_df():
        return
    
    def delete_duplicate():
        return

    def summary_df():
        return
    
    start, end = gen_empty('start', 'end')
    
    apply_type = gen_vpython(
            id="apply_type",
            func=pro_data,
            op_kwargs={
                "task_name": "apply_type!!!"
            }
    )
    
    
    merge_df = gen_vpython(
            id="merge_df",
            func=pro_data,
            op_kwargs={
                "task_name": "merge_df!!!"
            }
    )

    de_dup = gen_vpython(
            id="de_dup",
            func=pro_data,
            op_kwargs={
                "task_name": "de_dup!!!"
            }
    )

    summary_df = gen_vpython(
            id="summary_df",
            func=pro_data,
            op_kwargs={
                "task_name": "summary_df!!!"
            }
    )


    start >> apply_type >> merge_df >> de_dup >> summary_df >> end
