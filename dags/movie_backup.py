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

with DAG(
    'movie_backup',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3),
    },

    max_active_runs=1,
    max_active_tasks=3,
    description='movie',
    schedule="10 2 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'data', 'pandas'],
) as dag:
        
    # 지정한 load_dt의 데이터프레임 return하고 parquet로 변환
    def get_data(ds_nodash):
        from mov.api.call import save2df
        df = save2df(ds_nodash)
        print(df.head(5))

    # 지정한 load_dt의 데이터프레임 return하고 parquet로 변환, url_param 지정 
    def fun_multi_y(ds_nodash):
        from mov.api.call import save2df
        p = {"multiMovieYn": "Y"}
        df = save2df(load_dt=ds_nodash, url_param=p)
        print(df.head(5))    

    def branch_func(ds_nodash):
        import os
        home_dir = os.path.expanduser("~")
        # path = f'{home_dir}/tmp/test_parquet/load_dt{ld}'
        path = os.path.join(home_dir, f'tmp/test_parquet/load_dt={ds_nodash}')
        if os.path.exists(path):    
            return "rm_dir"
        else:
            return "get_start", "echo_task"
    
    # 저장한 parquet 파일 불러와 데이터프레임으로 저장하고 openDt 기준으로 groupby 
    def save_data(ds_nodash):
        from mov.api.call import apply_type2df
        df = apply_type2df(load_dt=ds_nodash)
        print(df.head(10))
        print("*" * 30)
        print(df.dtypes)
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt':'sum'}).reset_index()
        print(sum_df)

    throw_err = BashOperator(
        task_id='throw_err',
        bash_command="exit 1",
        trigger_rule='all_done'
    )

    branch_op = BranchPythonOperator(
        task_id="branch_op",
        python_callable=branch_func,
    )

    get_data = PythonVirtualenvOperator(
        task_id="get_data",
        python_callable=get_data,
        requirements=["git+https://github.com/Seokxkyu/mov.git@0.3/api"],
        system_site_packages=False,
        # trigger_rule='all_done'
        # venv_cache_path='/home/kyuseok00/tmp/air_venv/get_data'
    )
    
    save_data = PythonVirtualenvOperator(
        task_id="save_data",
        python_callable=save_data,
        requirements=["git+https://github.com/Seokxkyu/mov.git@0.3/api"],
        system_site_packages=False,
        trigger_rule="one_success"
    )
    
    # 다양성 유무
    multi_y = PythonVirtualenvOperator(
        task_id='multi_y',
        python_callable=fun_multi_y,
        requirements=["git+https://github.com/Seokxkyu/mov.git@0.3/api"],
        system_site_packages=False
    )
    '''
    multi_n = PythonVirtualenvOperator(
        task_id='multi_n',
        python_callable=fun_multi_n,
        requirements=["git+https://github.com/Seokxkyu/mov.git@0.3/api"],
        system_site_packages=False
    )
    '''

    
    rm_dir = BashOperator(
        task_id="rm_dir",
        bash_command="rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}"
    )


    echo_task = BashOperator(
        task_id="echo_task",
        bash_command="echo 'task'"
    )
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # multi_y = EmptyOperator(task_id='multi_y') 
    multi_n = EmptyOperator(task_id='multi_n')  
    nation_k = EmptyOperator(task_id='nation_k') # 한국 영화 
    nation_f = EmptyOperator(task_id='nation_f') # 외국 영화 
    
    get_start = EmptyOperator(
            task_id='get_start', 
            trigger_rule='all_done'
    )
    get_end = EmptyOperator(task_id='get_end')

    start >> branch_op
    start >> throw_err >> save_data

    branch_op >> rm_dir >> get_start
    branch_op >> echo_task
    get_start >> [get_data, multi_y, multi_n, nation_k, nation_f]
    branch_op >> get_start 
    get_start >> [get_data, multi_y, multi_n, nation_k, nation_f] >> get_end
    
    get_end >> save_data >> end

