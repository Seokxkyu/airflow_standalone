from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python import PythonOperator
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
    
    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print("=" * 20)
        print(f"ds_nodash ==> {kwargs['ds_nodash']}")
        print(f"kwargs type ==> {type(kwargs)}")
        print("=" * 20)
        from mov.api.call import get_key
        key = get_key()
        print(f"MOVIE_API_KEY ==> {key}")

    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        pprint(kwargs)
        print(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"

    run_this = PythonOperator(
            task_id="print_the_context", 
            python_callable=print_context
    )

    get_data = PythonOperator(
            task_id="get_data",
            python_callable=get_data
    )

    save_data = BashOperator(
        task_id="save_data",
        bash_command="""
            echo "save data"
        """,
    )

    
    task_start = gen_emp('start')
    task_end = gen_emp('end', 'all_done')

    task_start >> get_data >> save_data >> task_end
    task_start >> run_this >> task_end
