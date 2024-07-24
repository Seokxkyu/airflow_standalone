from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
    'make_parquet',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='make parquet',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['parquet', 'bash', 'shop', 'csv', 'db', 'history'],
) as dag:
    
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")

    task_check = BashOperator(
        task_id="check.done",
        bash_command="""
            echo "check done"
            DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ ds_nodash }}/_DONE
            bash {{ var.value.CHECK_SH }} $DONE_FILE
        """
    )

    task_to_parquet = BashOperator(
        task_id="to.parquet",
        bash_command="""
            echo "to.parquet"

            PYTHON_PATH=~/airflow/py/
            READ_PATH=~/data/csv/{{ds_nodash}}/csv.csv
            SAVE_PATH=~/data/parquet/{{ds_nodash}}/
            PARQUET_FILE=$SAVE_PATH/parquet.parquet
            
            mkdir -p $SAVE_PATH
            python $PYTHON_PATH/csv2parquet.py $READ_PATH $PARQUET_FILE
        """,
    )

    task_err = BashOperator(
        task_id="err.report",
        bash_command="""
            echo "err report"
        """,
        trigger_rule="one_failed"
    )

    task_done = BashOperator(
        task_id="make.done",
        bash_command="""
            echo "make.done"
        """,
    )
    
    task_start >> task_check
    task_check >> task_to_parquet >> task_done >> task_end
    task_check >> task_err >> task_end

