from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
with DAG(
    'import_db',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='history log 2 mysql db',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple', 'bash', 'etl', 'shop', 'csv', 'db', 'history'],
) as dag:

    task_check = BashOperator(
        task_id="check.done",
        bash_command='bash {{ var.value.CHECK_SH }} {{ ds_nodash }}'
        # bash_command='bash /home/kyuseok00/airflow/dags/check.sh {{ ds_nodash }}'
    )

    task_to_csv = BashOperator(
        task_id="to.csv",
        bash_command="""
            echo "to.csv"

            U_PATH=~/data/count/{{ds_nodash}}/count.log
            CSV_PATH=~/data/csv/{{ds_nodash}}
            
            mkdir -p $CSV_PATH

            cat ${U_PATH} | awk '{print "{{ds}}," $2 "," $1}' > ${CSV_PATH}/csv.csv

            echo $CSV_PATH
        """
    )

    task_to_tmp = BashOperator(
        task_id="to.tmp",
        bash_command="""
            echo "to.tmp"
        """
    )

    task_to_base = BashOperator(
        task_id="to.base",
        bash_command="""
            echo "to.base"
        """
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
        """
    )

    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> task_check
    task_check >> task_to_csv >> task_to_tmp >> task_to_base >> task_done
    task_check >> task_err >> task_end
    task_done >> task_end
