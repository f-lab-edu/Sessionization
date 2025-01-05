import sys
sys.path.append('/Users/doyeonpyun/Desktop/Sessionization/src')  # 로컬 환경
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from main.Sessionization import main as sessionization_main
from main.file_partitioner import main as file_divide_main

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='hourly_batch_sessionization',
    default_args=default_args,
    description='test',
    schedule=None,  # 수동 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def run_file_divide():
        file_divide_main()

    run_file_divide_task = PythonOperator(
        task_id='run_file_divide',
        python_callable=run_file_divide,
    )

    def run_sessionization():
        sessionization_main()

    run_sessionization_task = PythonOperator(
        task_id='run_sessionization_main',
        python_callable=run_sessionization,
    )

    run_file_divide_task >> run_sessionization_task
