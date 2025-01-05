import sys
sys.path.append('/Users/doyeonpyun/Desktop/Sessionization/src') # 로컬 환경
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from main.Sessionization import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='hourly_batch_sessionization',
    default_args=default_args,
    description='add session_id column',
    schedule=None,  # 수동 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def run_main():
        main()

    run_main_task = PythonOperator(
        task_id='run_sessionization_main',
        python_callable=run_main,
    )

    run_main_task
