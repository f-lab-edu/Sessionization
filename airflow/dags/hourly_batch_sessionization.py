from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Spark 설정
spark_config = {
    "application_args": [
        "{{ ds }}",  # 실행 날짜
        "{{ logical_date.strftime('%H') }}"  # 실행 시간
    ],
    # 필요 시 아래 주석 해제하여 추가 설정
    # 'driver_memory': "3g",
    # 'executor_cores': 4,
    # 'executor_memory': '4g',
}

# DAG 정의
with DAG(
    dag_id='hourly_batch_sessionization',
    default_args=default_args,
    description='Add session_id column',
    schedule_interval='@hourly',  # 매 시간 실행
    start_date=datetime(2019, 10, 10, tzinfo=timezone("Asia/Seoul")),  # 시작 시간 (KST 고정)
    catchup=False,
) as dag:

    # Spark 작업 정의
    submit_main_task = SparkSubmitOperator(
        task_id="sessionization",
        conn_id="local_spark",
        application="/Users/doyeonpyun/Desktop/Sessionization/src/main/Sessionization.py",  # 스크립트 경로
        **spark_config
    )

    submit_main_task
