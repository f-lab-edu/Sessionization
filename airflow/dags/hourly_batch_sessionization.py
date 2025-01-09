from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Spark 설정 변수
SPARK_JOB = {
    "task_id": "sessionization",
    "application": "/Users/doyeonpyun/Desktop/Sessionization/src/main/Sessionization.py",
    "conn_id": "local_spark",
    "application_args": [
        "{{ ds }}",
        "{{ logical_date.strftime('%H') }}"
    ],
    # 추가 설정
    # 'conf': {  # 필요 시 추가
    #     "spark.yarn.maxAppAttempts": "1",
    #     "spark.yarn.executor.memoryOverhead": "5120"
    # },
    # 'driver_memory': "3g",  # 필요 시 추가
    # 'executor_cores': 10,  # 필요 시 추가
    # 'num_executors': 10,  # 필요 시 추가
    # 'executor_memory': '5g',  # 필요 시 추가
    # 'keytab': '/keytab/path',  # Kerberos 인증 필요 시 추가
    # 'principal': '{keytab.principal}',  # Kerberos 인증 필요 시 추가
    # 'java_class': '{jar파일 안에 포함된 main class}'  # JAR 실행 시 필요
}

with DAG(
    dag_id='hourly_batch_sessionization',
    default_args=default_args,
    description='Add session_id column',
    schedule_interval=None,  # 수동 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    submit_main_task = SparkSubmitOperator(**SPARK_JOB)

    submit_main_task
