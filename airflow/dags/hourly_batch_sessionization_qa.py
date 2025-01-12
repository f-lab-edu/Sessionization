from datetime import datetime
from pendulum import timezone
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 10, tzinfo=timezone("Asia/Seoul")),  # KST로 고정
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

spark_config = {
    "application_args": [
        "{{ ds }}",
        "{{ logical_date.strftime('%H') }}"
    ],
    # 추가 설정 (필요 시 주석 제거)
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
    dag_id='hourly_batch_sessionization_qa',
    default_args=default_args,
    description='QA for sessionization',
    schedule_interval='@hourly',  # 매 시간 실행
    start_date=datetime(2019, 10, 1),
    catchup=False,
) as dag:

    submit_main_task = SparkSubmitOperator(
        task_id="sessionization",
        conn_id="local_spark",
        application="/Users/doyeonpyun/Desktop/Sessionization/src/main/sessionization.py",
        **spark_config)

    submit_main_task
