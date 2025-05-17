import pendulum
import socket

# Airflow 3.0 부터 각각 아래 경로로 import 합니다.
from airflow.sdk import DAG, task

# Airflow 2.10.5 이하 버전에서 실습시 각각 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task

with DAG(
        dag_id="dags_multiple_executor",
        schedule=None,
        start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['update:2.10.5','multi-exec']
) as dag:
    @task(task_id='task_on_celery',
          executor='CeleryExecutor')
    def task_on_celery():
        print('this is Celery Executor')
        print(socket.gethostname())

    @task(task_id='task_on_local',
          executor='LocalExecutor')
    def task_on_local():
        print('this is Local Executor')
        print(socket.gethostname())

    task_on_celery()
    task_on_local()