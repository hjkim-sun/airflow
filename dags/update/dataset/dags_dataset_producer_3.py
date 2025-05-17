import pendulum
# Airflow 3.0 부터 각각 아래 경로로 import 합니다.
from airflow.sdk import DAG, task, Asset

# Airflow 2.10.5 이하 버전에서 실습시 각각 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task
#from airflow import Dataset (DAG 코드 내 Asset --> Dataset 변경 필요)

dataset_dags_dataset_producer_3 = Asset("dags_dataset_producer_3")

with DAG(
        dag_id='dags_dataset_producer_3',
        schedule=None,
        start_date=pendulum.datetime(2025, 3, 1, tz='Asia/Seoul'),
        catchup=False,
        tags=['update:2.10.5','dataset','producer']
) as dag:
    @task(task_id='task_producer_3',
          outlets=[dataset_dags_dataset_producer_3])
    def task_producer_3():
        print('dataset update complete')

    task_producer_3()