from airflow import Dataset
from airflow import DAG
from airflow.decorators import task
import pendulum


with DAG(
        dag_id='dags_dataset_consume_logical',
        # 논리표현(&, |)을 이용할 때는 schedule 항목에 리스트가 아닌 튜플로 작성!
        schedule=(Dataset('dags_dataset_producer_3') &
                  (Dataset("dags_dataset_producer_1") | Dataset("dags_dataset_producer_2"))
        ),
        catchup=False,
        start_date=pendulum.datetime(2025, 3, 1, tz='Asia/Seoul'),
        tags=['update:2.10.5','dataset','consumer']
) as dag:
    @task(task_id='task_consume')
    def task_consume():
        print('task run!')

    task_consume()