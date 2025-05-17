import pendulum
import os

# Airflow 3.0 부터 각각 아래 경로로 import 합니다.
from airflow.sdk import DAG, task

# Airflow 2.10.5 이하 버전에서 실습시 각각 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task

with DAG(
        dag_id="dags_dynamic_task_mapping",
        schedule=None,
        start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['update:2.10.5','dnm_tsk_map']
) as dag:
    @task(task_id='task_list_file')
    def task_list_file():
        file_lst = os.listdir('/opt/airflow/dags')
        return file_lst

    @task(task_id='task_count_character')
    def task_count_character(file):
        file_path = f"/opt/airflow/dags/{file}"
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                character_cnt = len(f.read())
                print(f'file_path: {file_path}')
                print(f'character_cnt: {character_cnt}')
                return character_cnt
        except Exception as e:
            print(f"Error reading file: {e}")
            return 0

    task_count_character.expand(file=task_list_file())