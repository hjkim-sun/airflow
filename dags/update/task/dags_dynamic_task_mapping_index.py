from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import get_current_context
import os

with DAG(
        dag_id="dags_dynamic_task_mapping_index",
        schedule=None,
        start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['update:2.10.5','dnm_tsk_map','map_index']
) as dag:
    @task(task_id='task_list_file')
    def task_list_file():
        file_lst = [f for f in os.listdir('/opt/airflow/dags') if f.endswith('.py')]
        return file_lst

    @task(task_id='task_count_character',
          map_index_template="{{ file_name_index }}"
    )
    def task_count_character(file):
        context = get_current_context()
        context["file_name_index"] = file
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