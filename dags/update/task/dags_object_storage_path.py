from airflow.io.path import ObjectStoragePath
from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import get_current_context


src = ObjectStoragePath('s3://airflow-staging-hjkim/staging', conn_id='conn-amazon-s3-access')
dst = ObjectStoragePath('file:///opt/airflow/files/staging')

with DAG(
        dag_id="dags_object_storage_path",
        schedule=None,
        start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['update:2.10.5','objPath']
) as dag:
    @task(task_id='task_download_parquet')
    def task_download_parquet(base: ObjectStoragePath):
        file_lst = [obj for obj in base.iterdir() if obj.is_file()]
        return file_lst

    @task(task_id='task_download_to_local',
          map_index_template="{{ file_name_index }}")
    def task_download_to_local(src: ObjectStoragePath, dst: ObjectStoragePath):
        context = get_current_context()
        file_name = src.name
        dst = dst.joinuri(f'staging/{file_name}')
        context["file_name_index"] = file_name
        src.copy(dst=dst)

        print('copy complete')

    task_download_to_local.partial(dst=dst).expand(src=task_download_parquet(base=src))