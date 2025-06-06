from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
import pendulum

# Airflow 3.0 부터 각각 아래 경로로 import 합니다.
from airflow.sdk import DAG, task, ObjectStoragePath, Asset, AssetAlias

# Airflow 2.10.5 이하 버전에서 실습시 각각 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task
#from airflow.io.path import ObjectStoragePath
#from airflow import Dataset (DAG 코드 내 Asset --> Dataset 변경 필요)
#from airflow.datasets import DatasetAlias (DAG 코드 내 AssetAlias --> DatasetAlias 로 변경 필요)


BUCKET_NM = 's3://airflow-staging-hjkim/staging'
DATASET_PREFIX = f'{BUCKET_NM}/rt_bicycle_info'
DATASET_ALIAS = 'ds_rt_bicycle_to_s3'

with DAG(
        dag_id='dags_dataset_alias_producer',
        schedule="*/5 * * * *",
        catchup=False,
        start_date=pendulum.datetime(2025, 3, 1, tz='Asia/Seoul'),
        tags=['update:2.10.5','producer','metadata','dataset-alias','bicycle']
) as dag:
    seoul_api_to_csv_operator = SeoulApiToCsvOperator(
        task_id='rt_bicycle_info',
        dataset_nm='bikeList',
        path='/opt/airflow/files/rt_bicycle_info/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name="bikeList_{{ data_interval_end.in_timezone('Asia/Seoul').strftime('%H%M') }}.csv"
    )

    @task(task_id='task_producer_with_dataset_alias',
          outlets=[AssetAlias(DATASET_ALIAS)])
    def task_producer_with_dataset_alias(**kwargs):
        ymdhm = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y%m%d%H%M')
        ymd = ymdhm[:-4]
        ymdh = ymdhm[:10]
        hm = ymdhm[8:]
        file_nm = f'{ymd}/bikeList_{hm}.csv'
        file = f'/opt/airflow/files/rt_bicycle_info/{file_nm}'
        src = ObjectStoragePath(f'file://{file}')
        dst = ObjectStoragePath(f'{DATASET_PREFIX}/{file_nm}', conn_id='conn-amazon-s3-access')

        # Dataset alias 연결
        outlet_events = kwargs.get('outlet_events')

        # 실제 어떤 DataSet 명으로 publish 할지는 아래에서 결정 (런타임시 확정)
        # Metadata에는 저장한 S3의 상세 경로를 기입
        outlet_events[AssetAlias(DATASET_ALIAS)].add(Asset(f'{DATASET_PREFIX}/{ymdh}'),
                                                       extra={"path": f'{DATASET_PREFIX}/{file_nm}'})

        # S3 전송
        src.copy(dst)
        print(f'transfer complete: {src} -> {dst}')

    seoul_api_to_csv_operator >> task_producer_with_dataset_alias()