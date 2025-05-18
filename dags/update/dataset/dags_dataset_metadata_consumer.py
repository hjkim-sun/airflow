import pendulum
from zlib import crc32
# Airflow 3.0 부터 각각 아래 경로로 import 합니다.
from airflow.sdk import DAG, task, Asset

# Airflow 2.10.5 이하 버전에서 실습시 각각 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task
#from airflow import Dataset (DAG 코드 내 Asset --> Dataset 변경 필요)

seoul_api_rt_bicycle_info = Asset('seoul_api_rt_bicycle_info')

with DAG(
        dag_id='dags_dataset_metadata_consumer',
        schedule=[seoul_api_rt_bicycle_info],
        catchup=False,
        start_date=pendulum.datetime(2025, 3, 1, tz='Asia/Seoul'),
        tags=['update:2.10.5','dataset','consumer','metadata']
) as dag:
    '''
    airflow 3.0 -> Asset Trigger 되는 DAG의 data_interval_start, end 파라미터는 None 이 오도록 변경되었습니다. 
    따라서 Producer DAG에서 file 경로를 Metadata에 넣어 전송하고, Consumer DAG에서 file 경로를 꺼내오도록 변경합니다.
    '''

    # Task마다 Dataset에 접근하여 저장된 Metadata를 꺼낼수 있어야 함
    @task(task_id='task_consumer_with_metadata',
          inlets=[seoul_api_rt_bicycle_info])
    def task_consumer_with_metadata(**kwargs):
        inlet_events = kwargs.get('inlet_events')
        events = inlet_events[Asset('seoul_api_rt_bicycle_info')]
        print('::group::Dataset Event List')
        for i in events[-5:]:   # 최근 produce된 Asset 5개만 출력
            print(i)
        print('::endgroup::')

        print('::group::CRC verification process start')
        crc_val_from_ds = events[-1].extra['crc32']
        file_path = events[-1].extra['file_path']

        with open(file_path) as f:
            contents = f.read()
            crc_of_file = crc32(contents.encode())

        print(f'CRC of ds: {crc_val_from_ds}')
        print(f'CRC of file: {crc_of_file}')
        if crc_of_file == crc_val_from_ds:
            print('CRC verification Success')
        else:
            print('CRC verification Faile.')
        print('::endgroup::')

    task_consumer_with_metadata()
