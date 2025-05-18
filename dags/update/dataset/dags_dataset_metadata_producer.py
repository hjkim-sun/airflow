from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
import pendulum
from zlib import crc32

# Airflow 3.0 부터 각각 아래 경로로 import 합니다.
from airflow.sdk import DAG, task, Asset, Metadata

# Airflow 2.10.5 이하 버전에서 실습시 각각 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task
#from airflow.datasets.metadata import Metadata
#from airflow import Dataset (DAG 코드 내 Asset --> Dataset 변경 필요)



seoul_api_rt_bicycle_info = Asset('seoul_api_rt_bicycle_info')

with DAG(
        dag_id='dags_dataset_metadata_producer',
        schedule=None,
        catchup=False,
        start_date=pendulum.datetime(2025, 3, 1, tz='Asia/Seoul'),
        tags=['update:2.10.5','dataset','producer','metadata']
) as dag:
    seoul_api_to_csv_operator = SeoulApiToCsvOperator(
        task_id='rt_bicycle_info',
        dataset_nm='bikeList',
        path='/opt/airflow/files/rt_bicycle_info/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='bikeList.csv'
    )
    '''
    airflow 3.0 -> Asset Trigger 되는 DAG의 data_interval_start, end 파라미터는 None 이 오도록 변경되었습니다. 
    따라서 Producer DAG에서 file 경로를 Metadata에 넣어 전송하고, Consumer DAG에서 file 경로를 꺼내오도록 변경합니다.
    '''
    @task(task_id='task_producer_with_metadata',
          outlets=[seoul_api_rt_bicycle_info])
    def task_producer_with_metadata(**kwargs):
        dt = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y%m%d')
        file = f'/opt/airflow/files/rt_bicycle_info/{dt}/bikeList.csv'
        with open(file) as f:
            contents = f.read()
            crc = crc32(contents.encode())
            cnt = len(contents.split('\n')) - 1
            print('file_name: bikeList.csv')
            print(f'file_crc: {crc}')
            print(f'file_cnt: {cnt}')
        # Dataset에 Metadata를 넣는 방법, 첫 번째: context 변수 접근을 통해 입력
        kwargs["outlet_events"][seoul_api_rt_bicycle_info].extra = {"len_of_bikeList": cnt, 'crc32':crc, 'file_path':file}

        # Dataset에 Metadata를 넣는 방법, 두 번째: Metadata 클래스 + yield 를 이용해 입력
        # yield Metadata(
        #         Asset('ds_with_metadata'),
        #         extra={"len_of_bikeList": cnt, 'crc32':crc}
        #     )

    seoul_api_to_csv_operator >> task_producer_with_metadata()