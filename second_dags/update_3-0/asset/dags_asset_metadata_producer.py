from airflow.sdk import DAG, Asset, Metadata, task
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
import pendulum
from zlib import crc32

seoul_api_rt_bicycle_info = Asset('seoul_api_rt_bicycle_info')

with DAG(
        dag_id='dags_asset_metadata_producer',
        schedule='*/5 * * * *',
        catchup=False,
        start_date=pendulum.datetime(2025, 3, 1, tz='Asia/Seoul'),
        tags=['update:3.0','asset','producer','metadata']
) as dag:
    seoul_api_to_csv_operator = SeoulApiToCsvOperator(
        task_id='rt_bicycle_info',
        dataset_nm='bikeList',
        path='/opt/airflow/files/rt_bicycle_info/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='bikeList.csv'
    )

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
        # Asset에 Metadata를 넣는 방법, 첫 번째: context 변수 접근을 통해 입력
        kwargs["outlet_events"][seoul_api_rt_bicycle_info].extra = {"len_of_bikeList": cnt, 'crc32':crc, 'file_path':file}

        # Asset에 Metadata를 넣는 방법, 두 번째: Metadata 클래스 + yield 를 이용해 입력
        # yield Metadata(
        #          Asset('seoul_api_rt_bicycle_info'),
        #          extra={"len_of_bikeList": cnt, 'crc32':crc, 'file_path':file}
        #      )

    seoul_api_to_csv_operator >> task_producer_with_metadata()