from airflow.sdk import Asset, DAG, task
import pendulum
from zlib import crc32


seoul_api_rt_bicycle_info = Asset('seoul_api_rt_bicycle_info')

with DAG(
        dag_id='dags_asset_metadata_consumer',
        schedule=[seoul_api_rt_bicycle_info],
        catchup=False,
        start_date=pendulum.datetime(2025, 3, 1, tz='Asia/Seoul'),
        tags=['update:3.0','asset','consumer','metadata']
) as dag:
    '''
    Airflow 3.0 부터 Schedule=[Asset]인 경우 Context 변수 data_interval_end = None 으로 리턴됨
    아래 함수는 Disable 하고 task_consumer_with_metadata 에서 모두 처리하는 것으로 변경
    '''
    # @task(task_id='task_read_rt_bicycle_info_csv')
    # def task_read_rt_bicycle_info_csv(**kwargs):
    #     dt = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y%m%d')
    #     file = f'/opt/airflow/files/rt_bicycle_info/{dt}/bikeList.csv'
    #     with open(file) as f:
    #         contents = f.read()
    #         crc = crc32(contents.encode())
    #     return crc

    # Task마다 Dataset에 접근하여 저장된 Metadata를 꺼낼수 있어야 함
    @task(task_id='task_consumer_with_metadata',
          inlets=[seoul_api_rt_bicycle_info])
    def task_consumer_with_metadata(**kwargs):
        inlet_events = kwargs.get('inlet_events')
        events = inlet_events[seoul_api_rt_bicycle_info]
        print('::group::Dataset Event List')
        for i in events:
            print(i)
        print('::endgroup::')
        crc_val_from_ds = events[-1].extra['crc32']
        file_path = events[-1].extra['file_path']
        with open(file_path) as f:
            contents = f.read()
            crc_val_from_file = crc32(contents.encode())

        print('::group::CRC verification process start')
        print(f'CRC of ds: {crc_val_from_ds}')
        print(f'CRC of file: {crc_val_from_file}')
        if crc_val_from_file == crc_val_from_ds:
            print('CRC verification Success')
        else:
            print('CRC verification Faile.')
        print('::endgroup::')

    task_consumer_with_metadata()
