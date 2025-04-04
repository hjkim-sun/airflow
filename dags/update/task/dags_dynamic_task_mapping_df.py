from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import get_current_context
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
import pandas as pd


with DAG(
        dag_id="dags_dynamic_task_mapping_df",
        schedule=None,
        start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['update:2.10.5','dnm_tsk_map','map_index']
) as dag:
    task_get_rt_bicycle_info = SeoulApiToCsvOperator(
        task_id='task_get_rt_bicycle_info',
        dataset_nm='bikeList',
        path='/opt/airflow/files/rt_bicycle_info/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='bikeList.csv'
    )

    @task(task_id='task_read_csv_to_df')
    def task_read_csv_to_df(**kwargs):
        dt = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y%m%d')
        file = f'/opt/airflow/files/rt_bicycle_info/{dt}/bikeList.csv'
        bicycle_info_dict = pd.read_csv(file)[:100]
        return bicycle_info_dict
        
    @task(task_id='task_count_character',
          map_index_template="{{ station_name_index }}"
    )
    def task_station_info(station):
        station_nm = station['stationName']
        prk_cnt = station['parkingBikeTotCnt']
        context = get_current_context()
        context["station_name_index"] = station_nm

        print(f'{station_nm}에 보관중인 자전거 건수: {prk_cnt}')

    task_read_csv_to_df = task_read_csv_to_df()
    task_get_rt_bicycle_info >> task_read_csv_to_df >> task_station_info.expand(station=task_read_csv_to_df)
