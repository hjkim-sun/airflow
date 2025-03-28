from airflow import DAG
import pendulum
from airflow.decorators import task
import pandas as pd


with DAG(
        dag_id="dags_xcom_push_dataframe",
        schedule=None,
        start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['update:2.10.5','dnm_tsk_map']
) as dag:

    @task(task_id='task_read_csv_to_df')
    def task_read_csv_to_df(**kwargs):
        dt = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y%m%d')
        file = f'/opt/airflow/files/rt_bicycle_info/{dt}/bikeList.csv'
        bicycle_info_dict = pd.read_csv(file)[:100]
        return bicycle_info_dict

    # dynamic task mapping 으로 연결된 task가 아닌 경우 pandas Dataframe은 Xcom 입력이 가능합니다.
    @task(task_id='task_load_df_from_xcom')
    def task_load_df_from_xcom(df):
        print(df)

    task_load_df_from_xcom(task_read_csv_to_df())