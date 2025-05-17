import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.sensors.filesystem import FileSensor

with DAG(
    dag_id='dags_file_sensor',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False
) as dag:
    tvCorona19VaccinestatNew_sensor = FileSensor(
        task_id='tvCorona19VaccinestatNew_sensor',
        fs_conn_id='conn_file_opt_airflow_files',
        filepath='tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv',
        recursive=False,
        poke_interval=60,
        timeout=60*60*24, # 1일
        mode='reschedule'
    )
