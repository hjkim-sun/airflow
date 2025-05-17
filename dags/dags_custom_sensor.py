from sensors.seoul_api_date_sensor import SeoulApiDateSensor
import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG

with DAG(
    dag_id='dags_custom_sensor',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    tb_corona_19_count_status_sensor = SeoulApiDateSensor(
        task_id='tb_corona_19_count_status_sensor',
        dataset_nm='TbCorona19CountStatus',
        base_dt_col='S_DT',
        day_off=0,
        poke_interval=600,
        mode='reschedule'
    )
    
    tv_corona19_vaccine_stat_new_sensor = SeoulApiDateSensor(
        task_id='tv_corona19_vaccine_stat_new_sensor',
        dataset_nm='tvCorona19VaccinestatNew',
        base_dt_col='S_VC_DT',
        day_off=-1,
        poke_interval=600,
        mode='reschedule'
    )