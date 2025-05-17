import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.standard.sensors.date_time import DateTimeSensor

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.sensors.date_time import DateTimeSensor

with DAG(
    dag_id="dags_time_sensor",
    start_date=pendulum.datetime(2023, 5, 1, 0, 0, 0),
    end_date=pendulum.datetime(2023, 5, 1, 1, 0, 0),
    schedule="*/10 * * * *",
    catchup=True,
) as dag:
    sync_sensor = DateTimeSensor(
        task_id="sync_sensor",
        target_time="""{{ macros.datetime.utcnow() + macros.timedelta(minutes=5) }}""",
    )