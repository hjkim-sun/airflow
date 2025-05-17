from airflow.timetables.trigger import CronTriggerTimetable
import pendulum
# Airflow 3.0 부터 각각 아래 경로로 import 합니다.
from airflow.sdk import DAG, task, Asset
from airflow.timetables.assets import AssetOrTimeSchedule

# Airflow 2.10.5 이하 버전에서 실습시 각각 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task
#from airflow import Dataset (DAG 코드 내 Asset --> Dataset 변경 필요)
#from airflow.timetables.datasets import DatasetOrTimeSchedule (DAG 코드 내 Asset.. --> Dataset.. 변경 필요)

with DAG(
        dag_id='dags_dataset_time_n_ds',
        schedule=AssetOrTimeSchedule(
            timetable=CronTriggerTimetable("* * * * *", timezone="Asia/Seoul"),
            assets=(Asset('dags_dataset_producer_3') &
                      (Asset("dags_dataset_producer_1") | Asset("dags_dataset_producer_2"))
            )
        ),
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        catchup=False,
        tags=['update:2.10.5','asset','consumer']
) as dag:
    @task.bash(task_id='task_bash')
    def task_bash():
        return 'echo "schedule run"'

    task_bash()