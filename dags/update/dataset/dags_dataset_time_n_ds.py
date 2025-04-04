from airflow import Dataset
from airflow import DAG
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.decorators import task
import pendulum


with DAG(
        dag_id='dags_dataset_time_n_ds',
        schedule=DatasetOrTimeSchedule(
            timetable=CronTriggerTimetable("* * * * *", timezone="Asia/Seoul"),
            datasets=(Dataset('dags_dataset_producer_3') &
                      (Dataset("dags_dataset_producer_1") | Dataset("dags_dataset_producer_2"))
            )
        ),
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        catchup=False,
        tags=['update:2.10.5','dataset','consumer']
) as dag:
    @task.bash(task_id='task_bash')
    def task_bash():
        return 'echo "schedule run"'

    task_bash()