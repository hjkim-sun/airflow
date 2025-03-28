from airflow import Dataset
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
import pendulum

dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")

with DAG(
        dag_id='dags_dataset_time_n_ds',
        schedule=DatasetOrTimeSchedule(
            timetable=CronTriggerTimetable("* * * * *", timezone="UTC"),
            datasets=(Dataset('dags_dataset_producer_3') &
                      (Dataset("dags_dataset_producer_1") | Dataset("dags_dataset_producer_2"))
            )
        ),
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        catchup=False,
        tags=['update:2.10.5','dataset','consumer']
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[dataset_dags_dataset_producer_1],
        bash_command='echo "producer_1 수행 완료"'
    )