from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.exceptions import AirflowException


with DAG(
        dag_id="dags_consecutive_failed",
        schedule='* * * * *',
        start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['update:2.10.5','bash','taskflow'],
        max_consecutive_failed_dag_runs=3
) as dag:

    @task(task_id='task_failed')
    def task_failed():
        raise AirflowException('error occured!')

    task_failed()