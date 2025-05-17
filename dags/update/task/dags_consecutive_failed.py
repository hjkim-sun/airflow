import pendulum
from airflow.exceptions import AirflowException

# Airflow 3.0 부터 각각 아래 경로로 import 합니다.
from airflow.sdk import DAG, task

# Airflow 2.10.5 이하 버전에서 실습시 각각 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.operators.bash import BashOperator

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