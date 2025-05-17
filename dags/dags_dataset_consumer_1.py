import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow.operators.bash import BashOperator
#from airflow import DAG
#from airflow import Dataset

asset_dags_asset_producer_1 = Asset("dags_dataset_producer_1")

with DAG(
        dag_id='dags_dataset_consumer_1',
        schedule=[asset_dags_asset_producer_1],
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        catchup=False,
        tags=['asset','consumer']
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo {{ ti.run_id }} && echo "producer_1 이 완료되면 수행"'
    )