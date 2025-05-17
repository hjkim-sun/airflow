from datetime import timedelta
import pendulum
from config.sla_miss_callback_to_slack import sla_miss_callback_to_slack

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.bash import BashOperator

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.models import Variable
#from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dags_sla_miss_callback_to_slack',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    schedule='*/10 * * * *',
    catchup=False,
    sla_miss_callback=sla_miss_callback_to_slack
) as dag:
    task_slp100_sla120 = BashOperator(
        task_id='task_slp100_sla120',
        bash_command='sleep 100',
        sla=timedelta(minutes=2)
    )

    task_slp100_sla180 = BashOperator(
        task_id='task_slp100_sla180',
        bash_command='sleep 100',
        sla=timedelta(minutes=3)
    )

    task_slp60_sla245 = BashOperator(
        task_id='task_slp60_sla245',
        bash_command='sleep 60',
        sla=timedelta(seconds=245)
    )

    task_slp60_sla250 = BashOperator(
        task_id='task_slp60_sla250',
        bash_command='sleep 60',
        sla=timedelta(seconds=250)
    )

    task_slp100_sla120 >> task_slp100_sla180 >> task_slp60_sla245 >> task_slp60_sla250