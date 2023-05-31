from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum
from config.sla_miss_callback_to_slack import sla_miss_callback_to_slack


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