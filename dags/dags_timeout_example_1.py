# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from datetime import timedelta
from airflow.models import Variable

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_timeout_example_1',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None,
    dagrun_timeout=timedelta(minutes=1),
    default_args={
        'execution_timeout': timedelta(seconds=20),
        'email_on_failure': True,
        'email': email_lst
    }
) as dag:
    bash_sleep_30 = BashOperator(
        task_id='bash_sleep_30',
        bash_command='sleep 30',
    )

    bash_sleep_10 = BashOperator(
        trigger_rule='all_done',
        task_id='bash_sleep_10',
        bash_command='sleep 10',
    )
    bash_sleep_30 >> bash_sleep_10
    
    