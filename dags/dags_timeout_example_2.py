import pendulum
from datetime import timedelta
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.bash import BashOperator

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.models import Variable
#from airflow.operators.bash import BashOperator

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_timeout_example_2',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None,
    dagrun_timeout=timedelta(minutes=1),
    default_args={
        'execution_timeout': timedelta(seconds=40),
        'email_on_failure': True,
        'email': email_lst
    }
) as dag:
    bash_sleep_35 = BashOperator(
        task_id='bash_sleep_35',
        bash_command='sleep 35',
    )

    bash_sleep_36 = BashOperator(
        trigger_rule='all_done',
        task_id='bash_sleep_36',
        bash_command='sleep 36',
    )

    bash_go = BashOperator(
        task_id='bash_go',
        bash_command='exit 0',
    )

    bash_sleep_35 >> bash_sleep_36 >> bash_go
    