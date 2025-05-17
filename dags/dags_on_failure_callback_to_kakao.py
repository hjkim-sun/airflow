from datetime import timedelta
import pendulum
from config.on_failure_callback_to_kakao import on_failure_callback_to_kakao

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dags_on_failure_callback_to_kakao',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    schedule='*/20 * * * *',
    catchup=False,
    default_args={
        'on_failure_callback':on_failure_callback_to_kakao,
        'execution_timeout': timedelta(seconds=60)
    }

) as dag:
    task_slp_90 = BashOperator(
        task_id='task_slp_90',
        bash_command='sleep 90',
    )

    task_ext_1 = BashOperator(
        trigger_rule='all_done',
        task_id='task_ext_1',
        bash_command='exit 1'
    )

    task_slp_90 >> task_ext_1