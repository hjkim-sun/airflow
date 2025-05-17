import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.operators.bash import BashOperator
#from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id='dags_trigger_dag_run_operator',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='30 9 * * *',
    catchup=False
) as dag:

    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start!"',
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dags_python_operator',
        trigger_run_id=None,
        #execution_date='{{data_interval_start}}',
        logical_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None
        )

    start_task >> trigger_dag_task