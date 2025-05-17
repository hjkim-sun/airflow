import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow.operators.bash import BashOperator
#from airflow import DAG

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME; echo good",
    )

    bash_t3 = BashOperator(
        task_id="bash_t3",
        bash_command="echo $HOSTNAME; echo good",
    )

    bash_t4 = BashOperator(
        task_id="bash_t4",
        bash_command="echo $HOSTNAME; echo good",
    )

    bash_t1 >> bash_t2 >> bash_t3 >> bash_t4
