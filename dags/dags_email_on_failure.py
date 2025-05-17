from airflow.providers.standard.operators.bash import BashOperator
from airflow.exceptions import AirflowException
import pendulum
from datetime import timedelta
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, task, Variable

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task
#from airflow.models import Variable
#from airflow.operators.bash import BashOperator

email_str = Variable.get("email_target")
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_email_on_failure',
    start_date=pendulum.datetime(2023,5,1, tz='Asia/Seoul'),
    catchup=False,
    schedule='0 1 * * *',
    dagrun_timeout=timedelta(minutes=2),
    default_args={
        'email_on_failure': True,
        'email': email_lst
    }
) as dag:
    @task(task_id='python_fail')
    def python_task_func():
        raise AirflowException('에러 발생')
    python_task_func()

    bash_fail = BashOperator(
        task_id='bash_fail',
        bash_command='exit 1',
    )

    bash_success = BashOperator(
        task_id='bash_success',
        bash_command='exit 0',
    )