import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow.operators.bash import BashOperator
#from airflow import DAG

with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 L * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # START_DATE: 전월 말일, END_DATE: 1일 전
    bash_task_1 = BashOperator(
        task_id='bash_task_1',
        env={'START_DATE':'{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}',
             'END_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}'
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )