import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.smtp.operators.smtp import EmailOperator

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow.operators.email import EmailOperator
#from airflow import DAG

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='hjkim_sun@naver.com',
        subject='Airflow 성공메일',
        html_content='Airflow 작업이 완료되었습니다'
    )