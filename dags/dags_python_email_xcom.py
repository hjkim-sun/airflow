import pendulum

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, task
from airflow.providers.smtp.operators.smtp import EmailOperator

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow.operators.email import EmailOperator
#from airflow import DAG
#from airflow.decorators import task

with DAG(
    dag_id="dags_python_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='something_task')
    def some_logic(**kwargs):
        from random import choice 
        return choice(['Success','Fail'])


    send_email = EmailOperator(
        task_id='send_email',
        to='hjkim_sun@naver.com',
        subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리결과',
        html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과는 <br> \
                    {{ti.xcom_pull(task_ids="something_task")}} 했습니다 <br>'
    )

    some_logic() >> send_email
