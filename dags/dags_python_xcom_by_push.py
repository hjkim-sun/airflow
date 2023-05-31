from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id="dags_python_xcom_by_push",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='xcom_push')
    def xcom_push(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='first_xcom_push',value=[1,2,3])


    @task(task_id='xcom_push2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='first_xcom_push',value=[1,2,3,4])

    @task(task_id='xcom_pull')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        first_xcom_push_value = ti.xcom_pull(key='first_xcom_push',task_ids='xcom_push')
        first_xcom_push_value2 = ti.xcom_pull(key='first_xcom_push',task_ids='xcom_push2')
        print(first_xcom_push_value)
        print(first_xcom_push_value2)

    xcom_push() >> xcom_push2() >> xcom_pull() 