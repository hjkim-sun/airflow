import pendulum

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task
#from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 3, 10, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator(
        task_id='python_t1',
        python_callable=python_function1,
        op_kwargs={'start_date':'{{data_interval_start | ds}}', 'end_date':'{{data_interval_end | ds}}'}
    )

    @task(task_id='python_t2')
    def python_function2(**kwargs):
        print(kwargs)
        print('ds:' + kwargs['ds'])
        print('ts:' + kwargs['ts'])
        print('data_interval_start:' + str(kwargs['data_interval_start']))
        print('data_interval_end:' + str(kwargs['data_interval_end']))
        print('task_instance:' + str(kwargs['ti']))


    python_t1 >> python_function2()
