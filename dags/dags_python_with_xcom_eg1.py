import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, task

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_task1')
    def xcom_push1(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_1")
        ti.xcom_push(key="result2", value=[1,2,3])

    @task(task_id='python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_2")
        ti.xcom_push(key="result2", value=[1,2,3,4])

    @task(task_id='python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']

        # 2025/07/06 추가 사항
        # 3.0.0 버전부터 task_ids 값을 주지 않으면 Xcom 을 찾지 못합니다.
        # 버그인지, 의도한 것인지는 확실치 않으나 해결될 때까지 task_ids 값을 리스트로 넣어 결과가 어떻게 나오는지 보는 것으로 대체합니다.
        # value1 = ti.xcom_pull(key="result1")
        value1 = ti.xcom_pull(key="result1", task_ids=['python_xcom_push_task1','python_xcom_push_task2'])
        value2 = ti.xcom_pull(key="result2", task_ids='python_xcom_push_task1')
        print(value1)
        print(value2)


    xcom_push1() >> xcom_push2() >> xcom_pull()