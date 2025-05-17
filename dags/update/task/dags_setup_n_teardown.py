import pendulum
# Airflow 3.0 부터 각각 아래 경로로 import 합니다.
from airflow.sdk import DAG, task

# Airflow 2.10.5 이하 버전에서 실습시 각각 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task

with DAG(
        dag_id="dags_setup_n_teardown",
        schedule=None,
        start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['update:2.10.5','setup-teardown']
) as dag:
    @task(task_id='setup_task')
    def setup_task():
        print('start')

    @task(task_id='do_something')
    def do_something():
        print("::group::Do Something Group")
        print('do it!')
        print('thank you')
        print('complete')
        print("::endgroup::")
        # raise AirflowException('error!')

    @task(task_id='teardown_task')
    def teardown_task():
        print('teardown!')

    setup_task().as_setup() >> do_something() >> teardown_task().as_teardown()

    #Task 관계는 아래처럼 해도 됨
    #do_something() >> teardown_task().as_teardown(setups=setup_task())
