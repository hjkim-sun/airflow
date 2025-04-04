from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.exceptions import AirflowException


with DAG(
        dag_id="dags_setup_n_teardown_teardown_fail",
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
        print('do it!')

    @task(task_id='teardown_task')
    def teardown_task():
        raise AirflowException('error occured!')

    setup_task().as_setup() >> do_something() >> teardown_task().as_teardown()
    # teardown Task 실패시 dagrun Fail 되도록 하려면 on_failure_fail_dagrun=True 옵션 추가 필요
    # setup_task().as_setup() >> do_something() >> teardown_task().as_teardown(on_failure_fail_dagrun=True)