from airflow import DAG
import pendulum
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowException


with DAG(
        dag_id="dags_setup_n_teardown_tg",
        schedule=None,
        start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['update:2.10.5','setup-teardown']
) as dag:
    @task(task_id='task_outer_tg')
    def task_outer_tg():
        print('outer tg')

    @task_group(group_id='tg_inner_teardown')
    def tg_inner_teardown():
        @task(task_id='setup_task')
        def setup_task():
            print('start')

        @task(task_id='do_something')
        def do_something():
            raise AirflowException
            print('do it!')

        @task(task_id='teardown_task')
        def teardown_task():
            print('teardown!')

        setup_task().as_setup() >> do_something() >> teardown_task().as_teardown()

    tg_inner_teardown() >> task_outer_tg()
