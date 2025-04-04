from airflow import DAG
import pendulum
from airflow.decorators import task


with (DAG(
        dag_id="dags_setup_n_teardown_complex",
        schedule=None,
        start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['update:2.10.5','setup-teardown']
) as dag):
    @task(task_id='setup_task')
    def setup_task():
        print('start')

    @task()
    def do_something(**kwargs):
        print('do_something')

    @task(task_id='teardown_task')
    def teardown_task():
        print('teardown!')

    t1 = do_something(task_id='t1')
    t2 = do_something(task_id='t2')
    t3 = do_something(task_id='t3')
    t4 = do_something(task_id='t4')
    t5 = do_something(task_id='t5')
    t6 = do_something(task_id='t6')
    t7 = do_something(task_id='t7')
    t8 = do_something(task_id='t8')

    setup_task().as_setup() >> [t1, t2]
    t1 >> [t3, t4]
    t3 >> t5
    [t3, t6] >> teardown_task().as_teardown() >> t8
    t5 >> t6 >> t7
