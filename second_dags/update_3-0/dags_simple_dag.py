from airflow.sdk import DAG, task

with DAG(
    dag_id='dags_simple_dag',
    schedule='*/5 * * * *',
):
    @task(task_id='simple_task1')
    def simple_task1(**context):
        from pprint import pprint
        print('::group::show context variables')
        pprint(context)
        print('::endgroup::')

    @task(task_id='simple_task2')
    def simple_task2(**context):
        print('run')

    @task(task_id='simple_task3')
    def simple_task3(**context):
        print('run')

    @task(task_id='simple_task4')
    def simple_task4(**context):
        print('run')

    simple_task1() >> simple_task2() >> simple_task3() >> simple_task4()