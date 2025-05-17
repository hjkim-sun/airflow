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

    simple_task1() 