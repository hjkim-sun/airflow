from datetime import datetime

# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.decorators import task
#from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_branch_decorator',
    start_date=datetime(2023,4,1),
    schedule=None,
    catchup=False
) as dag:
    @task.branch(task_id='python_branch_task')
    def select_random():
        import random
        item_lst = ['A', 'B', 'C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B','C']:
            return ['task_b','task_c']
    
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    select_random() >> [task_a, task_b, task_c]