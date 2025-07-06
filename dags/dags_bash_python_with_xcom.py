import pendulum
# Airflow 3.0 부터 각각 아래 경로로 import 합니다.
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

# Airflow 2.10.5 이하 버전에서 실습시 각각 아래 경로에서 import 하세요.
#from airflow.operators.bash import BashOperator
#from airflow.decorators import task
#from airflow import DAG

with DAG(
    dag_id="dags_bash_python_with_xcom",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    @task(task_id='python_push')
    def python_push_xcom():
        result_dict = {'status':'Good','data':[1,2,3],'options_cnt':100}
        return result_dict

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={
            'STATUS':'{{ti.xcom_pull(task_ids="python_push")["status"]}}',
            'DATA':'{{ti.xcom_pull(task_ids="python_push")["data"]}}',
            'OPTIONS_CNT':'{{ti.xcom_pull(task_ids="python_push")["options_cnt"]}}'

        },
        bash_command='echo $STATUS && echo $DATA && echo $OPTIONS_CNT'
    )
    python_push_xcom() >> bash_pull

    bash_push = BashOperator(
    task_id='bash_push',
    bash_command='echo PUSH_START '
                 '{{ti.xcom_push(key="bash_pushed",value=200)}} && '
                 'echo PUSH_COMPLETE'
    )

    @task(task_id='python_pull')
    def python_pull_xcom(**kwargs):
        ti = kwargs['ti']

        # 2025/07/06 추가 사항
        # 3.0.0 버전부터 task_ids 값을 주지 않으면 Xcom 을 찾지 못합니다.
        # 버그인지, 의도한 것인지는 확실치 않으나 해결될 때까지 task_ids 값을 넣어서 수행합니다.

        # status_value = ti.xcom_pull(key='bash_pushed')
        status_value = ti.xcom_pull(key='bash_pushed', task_ids='bash_push')
        return_value = ti.xcom_pull(task_ids='bash_push')
        print('status_value:' + str(status_value))
        print('return_value:' + return_value)

    bash_push >> python_pull_xcom()


