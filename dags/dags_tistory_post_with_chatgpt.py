import pendulum
from operators.tistory_write_post_by_chatgpt_operator import TistoryWritePostByChatgptOperator
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG


with DAG(
    dag_id='dags_tistory_post_with_chatgpt',
    start_date=pendulum.datetime(2023, 5, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule='0 13 * * *',
) as dag:
    tistory_write_post_by_chatgpt = TistoryWritePostByChatgptOperator(
        task_id='tistory_write_post_by_chatgpt',
        post_cnt_per_market=3
    )