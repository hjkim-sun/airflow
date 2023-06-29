from airflow import DAG
import pendulum
from datetime import timedelta
from operators.tistory_write_post_by_chatgpt_operator import TistoryWritePostByChatgptOperator

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