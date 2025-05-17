import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG, Label
from airflow.providers.standard.operators.empty import EmptyOperator

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow import DAG
#from airflow.utils.edgemodifier import Label
#from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_empty_with_edge_label",
    schedule=None,
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    empty_1 = EmptyOperator(
        task_id='empty_1'
    )

    empty_2 = EmptyOperator(
        task_id='empty_2'
    )

    empty_1 >> Label('1과 2사이') >> empty_2

    empty_3 = EmptyOperator(
        task_id='empty_3'
    )

    empty_4 = EmptyOperator(
        task_id='empty_4'
    )

    empty_5 = EmptyOperator(
        task_id='empty_5'
    )

    empty_6 = EmptyOperator(
        task_id='empty_6'
    )

    empty_2 >> Label('Start Branch') >> [empty_3,empty_4,empty_5] >> Label('End Branch') >> empty_6