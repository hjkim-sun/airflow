import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
#from airflow.operators.python import PythonOperator
#from airflow import DAG

with DAG(
        dag_id='dags_python_with_postgres_hook',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        schedule=None,
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing
        
        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'hook insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s,%s,%s,%s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    insrt_postgres_with_hook = PythonOperator(
        task_id='insrt_postgres_with_hook',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'}
    )
    insrt_postgres_with_hook