from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import config.slack_block_builder as sb
from airflow import DAG
import pendulum
from contextlib import closing
import pandas as pd

with DAG(
    dag_id='dags_daily_dag_monitoring',
    start_date=pendulum.datetime(2023,5,1, tz='Asia/Seoul'),
    schedule='0 8 * * *',
    catchup=False
) as dag:
    
    @task(task_id='get_daily_monitoring_rslt_task')
    def get_daily_monitoring_rslt_task():
        postgres_hook = PostgresHook(postgres_conn_id='conn-db-postgres-airflow')
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                with open('/opt/airflow/files/sqls/daily_dag_monitoring.sql', 'r') as sql_file:
                    cursor.execute("SET TIME ZONE 'Asia/Seoul';")
                    sql = '\n'.join(sql_file.readlines())
                    cursor.execute(sql)
                    rslt = cursor.fetchall()
                    rslt = pd.DataFrame(rslt)
                    rslt.columns = ['dag_id','run_cnt','success_cnt','failed_cnt','running_cnt','last_failed_date','last_success_date','next_dagrun_data_interval_start','next_dagrun_data_interval_end']
                    return_blocks = []

                    # 1) 실패대상
                    failed_df = rslt.query("(failed_cnt > 0)")
                    return_blocks.append(sb.section_text("*2 실패 대상*"))
                    if not failed_df.empty:
                        for idx, row in failed_df.iterrows():
                            return_blocks.append(sb.section_text(f"*DAG:* {row['dag_id']}\n*최근 실패일자:* {row['last_failed_date']}\n*마지막 성공일자:* {'없음' if str(row['last_success_date']) =='NaT' else row['last_success_date']}"))
                    else:
                        return_blocks.append(sb.section_text("없음"))
                    return_blocks.append(sb.divider())

                    # 2) 미수행 대상
                    skipped_df = rslt.query("(run_cnt == 0)")
                    return_blocks.append(sb.section_text("*3 미수행 대상*"))
                    if not skipped_df.empty:
                        for idx, row in skipped_df.iterrows():
                            return_blocks.append(sb.section_text(f"*DAG:* {row['dag_id']}\n*예정일자:* {row['next_dagrun_data_interval_end']}"))
                    else:
                        return_blocks.append(sb.section_text("없음"))
                    return_blocks.append(sb.divider())

                    # 3) 수행 중 대상
                    running_df = rslt.query("(running_cnt > 0)")
                    return_blocks.append(sb.section_text("*4 수행 중*"))
                    if not running_df.empty:
                        for idx, row in running_df.iterrows():
                            return_blocks.append(sb.section_text(f"*DAG:* {row['dag_id']}\n*배치일자:* {row['next_dagrun_data_interval_start']}"))
                    else:
                        return_blocks.append(sb.section_text("없음"))
                    return_blocks.append(sb.divider())

                    # 4) 성공 대상
                    done_success_cnt = rslt.query("(failed_cnt == 0) and (run_cnt > 0) and (running_cnt == 0)").shape[0]
                    yesterday = pendulum.yesterday('Asia/Seoul').strftime('%Y-%m-%d')
                    now = pendulum.now('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S')
                    return_blocks = [ sb.section_text(f"DAG 수행현황 알람({yesterday} ~ {now})"),
                                      sb.divider(),
                                      sb.section_text(f"*1. 수행 대상 DAG 개수*: {rslt.shape[0]}\n    (1) 성공 DAG 개수: {done_success_cnt}\n    (2) 실패: {failed_df.shape[0]}\n    (3) 미수행: {skipped_df.shape[0]}\n    (4) 수행 중: {running_df.shape[0]}"),
                                      sb.divider()
                    ] + return_blocks
                    return return_blocks

    send_to_slack = SlackWebhookOperator(
        task_id='send_to_slack',
        slack_webhook_conn_id='conn_slack_airflow_bot',
        blocks='{{ ti.xcom_pull(task_ids="get_daily_monitoring_rslt_task") }}'
    )

    get_daily_monitoring_rslt_task() >> send_to_slack