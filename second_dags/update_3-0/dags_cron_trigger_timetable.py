from airflow.timetables.trigger import CronTriggerTimetable
from airflow.sdk import DAG, task
from datetime import timedelta
from pprint import pprint

with DAG(
    dag_id='dags_cron_trigger_timetable',
    schedule=CronTriggerTimetable(
        '*/5 * * * *',
        timezone='Asia/Seoul',
        interval=timedelta(minutes=5)
    ),
    tags=['cron']
) as dag:
    '''
    Config Parameter create_cron_data_intervals = True 를 지정하더라도
    CronTriggerTimetable을 사용하게 되면 interval을 고려하지 않습니다.
    즉 data_interval_start/end 컨텍스트 변수는 모두 동일한 값(DAG이 실행된 날짜) 가지게 됩니다.
    '''
    @task(task_id='task_show_context1')
    def task_show_context1(**context):
        print('::group::show context variables')
        pprint(context)
        print('::endgroup::')

    task_show_context1()
