from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
import pendulum

def sla_miss_callback_to_slack(dag, task_list, blocking_task_list, slas, blocking_tis):
    '''
    sla_miss_callback의 경우 print 하는 내용이 airflow Task Log에 남지 않으며 디버깅의 어려움이 존재함

    :param dag: DAG class 객체
    :param task_list: delimiter(\n)로 구분된 string, (Ex: task_1 on 2023-05-10T06:00:00+00:00\ntask_2 on 2023-05-10T006:00:00+00:00)
    :param blocking_task_list: 실행되지 않은 task 리스트들
    :param slas: list로 감싸여진 slaMiss 객체
    :param blocking_tis: 실행되지 않은 task 리스트의 ti 객체들
    '''
    slack_hook = SlackWebhookHook(slack_webhook_conn_id='conn_slack_airflow_bot')
    text = "SLA Miss 알람"
    block_fields = []
    
    for task in task_list.split('\n'):
        task_id = task.split(' ')[0]
        execution_date = task.split(' ')[2]
        execution_date_kr = pendulum.parse(execution_date, tz='UTC').in_timezone('Asia/Seoul').strftime('%Y-%m-%dT%H:%M:%S+09:00')
        block_fields.append(
            {
                "type": "mrkdwn",
                "text": f"*DAG:*: {dag.dag_id}, *task_id*: {task_id} , *Execution_date*: {execution_date_kr}"
            }
        )
    blocks = [
        {
			"type": "section",
			"text": {
				"type": "mrkdwn",
				"text": "*SLA Miss 알람!"
			}
		},
        {
            "type": "section",
            "fields": block_fields
        }
    ]
    slack_hook.send(text=text, blocks=blocks)