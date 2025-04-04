from airflow import DAG
import pendulum
from airflow.decorators import task
import os
from airflow.operators.bash import BashOperator


with DAG(
        dag_id="dags_bash_task_decorator",
        schedule=None,
        start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['update:2.10.5','bash','taskflow']
) as dag:
    # return 구문에 실행할 bash 명령어 기입
    # dags_bash_operator.py DAG과 비교해보세요.
    @task.bash(task_id='task_select_fruit')
    def task_select_fruit():
        return '/opt/airflow/plugins/shell/select_fruit.sh ORANGE'

    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",
    )

    @task.bash(task_id='task_skip_state')
    def task_skip_state():
        # code 99 -> Skip status 로 인식
        return 'echo "This is skip status";exit 99'

    # dags_bash_with_macro_eg1.py DAG과 비교해보세요.
    @task.bash(task_id='task_get_env',
               env={'START_DATE':'{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}',
                    'END_DATE':'{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}'
                    })
    def task_get_env():
        return 'echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'

    # @task.bash 를 사용하면 Python 문법을 활용한 로직 작성이 가능하므로 Bash Operator를 사용하는 것보다 추천.
    # 예시) dags 디렉토리내 디렉토리별 파일이 몇 개인지 세는 프로그램
    @task.bash(task_id='task_py_is_better_than_bash_operator')
    def task_py_is_better_than_bash_operator():
        file_cnt_per_dir = {}
        for (path, dir, files) in os.walk("/opt/airflow/dags"):
            if path.endswith('__pycache__'):
                continue
            file_cnt = len(files)
            file_cnt_per_dir[path] = file_cnt
        return f'echo file count in /opt/airflow/dags: {file_cnt_per_dir} '

    t1_orange >> task_select_fruit() >> task_skip_state()
    task_get_env()
    task_py_is_better_than_bash_operator()