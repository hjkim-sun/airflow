from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.models.param import Param
from datetime import timedelta
import datetime
from airflow.operators.python import get_current_context


with DAG(
        dag_id="dags_params",
        schedule=None,
        start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['update:2.10.5','params'],
        params={
                # string type
                "string-date": Param(f'{datetime.date.today()}', type="string", format='date'),
                "string-datetime": Param(f'{datetime.datetime.now()}', type="string", format='date-time'),
                "string-multiline": Param('hello, world', type="string", format='multiline'),
                "string-enum": Param('a', type=['null','string'], enum=["a", "b", "c"]),

                # number type
                "number": Param(3.141592, type="number"),

                # boolean type
                "boolean": Param(True, type='boolean'),

                # array type
                "array-default": Param(['aa','bb','cc'], type='array'),
                "array-example": Param(['aa','bb','cc'], type='array', examples=['aa','bb','cc']),
                "array-example_value_display": Param(['aa','bb','cc'], type='array', examples=['aa','bb','cc'], values_display={"aa":"first","bb":"second","cc":"third"}),
                "array-value_display": Param(['aa','bb','cc'], type='array', values_display={"aa":"first","bb":"second","cc":"third"}),

                # object type
                "object": Param({'country':'korea','city':'seoul'}, type=['null','object']),

                # use case example
                "from_date": Param(f'{datetime.date.today() - datetime.timedelta(days=10)}', type=["null","string"], format='date'),
                "to_date": Param(f'{datetime.date.today()}', type=["null","string"], format='date'),
                },
) as dag:
    @task(task_id='task_show_all_params')
    def task_show_all_params(**kwargs):
        params = kwargs.get('params')
        print(params)

    @task(task_id='task_run_from_to_retriever')
    def task_run_from_to_retriever(**kwargs):
        from_date = kwargs.get('params').get('from_date') or kwargs.get('data_interval_start')
        to_date = kwargs.get('params').get('to_date') or kwargs.get('data_interval_end')

        if isinstance(from_date, str):
            from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
        if isinstance(to_date, str):
            to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d")

        print(f'from_date: {from_date}, to_date:{to_date} / SQL을 실행합니다.')
        return [(from_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((to_date - from_date).days + 1)]

    @task(task_id='task_run_from_to',
          map_index_template="{{ target_date }}")
    def task_run_from_to(target_date):
        context = get_current_context()
        context["target_date"] = target_date
        print(f'{target_date} 날짜 SQL을 실행합니다.')

    task_show_all_params()
    task_run_from_to.expand(target_date=task_run_from_to_retriever())