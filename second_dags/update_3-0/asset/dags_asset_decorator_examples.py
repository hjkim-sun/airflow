from airflow.sdk import asset, Asset, Context
import os

'''
1. @asset 사용시 dag과 task는 자동생성되며
2. 만들어지는 Asset의 명은 uri 값이 아닌 함수명으로 설정됨(my_produce_asset_1 또는 2)
'''

@asset(
    dag_id='dags_asset_decorator_produce_1',
    schedule=None,
    tags=['asset'],
    uri='asset_by_decorator_1'
)
def my_produce_asset_1(context: Context):
    # context 변수를 이용해서 원래 task에서 하던 여러 작업을 할 수 있습니다 (xcom 활용 등)
    # 가급적 타입 힌팅을 통해 Context 타입을 명시해주세요.
    from pprint import pprint
    pprint(context)
    ti = context.get('ti')
    ti.xcom_push(key="asset_name", value="my_produce_asset")


@asset(
    dag_id='dags_asset_decorator_produce_2',
    schedule=None,
    tags=['asset'],
    uri='asset_by_decorator_2'
)
def my_produce_asset_2(self, context: Context):
    # 함수 내에서 자신이 produce 할 Asset에 접근할 때에는 self 변수를 활용할 수 있습니다.
    file_path = f"/opt/airflow/dags"
    py_cnt = 0
    for (root, dir, file) in os.walk(file_path):
        for f in file:
            if f.endswith('.py'):
                py_cnt += 1
    context.get('outlet_events').get(self).extra = {"py_cnt": py_cnt}
    # self를 통해 자기 자신이 발행하는 Asset 지정도 가능하나, 직접 이름을 넣어 찾는 것도 가능
    #context.get('outlet_events').get(Asset('my_produce_asset_2')).extra = {"py_cnt": py_cnt}

##########################################################################################################
# @asset 은 무조건 Asset을 produce 하는데에만 사용 가능합니다. (consume 작업은 불가)
# 그러나 다른 Asset 정보를 참조할 수는 있습니다.
##########################################################################################################

consume_asset_2 = Asset('my_produce_asset_2')

@asset(
    dag_id='dags_asset_decorator_produce_n_refer',
    schedule=None
)
def produce_and_refer(self, context: Context, my_produce_asset_2: Asset):
    from pprint import pprint
    print("::group::print context information")
    pprint(context)
    print("::endgroup::")

    # 다른 Asset(my_produce_asset_2) 을 참조합니다.
    extra_info = context['inlet_events'].get(my_produce_asset_2)[-1].extra
    print('my_produce_asset_2의 extra 정보:', str(extra_info))
