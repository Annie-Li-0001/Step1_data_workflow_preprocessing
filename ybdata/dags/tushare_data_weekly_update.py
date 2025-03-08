from airflow.operators.empty import EmptyOperator
from meta.daily_task.chief_task import ChiefDataTask
from datetime import datetime, timedelta
from airflow.operators.mysql_operator import MySqlOperator
import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

local_tz = pendulum.timezone("Asia/Shanghai")
with DAG(
    dag_id="download_tushare_stock_weekly_market_data_task",
    default_args={
        "depends_on_past": False,
        # 'email': ['william_sun1990@163.com'],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 10,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    # [END default_args]
    description="下载Tushare股票相关数据",
    concurrency=8,
    # schedule=timedelta(days=1),
    start_date=datetime(2023, 5, 5, tzinfo=local_tz),
    schedule_interval="0 0 * * 6",
    catchup=False,
    tags=["量化投资 股票周频数据下载任务"],
) as dag:
    dag.doc_md = """
        下载tushare股票行情数据
        """
    run_this_first = EmptyOperator(task_id="run_this_first")

    # task_list = [fetch_close, fetch_close_hfq, fetch_close_qfq, fetch_daily_basic]

    # join = EmptyOperator(task_id='join', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    chief = ChiefDataTask()
    task_dict = {}
    for task in chief.filter_weekly_tasks():
        task_dict[task.table] = task

    airflow_task_cache = {}
    for task_name in task_dict:
        task_class = task_dict[task_name]

        if task_class.parents is None:
            task = task_class()
            sql_task = MySqlOperator(
                task_id=f"create_table_weekly_{task.table}",
                mysql_conn_id="quant_db",
                sql=task.sql_task(),
            )
            data_task_id = f"download_tushare_data_{task.table}"
            data_task = PythonOperator(
                task_id=data_task_id,
                python_callable=task.data_task,
                provide_context=True,
            )
            airflow_task_cache[data_task_id] = data_task
            run_this_first >> Label(task.description) >> sql_task >> data_task
        else:
            for parent_name in task_class.parents:
                parent_class = task_dict[parent_name]
                parent = parent_class()
                parent_data_task_id = f"download_tushare_data_{parent.table}"
                parent_data_task = airflow_task_cache[parent_data_task_id]

                child = task_class()
                sql_task = MySqlOperator(
                    task_id=f"create_table_weekly_{child.table}",
                    mysql_conn_id="quant_db",
                    sql=child.sql_task(),
                )
                child_data_task_id = f"download_tushare_data_{child.table}"
                data_task = PythonOperator(
                    task_id=child_data_task_id,
                    python_callable=child.data_task,
                    provide_context=True,
                )
                airflow_task_cache[child_data_task_id] = data_task
                parent_data_task >> Label(child.description) >> sql_task >> data_task
