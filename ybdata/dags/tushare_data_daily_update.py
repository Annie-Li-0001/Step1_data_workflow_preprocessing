# import random
# from meta.daily_task.stock_market import fetch_close, fetch_daily_basic, fetch_close_hfq, fetch_close_qfq
from airflow.operators.empty import EmptyOperator
from meta.daily_task.chief_task import ChiefDataTask
from meta.daily_task.meta_data import DataTask
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
    dag_id="download_tushare_stock_daily_market_data_task",
    default_args={
        "depends_on_past": False,
        # 'email': ['william_sun1990@163.com'],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 20,
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
    concurrency=4,
    # schedule=timedelta(days=1),
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    schedule_interval="0 21 * * *",
    catchup=False,
    tags=["股票行情数据下载任务"],
    # tags=['量化投资 股票日频数据下载任务'],
) as dag:
    dag.doc_md = """
        下载tushare股票行情数据
        """

    # 先构造一个起点operator，没有实质性任务，只是为了在页面dag中画图有一个起点
    run_this_first = EmptyOperator(task_id="run_this_first")

    # task_list = [fetch_close, fetch_close_hfq, fetch_close_qfq, fetch_daily_basic]

    # join = EmptyOperator(task_id='join', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    chief = (
        ChiefDataTask()
    )  # ChiefDataTask是一个代理类，可以获取到Meta中所有以PrefixData开头的任务类class
    task_dict = {}
    for task in chief.filter_daily_tasks():
        # 先将所有以PrefixData开头的任务类放到一个 dict字典 中 task_dict，这个很重要，后面要用到
        task: DataTask = task
        if not task.turn_on:
            # 如果turn_on属性为False的就跳过，表示没有启动
            continue
        # ！！！ 注意：我们这里是用的每一个task的table来当key的，所以每一个task的table表名称都不能重复，一个任务存到一个表里面
        task_dict[task.table] = task

    airflow_task_cache = {}
    for task_name in task_dict:
        # 循环扫描一遍task的字典
        # 1.先获取task_class的类
        task_class = task_dict[task_name]
        # 2.然后找出没有爸爸的task，优先构造airflow中的第一层任务（这一层任务没有实质性的上层依赖）
        """
        Airflow操作器（Operator）是在Airflow中定义的任何工作流的核心组件。
        
        操作器代表一个独立运行的单个任务，不与其他任务共享任何信息。操作器可以执行各种操作，
        如运行Python函数、执行Bash命令、执行SQL查询、触发API、发送电子邮件和执行条件操作。
        在Airflow文档中，操作器和任务术语可以互换使用。任务用于管理在DAG中执行操作器的执行。
        
        Airflow操作器的类型 
        
        Airflow具有各种内置的操作器，可以执行所需的操作，如运行Python函数、执行Bash命令、执行SQL查询、触发API、发送电子邮件和执行条件操作。
        让我们看一些Airflow操作器的示例：
        
        BashOperator：用于执行Bash命令。 
        PythonOperator：用于运行Python可调用对象或Python函数。 
        EmailOperator：用于向接收者发送电子邮件。 
        MySqlOperator：用于在MySQL数据库中运行SQL查询。 
        S3ToHiveOperator：用于将数据从Amazon S3传输到Hive。 
        HttpOperator：用于触发HTTP端点。 
        BranchingOperator：类似于PythonOperator，但它期望python_callable返回一个task_id。
        EmptyOperator: 一个空的operator
        这些操作器是从基类BaseOperator创建的，它定义了所有必需的属性和方法。
        """
        if task_class.parents is None:
            task = task_class()  # 根据task类来构造task实例
            sql_task = MySqlOperator(
                task_id=f"create_table_daily_{task.table}",  # 名字task_id
                mysql_conn_id="quant_db",
                sql=task.sql_task(),
            )  # 构造airflow中的MysqlOperator
            data_task_id = f"download_tushare_data_{task.table}"  # 名字task_id
            data_task = PythonOperator(
                task_id=data_task_id,
                python_callable=task.data_task,
                provide_context=True,
            )  # 这是python任务的执行器，用于执行数据同步任务和因子等计算任务
            airflow_task_cache[data_task_id] = (
                data_task  # 这一步是为了承上启下，因为目前这个if中的都是没有爸爸的任务，这些任务data_task都可能有下游的任务，所以直接把任务存到airflow_task_cache字典中
            )
            run_this_first >> Label(task.description) >> sql_task >> data_task
        else:
            #  else中处理的都是有爸爸的下游任务task_class，这里的都是儿子task
            for (
                parent_name
            ) in task_class.parents:  # 这里的task_class都是有爸爸的，循环的处理爸爸
                parent_class = task_dict[
                    parent_name
                ]  # 先把爸爸找出来，注意这里是一个class
                parent = parent_class()  # 将爸爸类进行实例化
                parent_data_task_id = (
                    f"download_tushare_data_{parent.table}"  # 给爸爸取一个名字task_id
                )
                parent_data_task = airflow_task_cache[
                    parent_data_task_id
                ]  # 从airflow_task_cache中获取爸爸任务task,注意这时候已经是一个operator的task了

                child = task_class()  # else中的儿子task
                sql_task_id = f"create_table_daily_{child.table}"

                # 下面的逻辑是为了处理sql_task和data_task的连接而设计的，理解起来会有一些难度，可以照抄
                if sql_task_id in airflow_task_cache:
                    sql_task = airflow_task_cache[sql_task_id]
                else:
                    sql = child.sql_task()
                    if sql:
                        sql_task = MySqlOperator(
                            task_id=sql_task_id,
                            mysql_conn_id="quant_db",
                            sql=child.sql_task(),
                        )
                        airflow_task_cache[sql_task_id] = sql_task
                    else:
                        sql_task = EmptyOperator(task_id=sql_task_id)
                        airflow_task_cache[sql_task_id] = sql_task

                child_data_task_id = f"download_tushare_data_{child.table}"
                if child_data_task_id in airflow_task_cache:
                    data_task = airflow_task_cache[child_data_task_id]
                else:
                    data_task = PythonOperator(
                        task_id=child_data_task_id,
                        python_callable=child.data_task,
                        provide_context=True,
                    )
                    airflow_task_cache[child_data_task_id] = data_task

                # 有了上面的逻辑之后，就确定了sql_task和data_task了，也就可以构造任务链条了
                parent_data_task >> Label(child.description) >> sql_task >> data_task
