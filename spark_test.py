from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

import testoperator.conf as conf
import testoperator.printdate as printdate

#定义DAG默认参数。
# 注意start_date要设置为上线前的前一天，比如2019-02-12上线运行，start_date要设置为2019-02-11，这样
# 在2019-02-12日DAG运行时处理的是2019-02-11日的数据。
# provide_context 设置为true是因为任务之间有可能需要传递值，比如提交job到emr需要从上一个createemr的任务获取clusterid。

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019,2,10),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# 初始化DAG
# concurrency定义了在DAG内能并发允许的任务个数。
# schedule_interval可以采用和crontab相似的格式来指定运行的时间间隔
# user_defined_macros 定义了公共配置，这里是在单独的conf.py定义然后导入使用的。
dag = DAG('spark_test',concurrency=5,schedule_interval=None,
          default_args=default_args,user_defined_macros=conf.__dict__)

# 定义一个空任务作为起始任务
start = DummyOperator(task_id='start',queue='script',dag=dag)


# 使用SparkSubmitOperator定义执行spark作业的任务
# 和作业有关的所有参数和配置都在_config字段中定义
# tk-dev-emr-airflow-spark是定义的emr spark连接，该连接需要在airflow admin web页面定义
# application_args可用于指定pyspark脚本中自定义的参数
_config = {
    'name': '{{ ti.task_id }}',
    'application': '/server/airflow/dags/testoperator/wordcount.py',
    'executor_cores': 2,
    'executor_memory': '12g',
    'application_args': [
        '-fid', '{{ ti.job_id }}',
    ]
}

spark_task1 = SparkSubmitOperator(
    task_id='spark_task1',
    conn_id='tk_dev_dw_spark',
    queue='script',
    dag=dag,
    **_config
)




# 定义任务之间依赖关系
start >> spark_task1

