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
dag = DAG('testoperators_dag',concurrency=5,schedule_interval=None,
          default_args=default_args,user_defined_macros=conf.__dict__)

# 定义一个空任务作为起始任务
start = DummyOperator(task_id='start',queue='script',dag=dag)

# 使用PythonOperator定义执行python函数的任务
def displaydate(**kwargs):
    date=printdate.displaydate()
    return date

print_date = PythonOperator(
    task_id='print_date',
    python_callable=displaydate,
    queue='script',
    dag=dag)

# 使用HiveOperator定义执行hive脚本的任务
# tk-dev-emr-airflow-hive是定义的emr hive连接，该连接需要在airflow admin web页面定义。
# hiveconfs定义了hive脚本中的参数
# hive脚本内容：
# select * from ${hiveconf:tblname} where createdate='${hiveconf:createdate}';
# {{ ds }} 表示取airflow作业的执行日期

# 如果把hiveconf_jinja_translate设置为true，可以使用自定义的dict配置，比如
# 上面DAG定义里的user_defined_macros定义的conf.__dict__。
hive_task1 = HiveOperator(
    task_id='hive_task1',
    hql='./hive1.sql',
    schema='security',
    hive_cli_conn_id='tk-dev-emr-airflow-hive',
    hiveconf_jinja_translate=False,
    hiveconfs={
        'tblname':'scanresult',
        'createdate': '{{ ds }}'
    },
    queue='emr',
    dag=dag
)

# 使用SparkSubmitOperator定义执行spark作业的任务
# 和作业有关的所有参数和配置都在_config字段中定义
# tk-dev-emr-airflow-spark是定义的emr spark连接，该连接需要在airflow admin web页面定义
# application_args可用于指定pyspark脚本中自定义的参数
_config = {
    'name': '{{ task_instance.task_id }}',
    'application': '/Users/huiwang/airflow/dags/testoperators/wordcount.py',
    # 'conf': {
    #     'parquet.compression': 'SNAPPY'
    # },
    # 'files': 'hive-site.xml',
    # 'py_files': 'sample_library.py',
    # 'driver_classpath': 'parquet.jar',
    # 'jars': 'parquet.jar',
    # 'packages': 'com.databricks:spark-avro_2.11:3.2.0',
    # 'exclude_packages': 'org.bad.dependency:1.0.0',
    # 'repositories': 'http://myrepo.org',
    # 'total_executor_cores': 4,
    'executor_cores': 1,
    'executor_memory': '1g'
    # 'keytab': 'privileged_user.keytab',
    # 'principal': 'user/spark@airflow.org',
    # 'num_executors': 10,
    # 'verbose': True,
    # 'driver_memory': '3g',
    # 'java_class': 'com.foo.bar.AppMain',
    # 'application_args': [
    #     '-f', 'foo',
    #     '--bar', 'bar',
    #     '--start', '{{ macros.ds_add(ds, -1)}}',
    #     '--end', '{{ ds }}'
    # ]
}

spark_task1 = SparkSubmitOperator(
    task_id='spark_task1',
    conn_id='tk-dev-emr-airflow-spark',
    queue='emr',
    dag=dag,
    **_config
)

# 使用BashOperator定义运行hive脚本的任务
# 使用params来指定hive脚本中的参数
# hive脚本中的内容：
# select * from ${tblname} where createdate='${createdate}';
exec_date='{{ ds }}'

hive_task2 = BashOperator (
    task_id='hive_task2',
    bash_command = 'hive --database {{params.database}} -f {{params.hql_file}} '
                   '-hivevar tblname={{params.tblname}} -hivevar createdate='+exec_date,
     params = {
         'database':'security',
         'hql_file':'/Users/huiwang/airflow/dags/testoperators/hive2.sql',
         'tblname':'scanresult'
         # 'createdate': '{{ ds }}'
     },
    queue='emr',
     dag = dag
)

# 使用BashOperator定义运行spark程序的任务
# 使用params来指定pyspark主程序中的参数
spark_task2 = BashOperator(
    task_id='spark_task2',
    bash_command ='spark-submit --master yarn --name {{params.name}} /Users/huiwang/airflow/dags/testoperators/wordcount.py',
    params={'name':'spark_task2'},
    # bash_command='spark-submit --class {{ params.class }} {{ params.jar }}',
    # params={'class': 'MainClassName', 'jar': '/path/to/your.jar'},
    queue='emr',
    dag=dag
)

# 定义任务之间依赖关系
start >> print_date
start >> hive_task1
start >> hive_task2
start >> spark_task1
start >> spark_task2
