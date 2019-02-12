from pyspark import SparkConf, SparkContext
import os

# os.environ["PYTHONPATH"]="/anaconda3/envs/airflow/bin/python"
# os.environ["PYSPARK_PYTHON"]="/anaconda3/envs/airflow/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"]="/anaconda3/envs/airflow/bin/python"
# os.environ["SPARK_HOME"]="/usr/local/spark-2.4.0-bin-hadoop2.7"

conf = SparkConf().setAppName("wordcount")
sc = SparkContext(conf=conf)

data=["hello","world","hello","word","count","count","hello"]

text_file = sc.parallelize(data)
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
output = counts.collect()

for (word, count) in output:
    print("%s: %i" % (word, count))

