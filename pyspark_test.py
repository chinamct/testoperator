from pyspark.sql.functions import *
from pyspark.sql import Window
from pyspark.sql import SparkSession
from datetime import datetime

# df = spark.read \
#           .format("s3selectCSV") \
#           .options(compression='gzip', multiline='true') \
#           .load("s3://bnb-datalake/blockchain/bitcoin/raw_archive/")

spark = SparkSession\
        .builder\
        .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")\
        .config("spark.sql.session.timeZone", "UTC")\
        .enableHiveSupport()\
        .getOrCreate()

# incremental etl
height_max = spark.sql("select max(height) height from blockchain.btc_header where dt >= '2018-11-25'").collect()[0][0]
print("max height: {}".format(height_max))
