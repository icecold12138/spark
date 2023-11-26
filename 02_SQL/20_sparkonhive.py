#coding=utf8
import string
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType,ArrayType
import pandas as pd
import pyspark.sql.functions as F
if __name__=='__main__':
    #构建Sparksession对象
    spark=SparkSession.builder.appName('test').config('spark.sql.warehouse.dir','hdfs://node1:8020/user/hive/warehouse').\
        config('hive.metastore.uris','thrift://node1:9083').enableHiveSupport().config('spark.sql.shuffle.partitions',2).master('local[*]').getOrCreate()
    sc=spark.sparkContext
    spark.sql('select * from student').show()



