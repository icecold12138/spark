#coding=utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType,ArrayType
import pandas as pd
import pyspark.sql.functions as F
if __name__=='__main__':
    #构建Sparksession对象
    spark=SparkSession.builder.appName('test').config('spark.sql.shuffle.partitions',2).master('local[*]').getOrCreate()
    sc=spark.sparkContext
    rdd=sc.parallelize([['hadop spark flink'],['hadoop flink java']])
    df=rdd.toDF(['line'])
    def split_line(data):
        return data.split(' ')
    udf2=spark.udf.register('udf1',split_line,ArrayType(StringType()))
    df.select(udf2(df['line'])).show()
    df.createOrReplaceTempView('lines')
    spark.sql('select udf1(line) from lines').show(truncate=False)

