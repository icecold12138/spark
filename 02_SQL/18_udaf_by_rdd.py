#coding=utf8
import string

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType,ArrayType
import pandas as pd
import pyspark.sql.functions as F
if __name__=='__main__':
    #构建Sparksession对象
    spark=SparkSession.builder.appName('test').config('spark.sql.shuffle.partitions',2).master('local[*]').getOrCreate()
    sc=spark.sparkContext
    rdd=sc.parallelize([1,2,3,4,5],3)
    df=rdd.map(lambda x:[x]).toDF(['num'])
    #使用RDD的mapPartitions完成聚合
    #如果用mapPartitions API完成UDF聚合，一定要单分区
    single_partition_rdd=df.rdd.repartition(1)
    def process(iter):
        sum=0
        for row in iter:
            sum+=row['num']
        return [sum]
    print(single_partition_rdd.mapPartitions(process).collect())