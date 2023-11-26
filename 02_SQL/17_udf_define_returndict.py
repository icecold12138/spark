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
    #假设有数字123，传入数字，返回数字所在字母，然后和数字结合形成的dict返回
    rdd=sc.parallelize([[1],[2],[3]])
    df=rdd.toDF(['num'])
    def process(data):
        return {'num':data,'letters':string.ascii_letters[data]}
    #UDF返回值是字典，需要用StructType接受
    udf2=spark.udf.register('udf1',process,StructType().add('num',IntegerType()).add('letters',StringType()))
    df.selectExpr('udf1(num)').show(truncate=False)