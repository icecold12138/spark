#coding=utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd
import pyspark.sql.functions as F
if __name__=='__main__':
    #构建Sparksession对象
    spark=SparkSession.builder.appName('test').config('spark.sql.shuffle.partitions',2).master('local[*]').getOrCreate()
    sc=spark.sparkContext
    rdd=sc.parallelize([1,2,3,4,5,6,7]).map(lambda x:[x])
    df=rdd.toDF(['num'])
    #方式1：spark.udf.register()
    def num_ride_10(num):
        return num*10
    #参数1：注册的UDF名称，仅用于SQL风格
    #参数2：UDF的处理逻辑，是一个单独的方法
    #参数3：声明UDF的返回值类型，注意：UDF注册的时候，必须声明返回值类型，并且UDF的真实返回值一定要和声明的返回值一致
    #返回值对象，是一个UDF对象，仅用于DSL语法
    #当前这种方式定义UDF，可以通过参数1名称用于sQL风格，通过返回值对象用于DSL风格
    udf2=spark.udf.register('udf1',num_ride_10,IntegerType())
    #SQL风格使用
    #selectExpr，以select表达式执行，表达SQL风格表达式
    #select 方法，接受普通的字段名，或者返回值是column的对象的计算
    df.selectExpr('udf1(num)').show()
    #DSL风格
    #返回值UDF对象，如果作为方法使用，传入的参数一定是column对象
    df.select(udf2(df['num'])).show()

