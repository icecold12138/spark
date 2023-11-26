#coding=utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd
import pyspark.sql.functions as F
if __name__=='__main__':
    #构建Sparksession对象
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc=spark.sparkContext
    schema = StructType().add('customer_ID', StringType()).add('movie_ID', StringType()).add('score',IntegerType()).add('time',IntegerType())
    df=spark.read.format('csv').option('sep','\t').option('header',False).schema(schema=schema).load('../data/sql/u.data')
    #1导出df到mysql数据库中
    df.write.mode('overwrite').format('jdbc').\
        option('url','jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true').\
        option('dbtable','movie_data').\
        option('user','root').\
        option('password','123456').\
        save()
    '''
    JDBC写出，会自动创建表的
    因为dataframe中有表结构信息，structType记录各个字段的名称类型和是否运行为空'''
    df=spark.read.format('jdbc').option('url','jdbc:mysql://node1:3306/bigdata?useSSl=false&useUnicode=true').\
        option('dbtable','movie_data').\
        option('user','root').\
        option('password','123456').\
        load()
    df.show()
