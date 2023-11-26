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
    df=spark.read.format('csv').option('sep',';').option('header',True).\
        option('encoding','utf-8').schema('name STRING,age INT,job STRING').load('../data/sql/people.csv')
    df.dropDuplicates().dropna()
    df.createOrReplaceTempView('stu')
    #聚合窗口函数的使用
    spark.sql('select *,avg(age) over() as avg_age from stu').show()
    #排序窗口函数,rank over,DENSE_RANK over ROW_NUMBER over
    spark.sql('''select *,ROW_NUMBER() OVER(ORDER BY age desc) as row_number_rank,DENSE_RANKE() OVER(partition by class order by score desc) as dense_rank,
              rank() over(order by score) as rank from stu''').show()
    #NTILE窗口函数
    spark.sql('''select *,NTILE(6) over(order by score desc) from stu''')


