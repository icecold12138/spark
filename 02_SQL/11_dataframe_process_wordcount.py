#coding=utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import pandas as pd
import pyspark.sql.functions as F
if __name__=='__main__':
    #构建Sparksession对象
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc=spark.sparkContext
    rdd=sc.textFile('../data/words.txt').flatMap(lambda x:x.split(' ')).map(lambda x:[x])
    df=rdd.toDF(['word'])
    df.createTempView('words')
    spark.sql('select word,count(*) as cnt from words group by word order by cnt desc').show()
    #DSL风格处理
    df2=spark.read.format('text').load('../data/words.txt')
    #withcolumn方法
    #功能：对已经存在的列进行处理，返回一个新的列，如果名字和老列相同，那么替换，否则作为新列存在
    df3=df2.withColumn("value",F.explode(F.split(df2['value'],' ')))
    df3.groupby('value').count().withColumnRenamed('value','word').withColumnRenamed('count','cnt').orderBy('cnt',ascending=True).show()

