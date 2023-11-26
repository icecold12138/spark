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
    #write text写出，只能写出一个列数据，需要将df转换为单列
    df.select(F.concat_ws("---",'customer_ID','movie_ID','score','time')).\
        write.\
        mode("overwrite").\
        format('text').\
        save('../data/output/text')
    #csv
    df.write.mode("overwrite").format('csv').option('header',True).option('sep',';').save('../data/output/csv')
    #json
    df.write.mode('overwrite').format('json').save('../data/output/json')
    #parquet
    df.write.mode('overwrite').format('parquet').save('../data/output/parquet')

