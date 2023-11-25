#coding=utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
if __name__=='__main__':
    #构建Sparksession对象
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    #读取json文件构建dataframe
    df=spark.read.format('json').load('../data/sql/people.json')
    df.printSchema()
    df.show()

