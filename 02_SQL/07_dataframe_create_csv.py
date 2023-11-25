#coding=utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
if __name__=='__main__':
    #构建Sparksession对象
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    #读取csv文件构建dataframe
    df=spark.read.format('csv').option('sep',';').option('header',True).option('encoding','utf-8').schema('name STRING,age INT,job STRING').load('../data/sql/people.csv')
    df.printSchema()
    df.show()

