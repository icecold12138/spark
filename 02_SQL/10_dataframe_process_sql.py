#coding=utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
if __name__=='__main__':
    #构建Sparksession对象
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    df=spark.read.format('csv').schema('id INT,subject STRING,score INT').load('../data/sql/stu_score.txt')
    #注册成临时视图
    df.createTempView('score')
    #注册或者替换临时视图，存在就替换
    df.createOrReplaceTempView('score_2')
    #创建全区临时视图，可供多个sparksession使用
    df.createGlobalTempView('score_3')
    #使用sql API完成sql语句的执行
    spark.sql('select subject,count(*) as ant from score group by subject').show()
    spark.sql('select subject,count(*) as ant from score_2 group by subject').show()
    spark.sql('select subject,count(*) as ant from global_temp.score_3 group by subject').show()
