#coding=utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
if __name__=='__main__':
    #构建Sparksession对象
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    df=spark.read.format('csv').schema('id INT,subject STRING,score INT').load('../data/sql/stu_score.txt')
    #column对象的获取
    id_column=df['id']
    subject_column=df['subject']
    df.select(['id','subject']).show()
    df.select('id','subject').show()
    df.select(id_column,subject_column).show()
    df.filter('score<99').show()
    df.filter(df['score']<99).show()
    df.where('score<99').show()
    df.where(df['score']<99).show()
    #他是一个有分组关系的数据结构，有一组API供我们分组聚合，类似sum avg max min count
    #调用聚合方法后，才返回一个dataframe
    df.groupby('subject').count().show()
    df.groupby(df['subject']).count().show()
