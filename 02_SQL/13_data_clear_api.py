#coding=utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import pandas as pd
import pyspark.sql.functions as F
if __name__=='__main__':
    #构建Sparksession对象
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc=spark.sparkContext
    df=spark.read.format('csv').option('sep',';').option('header',True).load('../data/sql/people.csv')
    #dropDuplicates无参数使用完成整体去重
    '''df.dropDuplicates().show()
    df.dropDuplicates(['age','job']).show()
    #数据清洗：缺失值处理
    #无参数使用，只要列中有空，就删除这一行
    df.dropna().show()
    #thresh=3,表示必须满足三个有效列，不满足则删除
    df.dropna(thresh=3).show()
    #表示name 和age两列要达到2列有效
    df.dropna(thresh=2,subset=['name','age']).show()'''
    #缺失值填充,dataFrame.fillna
    df.fillna("loss").show()
    #指定列进行填充
    df.fillna("N/A",subset=['job']).show()
    #设定一个字典，对所有列定义填充规则
    df.fillna({'name':'未知姓名','age':1,'job':'worker'}).show()