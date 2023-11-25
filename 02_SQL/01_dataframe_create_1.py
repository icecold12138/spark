#coding=utf8
from pyspark.sql import SparkSession
if __name__=='__main__':
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc=spark.sparkContext
    rdd=sc.textFile('../data/sql/people.txt').map(lambda x:x.split(',')).map(lambda x:(x[0],int(x[1])))
    #构建dataFrame对象
    #参数一：被转化的RDD
    #参数二：指定列名，通过list的形式指定，按照顺序提供字符串名称即可
    df=spark.createDataFrame(rdd,schema=['name','age'])
    #打印DataFrame结构
    df.printSchema()
    #打印df中的数据
    #参数一：表示展示多少数据，不传，默认是20
    #参数2，表示是否对列进行截断，数据列的长度超过20字符，后续内容不显示，默认是True，Fasle表示不截断，全部显示
    df.show(20,False)
    df.createOrReplaceTempView('people')
    spark.sql('select * from people where age<30').show()