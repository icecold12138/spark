#coding=utf8
from pyspark.sql import SparkSession
if __name__=='__main__':
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    #通过SparkSession对象，获取SparkContext对象
    sc=spark.sparkContext
    rdd=sc.textFile('../data/stu_score.txt').map(lambda x:x.split(','))
    print(rdd.collect())
    df=spark.read.csv('../data/stu_score.txt',sep=',',header=False)
    df2=df.toDF('id','name','score')
    df2.printSchema()
    df2.show()

    df2.createTempView('score')
    spark.sql("""select * from score where name='语文' limit 5""").show()
    #DSL风格
    df2.where("name='语文'").limit(5).show()
