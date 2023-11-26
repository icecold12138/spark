#coidng=utf8
from pyspark.sql import SparkSession
from pyspark import SparkContext
import time
from pyspark.sql.types import StructType,StringType,IntegerType,TimestampType
if __name__=='__main__':
    #spark.sql.shuffle.partitions 参数指的是，在sql计算中，shuffle算子阶段默认的分区数是200个，对于集群模式来说，200个默认也算合适，如果在local下运行，200个很多，在调度上会带来额外消耗，建议调低/2/4
    spark=SparkSession.builder.appName('movie').master('local[*]').config("spark.sql.shuffle.partitions",2).getOrCreate()
    sc=spark.sparkContext
    rdd=sc.textFile('../data/u.data').map(lambda x:x.split('\t')).map(lambda x:(x[0],x[1],int(x[2]),int(x[3])))
    schema=StructType().add('customer_ID',StringType()).add('movie_ID',StringType()).add('score',IntegerType()).add('time',IntegerType())
    df=rdd.toDF(schema=schema)
    df.createOrReplaceTempView('movie')
    result1=spark.sql('select customer_ID,round(avg(score),2) as avscore from movie group by customer_ID order by avscore desc')
    result1.show()
    result2=spark.sql("select movie_ID,round(avg(score),2) as avscore from movie group by movie_ID order by avscore desc")
    result2.show()
    result3=spark.sql("select count(*) as number from movie where score>(select avg(score) from movie)")
    result3.show()
    result4=spark.sql("select round(avg(score),2) from (select customer_ID,score from movie where score>3) group by customer_ID order by count(*) desc limit 1")
    result4.show()
    result5=spark.sql("select customer_ID,round(avg(score),2),max(score),min(score) from movie group by customer_ID")
    result5.show()
    result6=spark.sql("select movie_ID,count(*) as cnt,round(avg(score),2) as avgscore from movie group by movie_ID having count(*)>100 order by avgscore desc limit 10")
    result6.show()
    time.sleep(10000)