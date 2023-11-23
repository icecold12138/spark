#coding=utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([('a',1),('a',2),('a',3),('a',4),('e',1),('a',1),('f',6),('a',8),('a',9)])
    #注意：如果要全局有序，分区数设置为1
    print(rdd.sortBy(lambda x:x[1],ascending=True,numPartitions=3).collect())
    #按照key进行排序
    print(rdd.sortBy(lambda x:x[0],ascending=False,numPartitions=1).collect())