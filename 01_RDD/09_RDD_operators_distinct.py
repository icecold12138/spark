#coding=utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([1,1,2,3,4,1,2,3,1])
    print(rdd.distinct().collect())
    rdd2=sc.parallelize([('a',1),('a',1),('b',2),('b',1),('b',1)])
    print(rdd2.distinct().collect())