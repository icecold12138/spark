#coding=utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([1,2,3,5,7,4,2,4,8],3)
    rdd.saveAsTextFile('hdfs://node1:8020/data/out1')