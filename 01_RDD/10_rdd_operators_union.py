#coding=utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd1=sc.parallelize([1,1,2,3,4])
    rdd2=sc.parallelize(['a','b','c'])
    print(rdd1.union(rdd2).collect())