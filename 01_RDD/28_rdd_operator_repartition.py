#coding=utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([1,2,34,54,6,5],3)
    print(rdd.repartition(1).glom().collect())
    print(rdd.repartition(5).glom().collect())
    #修改分区
    print(rdd.coalesce(1).collect())
    print(rdd.coalesce(5,shuffle=True).collect())
