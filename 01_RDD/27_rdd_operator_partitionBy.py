#coding=utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([('hadoop',1),('spark',1),('hello',1),('flink',1)])
    def process(k):
        if 'hadoop'==k or 'hello'==k:return 0
        elif 'spark'==k:return 1
        return 2
    print(rdd.partitionBy(3,process).glom().collect())