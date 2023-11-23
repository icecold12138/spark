#coding=utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([1,2,3,5,7,4,2,4,8],3)
    def process(iter):
        result=list()
        for it in iter:
            result.append(it*10)
        return result
    print(rdd.mapPartitions(process).collect())