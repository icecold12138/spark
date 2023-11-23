#coding=utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([('a', 1), ('e', 1), ('a', 1), ('W', 2), ('q', 3), ('F', 4), ('f', 6), ('a', 8), ('a', 9)],3)
    print(rdd.sortByKey(ascending=True,numPartitions=1,keyfunc=lambda key:str(key).lower()).collect())