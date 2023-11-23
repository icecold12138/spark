#coding=utf8
from pyspark import SparkConf,SparkContext
import time
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    sc.setCheckpointDir("hdfs://node1:8020/data/ckp")
    rdd=sc.textFile('../data/words.txt')
    rdd2=rdd.flatMap(lambda x:x.split(' '))
    rdd3=rdd2.map(lambda x:(x,1))
    rdd3.checkpoint()
    rdd4=rdd3.reduceByKey(lambda a,b:a+b)
    print(rdd4.collect())
    rdd5=rdd3.groupByKey()
    rdd6=rdd5.mapValues(lambda a:sum(a))
    print(rdd5.collect())
    time.sleep(10000)
