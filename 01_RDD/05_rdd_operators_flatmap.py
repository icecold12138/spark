#coding:utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize(["hadoop spark hadoop","spark hadoop hadoop","hadoop flink spark"])
    print(rdd.flatMap(lambda line:line.split(" ")).collect())