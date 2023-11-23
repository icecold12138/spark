#coding=utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([('a',1),('b',2),('a',3),('b',1),('b',1)])
    #传入函数意思是，通过这个函数，确定按照谁来分组,分组结果为二元元组
    print(rdd.groupBy(lambda t:t[0]).map(lambda t:(t[0],list(t[1]))).collect())
