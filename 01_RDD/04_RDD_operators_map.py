#coding:utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([1,2,3,4,5,6],3)
    def add(data):
        return data*10
    print(rdd.map(add).collect())
    print(rdd.map(lambda data:data*10).collect())
    '''
    对于算子的接受函数来说，两种方法都可以
    lambda表达式适用于一行代码就能搞定的计算方法'''
    