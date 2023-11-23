#coding=utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd1=sc.parallelize([(1001,'张三'),(1002,'李四'),(1003,'王五'),(1004,'赵六')])
    rdd2=sc.parallelize([(1001,'销售部'),(1002,'科技部')])
    #对于join算子，关联条件，按照key来进行关联
    print(rdd1.join(rdd2).collect())
    #左外连接
    print(rdd1.leftOuterJoin(rdd2).flatMap(lambda x:x).collect())
    #右外连接
    print(rdd1.rightOuterJoin(rdd2).collect())