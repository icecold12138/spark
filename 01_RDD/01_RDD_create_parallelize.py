#conding:utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.parallelize([1,2,3,4,5,6,7,8,9])
    #parallelize没有指定分区数，默认分区是多少
    print("默认分区：",rdd.getNumPartitions())
    rdd=sc.parallelize([1,2,3,4,5,6,7,8,9],3)
    print("分区数：",rdd.getNumPartitions())
    #collect方法是将rdd分区的数据，都发送给driver，形成一个python list对象
    #collect：分布式-->本地对象
    print("rdd的内容是：",rdd.collect())
