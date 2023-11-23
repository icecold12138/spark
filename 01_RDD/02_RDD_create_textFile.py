#coding:utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    #读取本地文件
    file_rdd1=sc.textFile('../data/words.txt')
    print("默认读取分区数：",file_rdd1.getNumPartitions())
    print("file_rdd1的内容是：",file_rdd1.collect())
    file_rdd2=sc.textFile("../data/words.txt",3)
    file_rdd3 = sc.textFile("../data/words.txt", 100)
    print("file_rdd2分区数是：",file_rdd2.getNumPartitions())
    print("file_rdd3分区数是：",file_rdd3.getNumPartitions())
    hdfs_rdd=sc.textFile("hdfs://node1:8020/words.txt")
    print("hdfs_rdd的内容是：",hdfs_rdd.collect())