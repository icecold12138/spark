#coding=utf8
from pyspark.storagelevel import StorageLevel
from pyspark import SparkConf,SparkContext
from operator import add
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.textFile('../../data/apache.log')
    rdd.persist(StorageLevel.DISK_ONLY)
    print('当前网站访问的次数是：',rdd.count())
    result2=rdd.map(lambda x:x.split(' ')).map(lambda x:x[1]).distinct().count()
    print('当前访问用户数是：',result2)
    result3=rdd.map(lambda x:x.split(' ')).map(lambda x:x[0]).distinct().collect()
    print('访问的IP是多少：',result3)
    #切分取页面,并组合成二元元组
    rdd2=rdd.map(lambda x:x.split(' ')).map(lambda x:(x[4],1))
    #按照页面地址分组聚合
    rdd3=rdd2.reduceByKey(add)
    #取第一名
    result4=rdd3.takeOrdered(1,lambda x:-x[1])
    print('访问量最多的页面是：',result4)
    rdd.unpersist()