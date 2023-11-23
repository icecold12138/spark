#coding=utf8
from pyspark import SparkConf,SparkContext
import os
import json
os.environ['HADOOP_CONF_DIR']='/export/server/hadoop/etc/hadoop'
os.environ['YARN_CONF_DIR']='/export/server/hadoop/etc/hadoop'
os.environ['PYSPARK_PYTHON']='/root/anaconda3/bin/python3'
if __name__=="__main__":
    conf=SparkConf().setAppName("test-yarn").setMaster("yarn")
    #如果提交到集群运行，除了主代码以外，还依赖了其他文件。需要设置一个参数，同步上传到集群中
    #参数的值可以是.py文件，也可以是zip压缩包
    conf.set("spark.submit.pyFiles","defs_9.py")
    sc = SparkContext(conf=conf)
    rdd=sc.textFile('hdfs://node1:8020/order.text')
    #进行rdd数据的split按照|符号进行切分
    json_rdd=rdd.flatMap(lambda x:x.split('|'))
    #完成json到字典的转化
    dict_rdd=json_rdd.map(lambda x:json.loads(x))
    beijing_rdd=dict_rdd.filter(lambda d:d['areaName']=='北京')
    #组合北京rdd和商品类型形成新的字符串
    new_rdd=beijing_rdd.map(lambda d:(d['areaName'],d['category'])).distinct().groupByKey().map(lambda x:(x[0],list(x[1])))
    print(new_rdd.collect())