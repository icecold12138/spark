#coding=utf8
from pyspark import SparkConf,SparkContext
import json
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.textFile('../data/order.text')
    #进行rdd数据的split按照|符号进行切分
    json_rdd=rdd.flatMap(lambda x:x.split('|'))
    #完成json到字典的转化
    dict_rdd=json_rdd.map(lambda x:json.loads(x))
    beijing_rdd=dict_rdd.filter(lambda d:d['areaName']=='北京')
    #组合北京rdd和商品类型形成新的字符串
    new_rdd=beijing_rdd.map(lambda d:(d['areaName'],d['category'])).distinct().groupByKey().map(lambda x:(x[0],list(x[1])))
    print(new_rdd.collect())