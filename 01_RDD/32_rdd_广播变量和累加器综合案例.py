#coding=utf8
from pyspark import SparkConf,SparkContext
import re
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    #读取文件
    file_rdd=sc.textFile('../data/accumulator_broadcast_data.txt')
    abnormal_char=[',','.','!','#','$','%']
    #特殊字符包装成广播变量
    broadcast = sc.broadcast(abnormal_char)
    #对特殊字符出现次数出现累加，累加使用累加器
    acmlt=sc.accumulator(0)
    #数据处理,先处理数据的空行，有内容就是True,None就是False
    line_rdd=file_rdd.filter(lambda line:line.strip())
    data_rdd=line_rdd.map(lambda line:line.strip())
    #对数据进行切分，按照正则表达式式切分，
    word_rdd=data_rdd.flatMap(lambda line:re.split("\s+",line))
    #当前rdd中有正常单词，也有特殊符号
    def filter_func(data):
        global acmlt
        #取出广播变量中特殊符号list
        abnormal_chars=broadcast.value
        if data in abnormal_chars:
            acmlt+=1
            return False
        else:
            return True
    #过滤特殊字符
    normal_word_rdd=word_rdd.filter(filter_func)
    result_rdd=normal_word_rdd.map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)
    print('正常单词技术结果：',result_rdd.collect())
    print('特殊单词数量：',acmlt)