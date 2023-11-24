#coding=utf8
from pyspark.storagelevel import StorageLevel
from pyspark import SparkConf,SparkContext
from defs import context_jieba,filter_words,append_words,extract_user_and_word
from operator import add
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    rdd=sc.textFile('../../data/SogouQ.txt')
    #对数据进行切分
    split_rdd=rdd.map(lambda x:x.split('\t'))
    #spilt_RDD会多次使用，使用缓存存储
    split_rdd.persist(StorageLevel.DISK_ONLY)
    #需求一：用户搜索的关键词分析
    #主要分析热点词语,将所有内容取出
    context_rdd=split_rdd.map(lambda x:x[2])
    #对搜索内容进行分词处理
    words_rdd=context_rdd.flatMap(context_jieba)
    #院校 帮-->院校帮
    #博学 谷-->博学谷
    filtered_rdd=words_rdd.filter(filter_words)
    final_words_rdd=filtered_rdd.map(append_words)
    #对单词进行分组聚合,求出前五名
    result1=final_words_rdd.reduceByKey(lambda a,b:a+b).sortBy(lambda x:x[1],ascending=False,numPartitions=1).take(5)
    print('需求一结果：',result1)
    #用户和关键词组合分析
    #1.我喜欢传智播客
    #1+我，1+喜欢，1+传智播客
    user_content_rdd=split_rdd.map(lambda x:(x[1],x[2]))
    #对用户搜索内容进行切分
    user_word_with_one_rdd=user_content_rdd.flatMap(extract_user_and_word)
    result2=user_word_with_one_rdd.reduceByKey(lambda a,b:a+b).sortBy(lambda x:x[1],ascending=False,numPartitions=1).take(5)
    print('需求二结果：',result2)
    #需求三：热门搜索时间段分析
    #取出所有时间
    time_rdd=split_rdd.map(lambda x:x[0])
    #只保留小时精度即可
    result3=time_rdd.map(lambda x:(x[0:2],1)).reduceByKey(add).sortBy(lambda x:x[1],ascending=False,numPartitions=1).collect()
    print('需求三结果：',result3)
    split_rdd.unpersist()