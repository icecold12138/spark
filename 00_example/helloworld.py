#conding:utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("worldcounthelloworld")
    sc=SparkContext(conf=conf)
    #读取文件
    file_add=sc.textFile("hdfs://node1:8020/words.txt")
    word_rdd=file_add.flatMap(lambda line:line.split(" "))
    #将单词转换为元组对象
    words_with_one_rdd=word_rdd.map(lambda x:(x,1))
    result_rdd=words_with_one_rdd.reduceByKey(lambda a,b:a+b)
    print(result_rdd.collect())