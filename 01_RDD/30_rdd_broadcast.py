#coding=utf8
from pyspark import SparkConf,SparkContext
if __name__=="__main__":
    conf=SparkConf().setAppName("test").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    stu_info_list=list()
    f=open('../data/stu_info.txt','r',encoding='UTF-8')
    for line in f.readlines():
        r=line.strip().split(',')
        stu_info_list.append((int(r[0]),r[1]))
    f.close()
    #将本地python对象标记为广播变量
    broadcast=sc.broadcast(stu_info_list)
    score_info_rdd=sc.textFile('../data/stu_score.txt').map(lambda x:x.split(',')).map(lambda x:(int(x[0]),x[1],int(x[2])))
    def map_func(data):
        id=data[0]
        name=''
        for stu_info in broadcast.value:
            stu_id=stu_info[0]
            if id==stu_id:
                name=stu_info[1]
        return (name,data[1],data[2])
    print(score_info_rdd.map(map_func).collect())