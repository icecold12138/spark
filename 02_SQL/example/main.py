#coding=utf8
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
'''
需求一：各省销售额统计
需求二：TOp3销售省份中，有多少店铺达到过日销售额1000+
需求三：TOP3省份中，各省的平均单单价
需求四：TOP3省份中，各个省份的支付类型比例
receivable :订单金额
storeProvince:店铺省份
dateTS:订单的销售日期
payType:支付类型
storeID;店铺ID'''
if __name__=='__main__':
    spark=SparkSession.builder.appName('example').master('local[*]').config('spark.sql.shuffle.partitions','2').\
        config('spark.sql.warehouse.dir','hdfs://node1:8020/user/hive/warehouse').\
        config('hive.metastore.uris','thrift://node1.itcast.cn:9083').\
        enableHiveSupport().getOrCreate()
    #读取数据
    df=spark.read.format('json').load('../../data/mini.json').dropna(thresh=1,subset=['storeProvince']).filter("storeProvince!='null'").\
        filter('receivable<10000').\
        select('storeProvince','storeID','receivable','dateTs','payType')
    #缺失值处理，省份信息
    #需求一：销售额统计
    result1=df.groupby('storeProvince').sum('receivable').withColumnRenamed('sum(receivable)','money').\
        withColumn('money',F.round('money',2)).orderBy('money',ascending=False)
    #写出结果到mysql
    #写出结果到hive
    '''result1.write.mode('overwrite').\
        format('jdbc').\
        option('url','jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&characterEncoding=utf8').\
        option('user','root').\
        option('dbtable','province_sale').\
        option('password','123456').\
        option('encoding','utf-8').save()'''
    #result1.write.mode('overwrite').saveAsTable('default.province_sale','parquet')
    #先找到TOP3的销售省份
    top3_province_df=result1.limit(3).select('storeProvince').withColumnRenamed('storeProvince','top3_province')
    top3_province_df_joined=df.join(top3_province_df,on=df['storeProvince']==top3_province_df['top3_province'])
    top3_province_df_joined.persist(StorageLevel.DISK_ONLY)
    '''province_hot_store_count_df=top3_province_df_joined.groupby('storeProvince','storeID',F.from_unixtime(df['dateTs'].substr(0,10),'yyyy-MM-dd').alias('day')).\
        sum('receivable').withColumnRenamed('sum(receivable)','money').filter('money>1000').\
        dropDuplicates(subset=['storeID']).\
        groupby('storeProvince').count()
    province_hot_store_count_df.write.mode('overwrite').\
        format('jdbc').\
        option('url','jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&characterEncoding=utf8').\
        option('user','root').\
        option('dbtable','province_hot_store_count').\
        option('password','123456').\
        option('encoding','utf-8').save()
    province_hot_store_count_df.write.mode('overwrite').saveAsTable('default.province_hot_store_count', 'parquet')'''
    #需求三
    '''top3_province_order_avg_df=top3_province_df_joined.groupby('storeprovince').\
        avg('receivable').withColumnRenamed('avg(receivable)','money').\
        withColumn('money',F.round('money',2)).\
        orderBy('money',ascending=False)
    top3_province_order_avg_df.write.mode('overwrite').\
        format('jdbc').\
        option('url','jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&characterEncoding=utf8').\
        option('user','root').\
        option('dbtable','top3_province_order_avg').\
        option('password','123456').\
        option('encoding','utf-8').save()
    top3_province_order_avg_df.write.mode('overwrite').saveAsTable('default.top3_province_order_avg', 'parquet')'''
    #需求四
    #湖南省 支付宝 33%
    top3_province_df_joined.createOrReplaceTempView('province_pay')
    pay_type_df=spark.sql("select storeProvince,payType,concat(round((count(payType)/total)*100,2),'%') as percent from (select storeProvince,payType,count(1) OVER(PARTITION BY storeProvince) as total from province_pay) as sub group by storeProvince,payType,total")
    pay_type_df.write.mode('overwrite').\
        format('jdbc').\
        option('url','jdbc:mysql://node1:3306/bigdata?useSSL=false&useUnicode=true&characterEncoding=utf8').\
        option('user','root').\
        option('dbtable','pay_type').\
        option('password','123456').\
        option('encoding','utf-8').save()
    pay_type_df.write.mode('overwrite').saveAsTable('default.pay_type','parquet')
    top3_province_df_joined.unpersist()