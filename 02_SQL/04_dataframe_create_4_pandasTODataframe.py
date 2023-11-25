#coding=utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
import pandas as pd
if __name__=='__main__':
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    sc=spark.sparkContext
    rdd=sc.textFile('../data/sql/people.txt').map(lambda x:x.split(',')).map(lambda x:(x[0],int(x[1])))
    #基于pandas的dataframe构建sparkSQL的打他frame
    pdf=pd.DataFrame({'id':[1,2,3],
                      'name':['张大仙','王笑笑','吕不韦'],
                      'age':[11,21,11]})
    df=spark.createDataFrame(pdf)
    df.printSchema()
    df.show()