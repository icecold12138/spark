#coding=utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType
if __name__=='__main__':
    spark=SparkSession.builder.appName('test').master('local[*]').getOrCreate()
    #构建structType，text数据源，读取数据的特点是将一整行作为一个列读取，默认列明是value，类型是string
    schema=StructType.add('data',StringType(),nullable=True)
    df=spark.read.format('text').schema(schema=schema).load('../data/sql/people.txt')
    df.printSchema()
    df.show()

