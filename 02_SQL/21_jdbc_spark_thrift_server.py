#coding=utf8
from pyhive import hive
if __name__=='__main__':
    #获取hive（spark thriftserver)的连接
    conn=hive.Connection(host='node1',port=10000,username='root')
    #获取游标对象，用来执行sql
    cursor=conn.cursor()
    #执行sql,使用executor APL
    cursor.executor('select * from test')
    #获取返回值
    result=cursor.fetchall()
    print(result)



