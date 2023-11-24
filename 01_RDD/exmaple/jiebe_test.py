#coding=utf8
import jieba
if __name__=="__main__":
    content="小明硕士毕业于中国科学院计算所，后在清华大学深造"
    result=jieba.cut(content,True)
    print(list(result))
    #类似list
    print(type(result))
    print(list(jieba.cut(content,False)))
    #搜索引擎模式，等同于二次组合场景
    print(','.join(jieba.cut_for_search(content)))