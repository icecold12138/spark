#coding=utf8
from jieba import cut_for_search
def context_jieba(data):
    seg= cut_for_search(data)
    l=list()
    for word in seg:
        l.append(word)
    return l
def filter_words(data):
    return data not in ['谷','帮','课']
def append_words(data):
    if data == '传智播':data='传智播客'
    if data=='院校':data='院校帮'
    if data =='博学':data='博学谷'
    return (data,1)
def extract_user_and_word(data):
    #传入数据是元组
    user_id=data[0]
    user_content=data[1]
    words=context_jieba(user_content)
    l=list()
    for word in words:
         #不要忘记过滤掉谷，帮，课
        if filter_words(word):
            l.append((user_id+'_'+append_words(word)[0],1))
    return l
