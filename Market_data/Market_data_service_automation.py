# -*- coding: utf-8 -*-
import pandas as pd
from jqdatasdk import *
import time
import numpy as np
import matplotlib.pyplot as plt


##TODO 面向对象

def cldata(df): ##TODO 函数名全称
    # 输人一个dataframe
    col = list(df.columns.values)#得到其列标签
    index = list(df.index.values)
    # 处理inf
    df.replace(np.inf,np.nan,inplace=True)  ##TODO -np.inf
    # 处理不是nan的缺失值 
    # 处理非法字符，即非浮点数的字符串类。
    ##TODO 这种遍历方式非常的不 pythonic 尽量多以向量化 apply等方式进行
    for i in index:
        for j in col:
            if isinstance(df.loc[i,j],float):##TODO 逻辑有点问题，如果某个因子是类似于0，1这样的信号，也会直接pass
                continue
            else:
                df.loc[i,j]=np.nan
    return df
    
    
 ##TODO 加上主函数入口 编写主函数 以免多进程&线程时出现问题
# 账号是申请时所填写的手机号；密码为聚宽官网登录密码，新申请用户默认为手机号后6位
auth('13123205042','205042')  ##TODO config

# 获取所有中证指数股票, 设为股票池
stocks1 = get_index_stocks('000300.XSHG')#沪深300
stocks2 = get_index_stocks('000905.XSHG')#中证500
stocks3 = get_index_stocks('000852.XSHG')#中证1000指数
stocks = stocks1 + stocks2 + stocks3
# 获取当日时间及两年跨度
end_date = time.strftime('%Y-%m-%d',time.localtime(time.time()))
span = 2*365*24*60*60
start_date = time.strftime('%Y-%m-%d',time.localtime(time.time()-span))
# dfs的关键词是股票代码，其对应值为两年的开盘、收盘等的dataframe
dfs = {stocks[i]:get_price(security=stocks[i], start_date=start_date, end_date=end_date, frequency='daily') 
       for i in range(len(stocks))}
date = list(dfs[stocks[0]].index.values)
col = list(dfs[stocks[0]].columns.values)

##TODO python尽量减少for循环 用map
# 清洗数据
for i in range(len(stocks)):
    df = dfs[stocks[0]]
    temp = cldata(df)
    dfs[stocks[0]] = temp





