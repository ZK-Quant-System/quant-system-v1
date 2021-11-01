# -*- coding: utf-8 -*-
import pandas as pd
from jqdatasdk import *
import time
import numpy as np

auth('13123205042','205042')
# 账号是申请时所填写的手机号；密码为聚宽官网登录密码，新申请用户默认为手机号后6位

# 获取所有中证指数股票, 设为股票池
stocks1 = get_index_stocks('000300.XSHG')#沪深300
stocks2 = get_index_stocks('000905.XSHG')#中证500
stocks3 = get_index_stocks('000852.XSHG')#中证1000指数
stocks = stocks1 + stocks2 + stocks3

end_date = time.strftime('%Y-%m-%d',time.localtime(time.time()))
span = 2*365*24*60*60
start_date = time.strftime('%Y-%m-%d',time.localtime(time.time()-span))
# dfs的关键词是股票代码，其对应值为两年的开盘、收盘等的dataframe
dfs = {stocks[i]:get_price(security=stocks[i], start_date=start_date, 
                           end_date=end_date, frequency='daily', panel=True) 
       for i in range(len(stocks))}




