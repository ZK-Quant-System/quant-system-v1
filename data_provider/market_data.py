# -*- coding: utf-8 -*-
from jqdatasdk import auth, get_index_stocks, get_price
import time
import numpy as np
import click
import sys
sys.path.append('../')
from config import data_config
from data_deploy import TradingDates
import json
import pandas as pd
import logging




class DataProvider:
    def __init__(self, data_provider_config):
        self.time_span = data_config.time_span
        self.start_date = time.strftime('%Y-%m-%d', time.localtime(time.time() - self.time_span))
        self.end_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        self.jq_account = data_provider_config['jq_account']
        self.jq_password = data_provider_config['jq_password']
        self.legal_type_list = data_config.legal_type_list
        self.stock_index_list = data_config.stock_index_list
        self.frequency = data_config.frequency
        self.target_stocks_list = []
        auth(self.jq_account, self.jq_password)


    # 处理非法字符，即非浮点整型数的字符串类，及inf。
    def _replace2nan(self, x):
        if type(x) in self.legal_type_list:
            if x in [np.inf, -np.inf]:
                return np.nan
            else:
                return x
        else:
            return np.nan


    # 得到数据
    def get_data(self):
        for stock_index_name in self.stock_index_list:
            stock_index_data = get_index_stocks(stock_index_name)
            self.target_stocks_list.append(stock_index_data)

        # 字典dfs的关键词是股票代码，其对应值为两年的开盘、收盘等的dataframe
        dfs = {self.target_stocks_list[i]: get_price(security=self.target_stocks_list[i], start_date=self.start_date,
                                                     end_date=self.end_date, frequency=self.frequency)
               for i in range(len(self.target_stocks_list))}
        col = list(dfs[self.target_stocks_list[0]].columns.values)
        

        # 清洗数据
        for (key, value) in dfs.items():
            dfs[key] = value.applymap(_replace2nan)

        
        
        # 设置双索引 
        coll = col
        coll.append('date')
        coll.append('code')
        dfs_double_index = pd.DataFrame(columns = coll)
        for stock in self.target_stocks_list:
            df_temp = dfs[stock]
            df_temp['date'] = list(df_temp.index.values)
            df_temp['code'] = stock
            dfs_double_index=pd.concat([dfs_double_index, df_temp])         
        dfs_double_index = dfs_double_index.sort_values.sort_values(by=['date','code'],ascending=[True,True])
        dfs_double_index = dfs_double_index.set_index(['date','code'])
        # 存为csv和pkl格式
        dfs_double_index.to_csv("./market_data_with_double_index/market_data_with_double_index.csv")
        dfs_double_index.to_pickle("./market_data_with_double_index/market_data_with_double_index.pkl")





    def data_to_mongo(self):
        pass



@click.command()
@click.option("--config_file", help="the config file for DataProvider", default="../config/config.json")
def main(config_file):
    with open(config_file) as f:
        data_provider_config = json.load(f)
    data_provider = DataProvider(data_provider_config)
    data_provider.get_data()
    data_provider.data_to_mongo()


if __name__ == "__main__":
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename='my.log', level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)
    main()
