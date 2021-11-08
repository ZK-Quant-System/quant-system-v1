# -*- coding: utf-8 -*-
from jqdatasdk import auth, get_index_stocks, get_price
import time
import numpy as np
import click
from data_provider.config import data_config
import json
import pandas as pd




##TODO 面向对象
class DataProvider:
    def __init__(self, data_provider_config):
        self.time_span = data_config.time_span
        self.start_date = time.strftime('%Y-%m-%d', time.localtime(time.time() - self.time_span))
        self.end_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        self.jq_account = data_provider_config['jq_account']
        self.jq_password = data_provider_config['jq_password']
        self.legal_type_list = data_config.legal_type_list
        self.stock_index_list = data_config.stock_index_list
        self.target_stocks_list = []
        auth(self.jq_account, self.jq_password)

    def _clean_data(self, df):
        # 处理inf
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        # 处理不是nan的缺失值
        # 处理非法字符，即非浮点数的字符串类。
        df_notlegal_type_list_locat = df.applymap(lambda x: False if type(x) in self.legal_type_list else True)
        df[df_notlegal_type_list_locat] = np.nan
        return df

    def get_data(self):
        for stock_index_name in self.stock_index_list:
            stock_index_data = get_index_stocks(stock_index_name)
            self.target_stocks_list.append(stock_index_data)

        # dfs的关键词是股票代码，其对应值为两年的开盘、收盘等的dataframe
        dfs = {self.target_stocks_list[i]: get_price(security=self.target_stocks_list[i], start_date=self.start_date,
                                                     end_date=self.end_date, frequency='daily')
               for i in range(len(self.target_stocks_list))}
        col = list(dfs[self.target_stocks_list[0]].columns.values)
        
        ##TODO python尽量减少for循环 用map
        # 清洗数据
        for stock in self.target_stocks_list:
            df = dfs[stock]
            temp = self._clean_data(df)
            dfs[stock] = temp
        
        
        # 设置双索引 
        coll = col.append('date')
        coll = coll.append('code')
        dfs_double_index = pd.DataFrame(columns = coll)
        for stock in self.target_stocks_list:
            df_temp = dfs[stock]
            df_temp['date'] = list(df_temp.index.values)
            df_temp['code'] = stock
            dfs_double_index=pd.concat([dfs_double_index, df_temp])         
        dfs_double_index = dfs_double_index.set_index(['date','code'])
        # 存为csv和pkl格式
        dfs_double_index.to_csv("market data with double index.csv", index=False)
        dfs_double_index.to_pickle("market data with double index.pkl")





    def data_to_mongo(self):
        pass


##TODO 加上主函数入口 编写主函数 以免多进程&线程时出现问题
@click.command()
@click.option("--config_file", help="the config file for DataProvider", default="./config/config.json")
def main(config_file):
    with open(config_file) as f:
        data_provider_config = json.load(f)
    data_provider = DataProvider(data_provider_config)
    data_provider.data_to_mongo()


if __name__ == "__main__":
    main()
