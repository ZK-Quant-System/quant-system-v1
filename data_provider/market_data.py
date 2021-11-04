# -*- coding: utf-8 -*-
from jqdatasdk import auth, get_index_stocks, get_price
import time
import numpy as np
import click
from config import data_config
import json


##TODO 面向对象
class DataProvider:
    def __init__(self, data_provider_config):
        self.start_date = time.strftime('%Y-%m-%d', time.localtime(time.time() - self.time_span))
        self.end_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        self.jq_account = data_provider_config['jq_account']
        self.jq_password = data_provider_config['jq_password']
        self.legal_type_list = data_config.legal_type_list
        self.stock_index_list = data_config.stock_index_list
        self.target_stocks_list = []
        self.time_span = data_config.time_span
        auth(self.jq_account, self.jq_password)

    def _clean_data(self, df):
        # 输人一个dataframe
        col = list(df.columns.values)  # 得到其列标签
        index = list(df.index.values)
        # 处理inf
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        # 处理不是nan的缺失值
        # 处理非法字符，即非浮点数的字符串类。
        ##TODO 这种遍历方式非常的不 pythonic 尽量多以向量化 apply等方式进行
        for i in index:
            for j in col:
                if type(df.at[i, j]) in self.legal_type_list:  ##TODO not sure逻辑有点问题，如果某个因子是类似于0，1这样的信号，也会直接pass
                    continue
                else:
                    df.at[i, j] = np.nan
        return df

    def get_data(self):

        for stock_index_name in self.stock_index_list:
            stock_index_data = get_index_stocks(stock_index_name)
            self.target_stocks_list += stock_index_data

        # # 获取所有中证指数股票, 设为股票池
        # stocks1 = get_index_stocks('000300.XSHG')  # 沪深300
        # stocks2 = get_index_stocks('000905.XSHG')  # 中证500
        # stocks3 = get_index_stocks('000852.XSHG')  # 中证1000指数
        # stocks = stocks1 + stocks2 + stocks3
        # 获取当日时间及两年跨度
        # dfs的关键词是股票代码，其对应值为两年的开盘、收盘等的dataframe
        dfs = {self.target_stocks_list[i]: get_price(security=self.target_stocks_list[i], start_date=self.start_date,
                                                     end_date=self.end_date, frequency='daily')
               for i in range(len(self.target_stocks_list))}
        date = list(dfs[self.target_stocks_list[0]].index.values)
        col = list(dfs[self.target_stocks_list[0]].columns.values)

        ## 以时间为主键,双索引
        ##TODO python尽量减少for循环 用map
        # 清洗数据
        for i in range(len(self.target_stocks_list)):
            df = dfs[self.target_stocks_list[0]]
            temp = self._clean_data(df)
            dfs[self.target_stocks_list[0]] = temp

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
