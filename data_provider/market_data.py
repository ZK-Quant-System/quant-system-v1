# -*- coding: utf-8 -*-
from jqdatasdk import auth, get_index_stocks, get_price
import time
import numpy as np
import click
import sys
import os.path

work_path=os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(work_path)
from config import data_config
import json
import pandas as pd
import glog
import os
import pickle


class DataProvider:
    def __init__(self, data_provider_config):
        self.time_span = data_config.time_span
        self.start_date = time.strftime('%Y-%m-%d', time.localtime(time.time() - self.time_span))
        self.end_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        self.jq_account = data_provider_config['jq_account']
        self.jq_password = data_provider_config['jq_password']
        self.market_data_file = data_config.market_data_file
        self.trading_dates_file = data_config.trading_dates_file
        self.legal_type_list = data_config.legal_type_list
        self.stock_index_list = data_config.stock_index_list
        self.frequency = data_config.frequency
        self.target_stocks_list = []
        auth(self.jq_account, self.jq_password)

    # TODO 可以把这个部分直接兼容到数据清洗模块
    # 处理非法字符，即非浮点整型数的字符串类及inf。
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
            self.target_stocks_list += stock_index_data
        glog.info('Stock pool acquired.')

        # 字典dfs的关键词是股票代码，其对应值为两年的开盘、收盘等的dataframe
        if os.path.exists(self.market_data_file):
            # 如果已有数据，则删除最开始一天，跟新最新一天
            glog.info('Update the latest day.')
            # 读取数据
            dfs_double_index = pickle.load(open(self.market_data_file, 'rb+'))
            # 得到日期列
            old_date = dfs_double_index.reset_index()['date']
            # 删除删除最开始一天数据
            dfs_double_index = dfs_double_index.drop(str(old_date.values[0])[0:10])
            # 得到原有数据的最后一天的日期
            yesterday = str(old_date.values[-1])[0:10]
            # 读取交易日历
            trading_dates = pd.read_csv(self.trading_dates_file)
            # 定位对应日期，得到最新日期及其对应的股票数据
            yesterday_location = trading_dates[(trading_dates.trading_date == yesterday)].index.tolist()[0]
            new_day = trading_dates.loc[yesterday_location + 1, 'trading_date']
            new_day_data = get_price(security=self.target_stocks_list, start_date=new_day, end_date=new_day,
                                     frequency=self.frequency)
            new_day_data = new_day_data.rename(columns={'time': 'date'})  # jqdatasdk得到日期的列名为time，为保持一致性，改为date
            new_day_data = new_day_data.set_index(['date', 'code'])  # 设置双索引
            # 清洗数据
            new_day_data = new_day_data.applymap(self._replace2nan)
            # 将新数据数据跟新到原有的整个数据中
            dfs_double_index = pd.concat([dfs_double_index, new_day_data])
        else:
            # 若无已有数据，则直接获取最新数据
            glog.info('Get all market data.')
            dfs = {
                self.target_stocks_list[i]: get_price(security=self.target_stocks_list[i], start_date=self.start_date,
                                                      end_date=self.end_date, frequency=self.frequency)
                for i in range(len(self.target_stocks_list))}
            # 清洗数据
            for (stock_index_name, df) in dfs.items():
                dfs[stock_index_name] = df.applymap(self._replace2nan)
            col = list(dfs[self.target_stocks_list[0]].columns.values)
            # 设置双索引
            coll = col
            coll.append('date')
            coll.append('code')
            dfs_double_index = pd.DataFrame(columns=coll)
            for stock in self.target_stocks_list:
                df_temp = dfs[stock]
                df_temp['date'] = list(df_temp.index.values)
                df_temp['code'] = stock
                dfs_double_index = pd.concat([dfs_double_index, df_temp])
            dfs_double_index = dfs_double_index.sort_values(by=['date', 'code'], ascending=[True, True])
            dfs_double_index = dfs_double_index.set_index(['date', 'code'])

        glog.info('Data obtained.')
        # 存为pkl格式
        dfs_double_index.to_pickle(self.market_data_file)
        glog.info('Data stored.')

    def data_to_mongo(self):
        pass


@click.command()
@click.option("--config_file", help="the config file for DataProvider", default=work_path+"/config/config.json")
def main(config_file):
    with open(config_file) as f:
        data_provider_config = json.load(f)
    glog.info('loaded config.json.')
    data_provider = DataProvider(data_provider_config)
    glog.info('start get_data.')
    data_provider.get_data()
    data_provider.data_to_mongo()
    glog.info('End.')


if __name__ == "__main__":
    glog.info('Start program execution.')
    main()
