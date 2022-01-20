# -*- coding: utf-8 -*-
from jqdatasdk import auth, bond, query, get_price
import time
import numpy as np
import click
import sys
import os.path

work_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(work_path)
from config import conbond_config
import json
import pandas as pd
import glog
import os
import pickle
from feature_engineering import feature_cleaner


class ConBondDataProvider:
    def __init__(self, data_provider_config):
        self.time_span = conbond_config.time_span
        self.start_date = time.strftime('%Y-%m-%d', time.localtime(time.time() - self.time_span))
        self.end_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        self.jq_account = data_provider_config['jq_account']
        self.jq_password = data_provider_config['jq_password']
        self.conbond_market_data_file = conbond_config.conbond_market_data_file
        self.trading_dates_file = conbond_config.trading_dates_file
        self.legal_type_list = conbond_config.legal_type_list
        self.conbond_code_list = []
        self.fields = conbond_config.fields
        self.list_status = conbond_config.list_status
        auth(self.jq_account, self.jq_password)

    # 得到数据
    def get_data(self):

        if os.path.exists(self.conbond_market_data_file):
            # 如果已有数据，则删除最开始一天，跟新最新一天
            glog.info('Update the latest day.')
            # 读取数据
            dfs_double_index = pickle.load(open(self.conbond_market_data_file, 'rb+'))
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
            new_day_data = bond.run_query(query(bond.CONBOND_DAILY_PRICE).filter(bond.CONBOND_DAILY_PRICE.date==new_day))[self.fields]
            new_day_data = new_day_data.set_index(['date', 'code'])  # 设置双索引
            # 清洗数据
            new_day_data = feature_cleaner.outlier_replace(new_day_data)
            # 将新数据数据跟新到原有的整个数据中
            dfs_double_index = pd.concat([dfs_double_index, new_day_data])
        else:
            # 若无已有数据，则直接获取最新数据
            glog.info('Get all market data.')
            dfs_double_index = pd.DataFrame(columns=self.fields)
            # 利用上证指数获得两年的交易日
            temp_data = get_price(security='000001.XSHG', start_date=self.start_date, end_date=self.end_date, frequency='daily', fields=['close'])
            dates = temp_data.reset_index()['index']
            for date in dates:
                date = str(date)[0:10]
                df_temp = bond.run_query(query(bond.CONBOND_DAILY_PRICE).filter(bond.CONBOND_DAILY_PRICE.date==date))[self.fields]
                dfs_double_index = pd.concat([dfs_double_index, df_temp])
            dfs_double_index = dfs_double_index.sort_values(by=['date', 'code'], ascending=[True, True])
            dfs_double_index = dfs_double_index.set_index(['date', 'code'])
            # 清洗数据
            dfs_double_index = feature_cleaner.outlier_replace(dfs_double_index)
        glog.info('Data obtained.')
        # 存为pkl格式
        dfs_double_index.to_pickle(self.conbond_market_data_file)
        glog.info('Data stored.')

    def data_to_mongo(self):
        pass


@click.command()
@click.option("--config_file", help="the config file for ConBondDataProvider", default=work_path+"/config/config.json")
def main(config_file):
    with open(config_file) as f:
        data_provider_config = json.load(f)
    glog.info('loaded config.json.')
    data_provider = ConBondDataProvider(data_provider_config)
    glog.info('start get_data.')
    data_provider.get_data()
    data_provider.data_to_mongo()
    glog.info('End.')


if __name__ == "__main__":
    glog.info('Start program execution.')
    main()
