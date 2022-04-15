# -*- coding: utf-8 -*-
from jqdatasdk import auth, bond, query, get_price
import time
import click
import sys
from config import data_config, base_config
import json
import pandas as pd
import numpy as np
import glog
import os
import pickle
from collections import OrderedDict
from feature_engineering import data_cleaner
from utils import datetime_tools
import os.path

work_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(work_path)


class ConBondDataProvider:
    def __init__(self, data_provider_config):
        self.config = data_config.cb_config
        self.time_span = self.config["time_span"]
        self.start_date = time.strftime('%Y-%m-%d', time.localtime(time.time() - self.time_span))
        self.end_date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        self.jq_account = data_provider_config['jq_account']
        self.jq_password = data_provider_config['jq_password']
        self.conbond_market_data_file = self.config['cb_market_data_file']
        self.trading_dates_file = base_config.trading_date_file
        self.legal_type_list = self.config["legal_type_list"]
        self.conbond_code_list = []
        self.fields_market = self.config["fields_market"]
        self.fields_basic = self.config["fields_basic"]
        self.fields_price_adjust = self.config["fields_price_adjust"]
        self.list_status = self.config["list_status"]
        self.clear_list = self.config["clear_list"]
        auth(self.jq_account, self.jq_password)

    # 得到数据
    def get_data(self):
        # 读取交易日历
        trading_dates = pd.read_csv(self.trading_dates_file)
        if os.path.exists(self.conbond_market_data_file):
            # 如果已有数据，则删除最开始一天，跟新最新一天
            glog.info('Update the latest day of conbond.')
            # 读取数据
            dfs_double_index = pickle.load(open(self.conbond_market_data_file, 'rb+'))
            # 得到日期列
            old_date = dfs_double_index.reset_index()['date']
            # 删除删除最开始一天数据
            dfs_double_index = dfs_double_index.drop(str(old_date.values[0])[0:10])
            # 得到原有数据的最后一天的日期
            yesterday = str(old_date.values[-1])[0:10]
            # 定位对应日期，得到最新日期及其对应的股票数据
            yesterday_location = trading_dates[(trading_dates.trading_date == yesterday)].index.tolist()[0]
            new_day = trading_dates.loc[yesterday_location + 1, 'trading_date']
            new_day_data = bond.run_query(query(bond.CONBOND_DAILY_PRICE).filter(
                bond.CONBOND_DAILY_PRICE.date == new_day))[self.fields_market]
            new_day_data = new_day_data.set_index(['date', 'code'])  # 设置双索引
            # 清洗数据
            new_day_data = data_cleaner.outlier_replace(new_day_data)
            # 将新数据跟新到原有的整个数据中
            dfs_double_index = pd.concat([dfs_double_index, new_day_data])
        else:
            # 若无已有数据，则直接获取最新数据
            glog.info('Get all conbond data.')
            df_m = pd.DataFrame(columns=self.fields_market)
            # 获得所需的交易日序列
            now_day = datetime_tools.get_current_date()
            # 判断今天是否是交易日，不是则得到前一个交易日
            if any(trading_dates.trading_date == now_day) == False:
                now_day = datetime_tools.get_pre_trading_date(now_day)
            now_day_location = trading_dates[(trading_dates.trading_date == now_day)].index.tolist()[0]
            dates = trading_dates.loc[now_day_location - self.time_span + 1:now_day_location, 'trading_date']
            for date in dates:
                date = str(date)[0:10]
                df_temp = bond.run_query(query(bond.CONBOND_DAILY_PRICE).filter(bond.CONBOND_DAILY_PRICE.date == date))[
                    self.fields_market]
                df_m = pd.concat([df_m, df_temp])
            df_m = df_m.set_index(['code'])
            col = list(OrderedDict.fromkeys(self.fields_market + self.fields_basic))
            df_m_b = pd.DataFrame(columns=col)
            for code, df in df_m.groupby('code'):
                # 得到一只可转债的基本资料
                df_basic = bond.run_query(query(bond.CONBOND_BASIC_INFO).filter(bond.CONBOND_BASIC_INFO.code == code))[
                    self.fields_basic]
                df_basic_n = df_basic.reindex(np.repeat(df_basic.index.values, len(df)), method='ffill')
                df_basic_n = df_basic_n.set_index(['code'])
                df_temp = pd.concat([df, df_basic_n], axis=1)
                # 得到一只可转债的转股价格调整数据
                df_price_adjust = bond.run_query(query(bond.CONBOND_CONVERT_PRICE_ADJUST).filter(
                    bond.CONBOND_CONVERT_PRICE_ADJUST.code == code))[self.fields_price_adjust]
                df_price_adjust['adjust_date'] = pd.to_datetime(df_price_adjust['adjust_date'])
                # 进行调整后的可转债转股价调整
                if min(df['date']) < min(df_price_adjust['adjust_date']):
                    bins = [min(df['date'])] + list(df_price_adjust['adjust_date']) + [max(df['date'])]
                    convert_price = list(df_basic['convert_price']) + list(df_price_adjust['new_convert_price'])
                else:
                    bins = list(df_price_adjust['adjust_date']) + [max(df['date'])]
                    convert_price = list(df_price_adjust['new_convert_price'])
                df_temp['convert_price'] = pd.cut(df_temp['date'], bins=bins, labels=convert_price)
                df_temp.reset_index()
                df_m_b = pd.concat([df_m_b, df_temp])
            # 筛出正常上市的，并删除list_status和list_date列
            df_m_b = df_m_b[df_m_b['list_status'] == self.list_status].drop(columns=['list_date', 'list_status'])
            # 清洗数据
            df_m_b[self.clear_list] = data_cleaner.outlier_replace(df_m_b[self.clear_list])
            df_m_b = df_m_b.sort_values(by=['date', 'code'], ascending=[True, True])
            df_m_b = df_m_b.set_index(['date', 'code'])
        glog.info('Data obtained.')
        # 存为pkl格式
        df_m_b.to_pickle(self.conbond_market_data_file)
        glog.info('Data stored.')


    def data_to_mongo(self):
        pass


@click.command()
@click.option("--config_file", help="the config file for ConBondDataProvider",
              default=work_path + "/config/jq_account_config.json")
def main(config_file):
    with open(config_file) as f:
        data_provider_config = json.load(f)
    glog.info('loaded jq_account_config.json.')
    data_provider = ConBondDataProvider(data_provider_config)
    glog.info('start get_data.')
    data_provider.get_data()
    data_provider.data_to_mongo()
    glog.info('End.')


if __name__ == "__main__":
    glog.info('Start to obtain convertible bond market data.')
    main()
