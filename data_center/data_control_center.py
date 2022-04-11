import pandas as pd
import sys

sys.path.append("../")

from config import data_center_config


class DataCenter:
    """
    数据中心，负责将所有注册的基础数据和因子进行合并分组
    """

    def __init__(self) -> None:
        self.register_info = data_center_config.register_info
        self.stock_daily_data_output_path = data_center_config.stock_daily_data_output_path

    def merge_daily_data_stock(self):
        """
        将注册表中的所有日度股票行情/因子数据合并起来
        """
        stock_data_df = pd.DataFrame()
        stock_data_info = self.register_info['stock_data']

        for data_info in stock_data_info:
            data_path = stock_data_info[data_info]['data_path']
            origin_data = pd.read_pickle(data_path)
            stock_data_df = pd.merge(stock_data_df, origin_data, how='outer')

        stock_data_df.to_pickle(self.stock_daily_data_output_path)


if __name__ == '__main__':
    DC = DataCenter()
    DC.merge_daily_data_stock()
