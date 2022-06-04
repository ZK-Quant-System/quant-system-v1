import backtrader as bt
import pandas as pd
import datetime


class StockDataFeed(bt.feeds.PandasData):
    lines = ('vwap',)
    params = {'vwap': -1}


class DataLoader(object):
    def __init__(self, config, cerebro):
        self.market_data_path = config["market_data_path"]
        self.__start_time = config["start_time"]
        self.__end_time = config["end_time"]
        self.__market_data = None
        self.__cerebro = cerebro

    @property
    def market_data(self):
        if self.__market_data is None:
            self.__market_data = pd.read_pickle(self.market_data_path)
        return self.__market_data

    @property
    def start_time(self):
        return datetime.datetime.strptime(self.__start_time, '%Y-%m-%d')

    @property
    def end_time(self):
        return datetime.datetime.strptime(self.__end_time, '%Y-%m-%d')

    def data_feeds_apply_func(self, df_stock):
        df_stock = df_stock.reset_index(level='code')

        stock_id = df_stock.code.values[0]
        datafeed = StockDataFeed(dataname=df_stock,
                                 fromdate=self.start_time,
                                 todate=self.end_time)
        self.__cerebro.adddata(datafeed, name=stock_id)

    def load_data_to_cerebro(self):
        self.market_data.groupby('code').apply(self.data_feeds_apply_func)
        return self.__cerebro


def main():
    cerebro = bt.Cerebro()
    config = {
        "market_data_path": "../../data/market_data/market_data.pkl",
        "start_time": "2021-05-21",
        "end_time": "2022-05-21",
    }

    dl = DataLoader(config, cerebro)


if __name__ == '__main__':
    main()
