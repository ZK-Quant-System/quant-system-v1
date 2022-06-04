import backtrader as bt
import backtrader.indicators as btind # 导入策略分析模块
import pandas as pd
from feed_data import data_feeds_apply, TestDataFeed
import datetime

class SMAStrategy(bt.Strategy):
    def __init__(self):
        # 最简方式：直接省略指向的数据集
        self.sma1 = btind.SimpleMovingAverage(period=5)
        # 只指定第一个数据表格
        self.sma2 = btind.SMA(self.data, period=5)
        # 指定第一个数据表格的close 线
        self.sma3 = btind.SMA(self.data.close, period=5)
        # 完整写法
        self.sma4 = btind.SMA(self.datas[0].lines[0], period=5)
        # 指标函数也支持简写 SimpleMovingAverage → SMA

    def next(self):
        # 提取当前时间点
        print('datetime', self.datas[0].datetime.date(0))
        # 打印当日、昨日、前日的均线
        print('sma1', self.sma1.get(ago=0, size=3))
        print('sma2', self.sma2.get(ago=0, size=3))
        print('sma3', self.sma3.get(ago=0, size=3))
        print('sma4', self.sma4.get(ago=0, size=3))


def main():
    df_market_data = pd.read_pickle('../../data/market_data/market_data.pkl')[:5000]
    df_market_data['openinterest'] = 0
    df_market_data['pe'] = 2
    df_market_data['pb'] = 3

    cerebro = bt.Cerebro()

    start_time = datetime.datetime(2020, 5, 26)
    end_time = datetime.datetime(2022, 5, 28)
    df_market_data.groupby('code').apply(data_feeds_apply, start_time=start_time, end_time=end_time, cerebro=cerebro)
    cerebro.addstrategy(SMAStrategy)
    result = cerebro.run()


if __name__ == "__main__":
    main()