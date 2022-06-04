import backtrader as bt
import pandas as pd
import datetime


class PrinterStrategy(bt.Strategy):
    def __init__(self):
        self.count = 0  # 用于计算 next 的循环次数
        # 打印数据集和数据集对应的名称
        print("------------- init 中的索引位置-------------")
        print(f"{self.datas[0].lines.getlinealiases()}")
        print("0 索引：", 'datetime', self.datas[0].lines.datetime.date(0), 'close', self.datas[0].lines.close[0])
        print("-1 索引：", 'datetime', self.datas[0].lines.datetime.date(-1), 'close', self.datas[0].lines.close[-1])
        print("1 索引：", 'datetime', self.datas[0].lines.datetime.date(1), 'close', self.datas[0].lines.close[1])
        print("从 0 开始往前取3天的收盘价：", self.datas[0].lines.pb.get(ago=0, size=3))
        print("line的总长度：", self.datas[0].buflen())

    def next(self):
        print(f"------------- next 的第{self.count + 1}次循环 --------------")
        print("当前时点（今日）1：", 'datetime', self.datas[0].lines.datetime.date(0), 'close', self.datas[0].lines.close[0])
        print("当前时点（今日）2：", 'datetime', self.datas[1].lines.datetime.date(0), 'pb', self.datas[1].lines.pb[0])
        print("往前推1天（昨日）：", 'datetime', self.datas[0].lines.datetime.date(-1), 'close', self.datas[0].lines.close[-1])
        print("往前推2天（前日）", 'datetime', self.datas[0].lines.datetime.date(-2), 'close', self.datas[0].lines.close[-2])
        print("前日、昨日、今日的收盘价：", self.datas[0].lines.close.get(ago=0, size=3))
        print("往后推1天（明日）：", 'datetime', self.datas[0].lines.datetime.date(1), 'close', self.datas[0].lines.close[1])
        print("往后推2天（明后日）", 'datetime', self.datas[0].lines.datetime.date(2), 'close', self.datas[0].lines.close[2])
        print("已处理的数据点：", len(self.datas[0]))
        print("line的总长度：", self.datas[0].buflen())
        self.count += 1


class TestDataFeed(bt.feeds.PandasData):
    def __init__(self):
        super(TestDataFeed, self).__init__()

    lines = ('pe', 'pb',)  # 要添加的线
    # 设置 line 在数据源上的列位置
    params = {
        'pe': -1,
        'pb': -1
    }


def data_feeds_apply(df_stock, start_time, end_time, cerebro):
    df_stock = df_stock.reset_index(level='code')

    stock_id = df_stock.code.values[0]
    datafeed = TestDataFeed(dataname=df_stock,
                            fromdate=start_time,
                            todate=end_time)
    cerebro.adddata(datafeed, name=stock_id)


def main():
    df_market_data = pd.read_pickle('../data/market_data/market_data.pkl')[:5000]
    df_market_data['openinterest'] = 0
    df_market_data['pe'] = 2
    df_market_data['pb'] = 3

    cerebro = bt.Cerebro()

    start_time = datetime.datetime(2020, 5, 26)
    end_time = datetime.datetime(2022, 5, 28)
    df_market_data.groupby('code').apply(data_feeds_apply, start_time=start_time, end_time=end_time, cerebro=cerebro)
    cerebro.addstrategy(PrinterStrategy)
    result = cerebro.run()


if __name__ == "__main__":
    main()
