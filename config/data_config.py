from typing import List

stock_index_list: List[str] = ['000300.XSHG', '000905.XSHG', '000852.XSHG']
#stock_index_list: List[str] = ['000905.XSHG']
legal_type_list = [float, int]
time_span = 2 * 365 * 24 * 60 * 60
frequency = 'daily'
fields = ['open', 'close', 'high', 'low', 'volume', 'money',  'avg', 'factor']
fq = 'pre' # 'pre': 前复权;'post': 后复权;None: 不复权
data_path='/ZK-quant-system/data/'
market_data_file = data_path + "market_data_with_double_index.pkl"
dates_path='/ZK-quant-system/gitrepos/quant-system-v1/data_deploy/'
trading_dates_file = dates_path+'TradingDates.csv'