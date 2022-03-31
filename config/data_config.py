from typing import List

#stock_index_list: List[str] = ['000300.XSHG', '000905.XSHG', '000852.XSHG']
stock_index_list: List[str] = ['000905.XSHG']
legal_type_list = [float, int]
time_span = 1 * 365 * 24 * 60 * 60
frequency = 'daily'
fields = ['open', 'close', 'high', 'low', 'volume', 'money', 'factor']
fq = 'pre' # 'pre': 前复权;'post': 后复权;None: 不复权
weight_method = 'avg' #weight_method : 计算各分位收益时, 每只股票权重, 默认为 'avg'
# 'avg': 等权重;'mktcap': 按总市值加权;'ln_mktcap': 按总市值的对数加权;'cmktcap': 按流通市值加权;'ln_cmktcap': 按流通市值的对数加权

user = 'jws'

if user is 'server':
    data_path='/ZK-quant-system/data/'
    dates_path='/ZK-quant-system/gitrepos/quant-system-v1/data_deploy/'
    fig_path = '/ZK-quant-system/figures/'
elif user is 'jws':
    data_path = r'C:\Users\lenovo\Desktop\hnu\ZK-Quant-System\v1\tmprepo\data\\'[:-1]
    dates_path = r'C:\Users\lenovo\Desktop\hnu\ZK-Quant-System\v1\quant-system-v1\data_deploy\\'[:-1]
    fig_path = r'C:\Users\lenovo\Desktop\hnu\ZK-Quant-System\v1\tmprepo\figures\\'[:-1]

market_data_file = data_path + "market_data_with_double_index.pkl"
groupby_data_file = data_path + "groupby_data_with_double_index.pkl"
market_cap_data_file = data_path + "market_cap_data_with_double_index.pkl"
circulating_market_cap_data_file = data_path + "circulating_market_cap_data_with_double_index.pkl"
weight_data_file = data_path + "weight_data_with_double_index.pkl"
trading_dates_file = dates_path + 'TradingDates.csv'