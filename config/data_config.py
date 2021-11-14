from typing import List

stock_index_list: List[str] = ['000300.XSHG', '000905.XSHG', '000852.XSHG']
legal_type_list = [float, int]
time_span = 5 * 365 * 24 * 60 * 60
frequency = 'daily'
market_data_file = "/data/market_data/market_data_daily.pkl"
trading_dates_file = '../data_deploy/TradingDates.csv'