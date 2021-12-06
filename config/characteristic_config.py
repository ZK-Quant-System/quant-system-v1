import pandas as pd
from typing import List

filling_methods: List[str] = ['ffill', 'rolling_mean', 'KNN']
methon = ['roling_mean']
rolling_span = 3 # 大等于2
KNN_k = 3
max_value = 100000
min_value = 0
null_scale = 0.8
trading_dates_file = '../data_deploy/TradingDates.csv'
factor_file = '../data_deploy/TradingDates.csv'
