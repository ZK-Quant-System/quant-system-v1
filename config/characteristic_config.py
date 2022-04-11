from typing import List

filling_methods: List[str] = ['ffill', 'rolling_mean', 'KNN']
instrument = 'instrument'
datetime = 'datetime'
method = 'rolling_mean'
rolling_span = 3 # 大等于2
KNN_k = 3
min_value = 0
null_scale = 0.8
trading_dates_file = '../data_deploy/TradingDates.csv'
feature_file = '../factor_alpha/factor_alpha_1.csv'
