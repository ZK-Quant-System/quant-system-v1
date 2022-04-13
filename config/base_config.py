import pandas as pd
import os
import sys

sys.path.append('../..')
from utils import path_wrapper

# path
base_path = os.path.dirname(os.path.dirname(__file__))
base_data_path = path_wrapper.wrap_path(f"{base_path}/data")

# for now using csv, later upload to mongo
trading_date_file = f"{base_path}/data_deploy/TradingDates.csv"
trading_dates_df = pd.read_csv(trading_date_file, index_col=0)
