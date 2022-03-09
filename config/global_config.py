import pandas as pd

# for now using csv, later upload to mongo
trading_date_path = "../data_deploy/TradingDates.csv"
trading_dates_df = pd.read_csv(trading_date_path, index_col=0)