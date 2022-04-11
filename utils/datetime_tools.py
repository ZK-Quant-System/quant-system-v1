import datetime
import sys

sys.path.append("../")
from config import base_config


def get_current_date(date_format="%Y-%m-%d"):
    now_time = datetime.datetime.now()
    current_date = now_time.strftime(date_format)
    return current_date


def get_next_trading_date(date):
    trading_date_list = global_config.trading_dates_df.trading_date.to_list()
    try:
        current_date_index = trading_date_list.index(date)
    except ValueError:
        raise Exception(f'Input date {date} is not a trading date & not in %Y-%m-%d format')
    next_date_index = current_date_index + 1
    next_trading_date = trading_date_list[next_date_index]
    return next_trading_date


def get_pre_trading_date(date):
    trading_date_list = global_config.trading_dates_df.trading_date.to_list()
    try:
        current_date_index = trading_date_list.index(date)
    except ValueError:
        raise Exception(f'Input date {date} is not a trading date & not in %Y-%m-%d format')
    pre_date_index = current_date_index - 1
    pre_trading_date = trading_date_list[pre_date_index]
    return pre_trading_date


if __name__ == "__main__":
    print(get_current_date())
    print(get_next_trading_date('2021-10-29'))
    print(get_pre_trading_date('2021-10-29'))
