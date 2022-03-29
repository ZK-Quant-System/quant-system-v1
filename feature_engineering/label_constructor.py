import pandas as pd


def apply_calc_return(df_stock: pd.DataFrame, label: str, time_lag: int, price_type: str):
    df_stock[label] = df_stock[price_type].pct_change(time_lag).fillna(method='ffill').fillna(method='bfill')
    return df_stock


def calc_return(df, label='return', time_lag=1, price_type='close'):
    df = df.groupby('code').apply(apply_calc_return, label=label, time_lag=time_lag, price_type=price_type)
    return df[label]
