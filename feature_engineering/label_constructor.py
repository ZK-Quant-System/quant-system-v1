def calc_return(df, price_type='close'):
    return df[price_type].pct_change().fillna(method='bfill')
