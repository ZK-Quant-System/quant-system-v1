feature_constructor_config = {
    # symbolic transformer
    "function_set": ['add', 'sub', 'mul', 'div', 'abs']
}

data_cleaner_config = {
    "filling_methods": ['ffill', 'rolling_mean', 'KNN'],
    "instrument": 'instrument',
    "datetime": 'datetime',
    "method": 'rolling_mean',
    "rolling_span": 3,  # 大等于2
    "KNN_k": 3,
    "min_value": 0,
    "null_scale": 0.8,
}
