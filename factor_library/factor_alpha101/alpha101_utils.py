from scipy.stats import rankdata
from dateutil import parser
import numpy as np
import numpy.linalg as la
import pandas as pd
from datetime import datetime
import scipy.stats as stats
import matplotlib.pyplot as plt


# 中性(行业中性)
def neu_industry(stock_data, sector_series):
    """
    a_transform = a / sector_average(a)
    """
    # align sector_series to stock_data in case of stock_data's sparse stocks
    inner = sector_series[stock_data.columns]
    stock_data = stock_data.replace([-np.inf, np.inf], 0).fillna(value=0)
    trans_matrix = pd.DataFrame()
    for stock in stock_data.columns:
        df = inner == inner[stock]
        df /= df.sum()
        trans_matrix = pd.concat([trans_matrix, df], axis=1, sort=True)

    result = pd.DataFrame(np.dot(stock_data.fillna(value=0), trans_matrix.fillna(value=0)),
                          index=stock_data.index, columns=stock_data.columns)
    return (stock_data / result)


def neutral(data, ind):
    stocks = list(data.index)
    X = np.array(pd.get_dummies(ind))
    y = data.values
    beta_ols = la.inv(X.T.dot(X)).dot(X.T).dot(y)
    residual = y - X.dot(beta_ols)
    return residual


def IndNeutralize(vwap, ind):
    vwap_ = vwap.fillna(value=0)
    for i in range(len(vwap_)):
        vwap_.iloc[i] = neutral(vwap_.iloc[i], ind)
    return vwap_


# 移动求和
def ts_sum(df, window):
    return df.rolling(window).sum()


# 移动平均
def sma(df, window):
    return df.rolling(window).mean()


# 移动标准差
def stddev(df, window):
    return df.rolling(window).std()


# 移动相关系数
def correlation(x, y, window):
    return x.rolling(window).corr(y)


# 移动协方差
def covariance(x, y, window):
    return x.rolling(window).cov(y)


# 在过去d天的时序排名
def rolling_rank(na):
    return rankdata(na)[-1]


def ts_rank(df, window):
    return df.rolling(window).apply(rolling_rank)


# 过去d天的时序乘积
def rolling_prod(na):
    return np.prod(na)


def product(df, window):
    return df.rolling(window).apply(rolling_prod)


# 过去d天最小值
def ts_min(df, window):
    return df.rolling(window).min()


# 过去d天最大值
def ts_max(df, window):
    return df.rolling(window).max()


# 当天取值减去d天前的值
def delta(df, period):
    return df.diff(period)


# d天前的值，滞后值
def delay(df, period):
    return df.shift(period)


# 截面数据排序，输出boolean值
def rank(df):
    return df.rank(pct=True, axis=1)


# 缩放时间序列，使其和为1
def scale(df, k=1):
    return df.mul(k).div(np.abs(df).sum())


# 过去d天最大值的位置
def ts_argmax(df, window):
    return df.rolling(window).apply(np.argmax) + 1


# 过去d天最小值的位置
def ts_argmin(df, window):
    return df.rolling(window).apply(np.argmin) + 1


# 线性衰减的移动平均加权
def decay_linear(df, period):
    if df.isnull().values.any():
        df.fillna(method='ffill', inplace=True)
        df.fillna(method='bfill', inplace=True)
        df.fillna(value=0, inplace=True)
    na_lwma = np.zeros_like(df)  # 生成与df大小相同的零数组
    na_lwma[:period, :] = df.iloc[:period, :]  # 赋前period项的值
    na_series = df.as_matrix()
    # 计算加权系数
    divisor = period * (period + 1) / 2
    y = (np.arange(period) + 1) * 1.0 / divisor
    # 从第period项开始计算数值
    for row in range(period - 1, df.shape[0]):
        x = na_series[row - period + 1: row + 1, :]
        na_lwma[row, :] = (np.dot(x.T, y))
    return pd.DataFrame(na_lwma, index=df.index, columns=df.columns)
