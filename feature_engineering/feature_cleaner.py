import numpy as np
import sys
sys.path.append('../')
from config import feature_config
import pandas as pd
import glog
from sklearn.impute import KNNImputer
import pickle

'''Nested Function'''
def outlier_handling(x, max_value: float=100000, min_value: float=0):
    """
    异常值处理函数：将无穷值、非法字符及不合理值进行处理，替代为np.nan，将字符型的数字转换为浮点型的数字数
    :param x: 元素值
    :param max_value: 最大值
    :param min_value: 最小值
    :return: x: 元素值
    """
    if type(x) in [str]:
        if str.isdigit(x):
            return float(x)
        else:
            return np.nan
    elif type(x) in [float, int]:
        if x in [np.inf, -np.inf]:
            return np.nan
        if x > max_value or x < min_value:
            return np.nan
        else:
            return x
    else:
        return np.nan

def timestamp_matching_handing(df: pd.DataFrame, trading_dates_file=feature_config.trading_dates_file):
    """
    时间戳匹配处理函数：若feature表格是否与交易时间标准匹配，若有遗漏则出发预警，并与交易时间标准进行合并，以空行填充；若完全匹配，则不进行处理
    :param df:一只股票的所有特征因子的dataframe，行索引为时间，列索引为特征名
    :param trading_dates_file: 交易时间标准文件
    :return: df：经过时间戳匹配后的特征因子的dataframe
    """
    # 得到列索引
    col = list(df.columns.values)
    # 得到日期列
    feature_date = list(df.index.values)
    # 得到原有数据的开始和最后一天的日期
    feature_date_begin = str(feature_date[0])[:10]
    feature_date_end = str(feature_date[-1])[:10]
    # 读取交易日历
    trading_dates = pd.read_csv(trading_dates_file)
    # 定位对应日期，得到标准的区间日期
    glog.info('Locate the corresponding date to get the standard interval date.')
    beginday_location = trading_dates[(trading_dates.trading_date == feature_date_begin)].index.tolist()[0]
    endday_location = trading_dates[(trading_dates.trading_date == feature_date_end)].index.tolist()[0]
    standard_span = trading_dates.loc[beginday_location:endday_location, 'trading_date']
    if len(feature_date) == len(standard_span):
        glog.info('The timestamp matches.')
        return df
    else:
        glog.info('The timestamp dose not match.')
        difference_list = list(set(standard_span).difference(set(feature_date)))
        for d in difference_list:
            df.loc[d] = np.nan
        df = df.sort_index(axis=0)
        glog.info('Timestamp matching complete.')
        return df

def missing_value_handing(factor_series: pd.Series, method='ffill', rolling_span: int=3, KNN_k: int=3):
    """
    缺失值处理函数：针对输入的特征列进行处理，
    :param factor_series: 特征列
    :param method: 填充方法：['ffill', 'rolling_mean', 'KNN']中的一个：字符型
    :param rolling_span: 滚动均值填充的均值区间
    :param KNN_k: KNN的k值
    :return:
    """
    valid_method = ('ffill', 'rolling_mean', 'KNN')
    if method not in valid_method:
        raise Exception("The filling methods are limited to: "+str(valid_method))

    if method == 'ffill':
        glog.info('ffill前填充')
        if factor_series[0] == np.nan:
            factor_series[0] = 0
        factor_series = factor_series.fillna(method='ffill')
    elif method == 'rolling_mean':
        glog.info('rolling_mean滚动均值')
        if factor_series[0] == np.nan:
            factor_series[0] = 0
        if factor_series[1] == np.nan:
            factor_series[1] = factor_series[0]
        for i in range(2, len(factor_series)):
            if factor_series[i] == np.nan:
                if i < rolling_span:
                    factor_series[i] = np.mean(factor_series[0:i])
                else:
                    factor_series[i] = np.mean(factor_series[i - rolling_span:i])
    elif method == 'KNN':
        glog.info('KNN填充')
        factor_index = factor_series.index.tolist()
        factor_array = np.array(factor_series)
        factor_array = factor_array.reshape(-1, 1)
        imputer = KNNImputer(n_neighbors=KNN_k)
        factor_array = imputer.fit_transform(factor_array)
        factor_list = factor_array.tolist()
        factor_series = pd.Series(data=factor_list, index=factor_index)
    return factor_series


'''Processing Function'''
def outlier_replace(df_feature: pd.DataFrame):
    """
    异常值替换函数：将无穷值、非法字符及不合理值进行处理，替代为np.nan，将字符型的数字转换为浮点型的数字数，嵌套函数outlier_handling
    :param df_feature:包括所有特征因子的dataframe
    :return: df_feature：经过异常值处理过后的特征因子的dataframe
    """
    df = df_feature.applymap(outlier_handling)
    glog.info('Outlier replacement complete.')
    return df

def timestamp_matching(df_feature: pd.DataFrame):
    datetime = list(df_feature.index.names)[0]
    instrument = list(df_feature.index.names)[1]
    df_feature = df_feature.reset_index()
    instruments = df_feature[instrument].drop_duplicates()
    df_feature = df_feature.sort_values(by=[instrument, datetime], ascending=[True, True])
    df_feature = df_feature.set_index([instrument, datetime])
    col = list(df_feature.columns.values) + [instrument]
    df_feature_clean = pd.DataFrame(columns=col)
    for code in instruments:
        #glog.info(code + ' start timestmap matches.')
        df = df_feature.xs(([code]))
        df = timestamp_matching_handing(df)
        df[instrument] = code
        df_feature_clean = pd.concat([df_feature_clean, df])
    df_feature_clean.reset_index(inplace=True)
    df_feature_clean = df_feature_clean.rename(columns={'index': datetime})
    df_feature_clean = df_feature_clean.sort_values(by=[datetime, instrument], ascending=[True, True])
    df_feature = df_feature_clean.set_index([datetime, instrument])
    glog.info('Timestamp matching complete.')
    return df_feature

def data_replace(df_feature: pd.DataFrame, null_scale: float=0.8):
    """
    数据填充函数：将feature表格的缺失值进行填充
    :param df_feature: :包括所有特征因子的dataframe
    :param method: 填充方法：['ffill', 'rolling_mean', 'KNN']：字符型
    :param null_scale: 缺失值最大比例,缺失值比例多于null_scale的因子需被删除：浮点型
    :return: df_feature：经过缺失值填充过后的特征因子的dataframe
    """
    # 输入特征序列，缺失值比例阈值，填充方法
    col = list(df_feature.columns.values)
    df = pd.DataFrame(columns=col, index=['nan_percent'])
    df.loc['nan_percent'] = df_feature.isnull().sum() / len(df_feature.index)
    glog.info('Eliminate columns with missing values exceeding the threshold.')
    df_feature.drop(df.columns[df.loc['nan_percent'] >= null_scale], axis=1, inplace=True)
    glog.info('Fill missing value.')
    df_feature.apply(missing_value_handing)
    glog.info('Data replacement complete.')
    return df_feature


