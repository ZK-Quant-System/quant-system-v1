import numpy as np
import click
import sys
sys.path.append('../')
from config import characteristic_config
import pandas as pd
import glog
from sklearn.impute import KNNImputer




def outlier_replace(df_factor):
    df = df_factor.applymap(outlier_handling)
    return df

def outlier_handling(x, max_value = characteristic_config.max_value, min_value = characteristic_config.min_value):
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


def missing_value_handing(factor_series, method, rolling_span = characteristic_config.rolling_span,
                          KNN_k = characteristic_config.KNN_k,filling_methods = characteristic_config.filling_methods):
    #输入特征序列，填充方法，
    method_num = filling_methods.index(method)
    if method_num == 0:
        if factor_series[0] == np.nan:
            factor_series[0] = 0
        factor_series = factor_series.fillna(method='ffill')
    elif method_num == 1:
        if factor_series[0] == np.nan:
            factor_series[0] = 0
        if factor_series[1] == np.nan:
            factor_series[1] = factor_series[0]
        for i in range(2 to len(factor_series)):
            if  factor_series[i] == np.nan:
                if i <rolling_span:
                    factor_series[i] = np.mean(factor_series[0:i])
                else:
                    factor_series[i] = np.mean(factor_series[i-rolling_span:i])
    elif method_num == 2:
        factor_index = factor_series.index.tolist()
        factor_array = np.array(factor_series)
        factor_array = factor_array.reshape(-1, 1)
        imputer = KNNImputer(n_neighbors=KNN_k)
        factor_array = imputer.fit_transform(factor_array)
        factor_list = factor_array.tolist()
        factor_series = pd.Series(data=factor_list, index=factor_index)
    return factor_series



def missing_value(factor_series, method, null_scale = characteristic_config.null_scale):
    # 输入特征序列，缺失值比例阈值，填充方法
    if factor_series.isnull().sum() / len(factor_series) < null_scale:
        return missing_value_handing(factor_series, method)
    """未完待续""""
    else:
         pass


def timestamp_matching(df_factor, trading_dates_file=characteristic_config.trading_dates_file):
    # 得到列索引
    col = list(df_factor.columns.values)
    # 得到日期列
    factor_date = list(df_factor.index.values)
    # 得到原有数据的开始和最后一天的日期
    factor_date_begin = str(factor_date.values[0])
    factor_date_end = str(factor_date.values[-1])
    # 读取交易日历
    trading_dates = pd.read_csv(trading_dates_file)
    # 定位对应日期，得到标准的区间日期
    beginday_location = trading_dates[(trading_dates.trading_date == factor_date_begin)].index.tolist()[0]
    endday_location = trading_dates[(trading_dates.trading_date == factor_date_end)].index.tolist()[0]
    standard_span = trading_dates.loc[beginday_location:endday_location+1,'trading_date']
    if len(factor_date) == len(standard_span):
        glog.info('The timestamp matches.')
        return df_factor
    else:
        glog.info('The timestamp dose not match.')
        standard_df_factor = pd.DataFrame(columns=col, index=standard_span)
        standard_df_factor = standard_df_factor.apply(lambda x: df_factor[x.index, x.name] if x.name in factor_date else np.nan)
        glog.info('Timestamp matching complete.')
        return standard_df_factor







@click.command()
def main():
    glog.info('Start characteristic cleaning.')
    # df的行索引是日期，列索引是特征名称
    factor_file = characteristic_config.factor_file
    glog.info('Load the factors data.')
    df_factor = pd.read_csv(factor_file)
    glog.info('Factors data is loaded.')
    method = characteristic_config.method
    glog.info('Start outlier handing.')
    df_factor = outlier_replace(df_factor)
    glog.info('Start to determine whether the timestamp matches.')
    df_factor = timestamp_matching(df_factor)
    glog.info('Start fill the np.nan.')
    df_factor.apply(missing_value(factor_series, method))
    glog.info('End.')



if __name__ == "__main__":
    glog.info('Start program execution.')
    main()

