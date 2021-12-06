import pandas as pd
import numpy as np
from sklearn.feature_selection import VarianceThreshold, SelectKBest, SelectPercentile


class FeatureSelector:
    def __init__(self):
        pass

    def __get_sorted_values(self, df, selector, scores='scores_'):
        values = eval(f"selector.{scores}")
        keys = df.columns
        dictionary = {key: value for key, value in zip(keys, values)}
        sorted_result = dict(sorted(dictionary.items(), key=lambda dictionary: dictionary[1], reverse=True))
        return sorted_result

    def variance_selector(self, df_feature: pd.DataFrame, threshold: float = None,
                          k_highest: int = None, percentile: int = None):
        """
        方差筛选器：当某特征的方差小于阈值时，删除该特征。
        :param df_feature: 包括所有特征因子的dataframe
        :param threshold:  取方差大小为为前k_highest的特征
        :param k_highest:  取方差大小为为前k_highest的特征
        :param percentile: 取方差大小为为前percentile%的特征 （参数取值通常为10，5等 代表前10% 5%）

        :return: df_filtered: 经过筛选后的df_feature / sorted_result: 在该筛选方法下，各个特征对应取值的大小
        """

        if (threshold is None) and (k_highest is None) and (percentile is None):
            raise Exception(f"至少输入threshold & k_highest & percentile参数中的一个")

        def udf_var(df):
            variance_value = df.var(axis=0)
            return variance_value

        if threshold:
            selector = VarianceThreshold(threshold=threshold)
            df_filtered = pd.DataFrame(selector.fit_transform(df_feature))

            remained_columns = df_feature.columns[selector.get_support(indices=True)]
            df_filtered.index = df_feature.index
            df_filtered.columns = remained_columns

            sorted_result = self.__get_sorted_values(df_filtered, selector, scores='variances_')

            return df_filtered, sorted_result

        if k_highest:
            selector = SelectKBest(score_func=udf_var, k=k_highest)
        if percentile:
            selector = SelectPercentile(score_func=udf_var, percentile=percentile)

        df_filtered = pd.DataFrame(selector.fit_transform(df_feature))

        remained_columns = df_feature.columns[selector.get_support(indices=True)]
        df_filtered.index = df_feature.index
        df_filtered.columns = remained_columns

        sorted_result = self.__get_sorted_values(df_feature, selector)

        return df_filtered, sorted_result

    def corr_selector(self, df_feature: pd.DataFrame, threshold: float = None):
        """
        相关性筛选器：当任意两特征之间相关性超过阈值时，删除靠前的特征。
        :param df_feature: 包括所有特征因子的dataframe
        :param threshold:  取方差大小为为前k_highest的特征

        :return: df_filtered: 经过筛选后的df_feature
        """
        if threshold is None:
            raise Exception(f"至少输入threshold & k_highest & percentile参数中的一个")

        df_feature = df_feature.astype('float')
        df_corr = df_feature.corr()
        df_corr_stack = df_corr.stack()
        triu_select = np.triu(np.ones(df_corr.shape)).astype('bool').reshape(df_corr.size)
        remove_self_select = (df_corr_stack != 1)
        corr_select = np.logical_and(triu_select, remove_self_select)
        df_corr_stack = df_corr_stack[corr_select]

        threshold_select = df_corr_stack > threshold
        df_corr_drop = df_corr_stack[threshold_select]

        drop_list = list(df_corr_drop.index.get_level_values(1))
        df_filtered = df_feature.drop(columns=drop_list)

        return df_filtered
