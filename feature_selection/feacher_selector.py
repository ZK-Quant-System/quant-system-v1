import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import glog
from functools import partial
from sklearn.feature_selection import VarianceThreshold, SelectKBest, \
    SelectPercentile, f_regression, mutual_info_regression


class FeatureSelector:
    def __init__(self):
        self.data = pd.read_pickle("./test_data.pkl")

    def __get_sorted_values(self, df, selector, scores):
        values = eval(f"selector.{scores}")
        keys = df.columns
        dictionary = {key: value for key, value in zip(keys, values)}
        sorted_result = dict(sorted(dictionary.items(), key=lambda dictionary: dictionary[1], reverse=True))
        return sorted_result

    def __get_selector_result(self, selector, df_feature, df_label, scores='scores_'):
        df_selected = pd.DataFrame(selector.fit_transform(df_feature, df_label))

        remained_columns = df_feature.columns[selector.get_support(indices=True)]
        df_selected.index = df_feature.index
        df_selected.columns = remained_columns
        sorted_result = self.__get_sorted_values(df_feature, selector, scores)

        return df_selected, sorted_result

    def variance_selector(self, df_feature: pd.DataFrame, df_label: pd.Series,
                          threshold: float = None, k_highest: int = None, percentile: int = None):
        """
        方差筛选器：当某特征的方差小于阈值时，删除该特征。
        :param df_feature: 包括所有特征因子的dataframe
        :param df_label:   标签列Series
        :param threshold:  取方差大小为为前k_highest的特征
        :param k_highest:  取方差大小为为前k_highest的特征
        :param percentile: 取方差大小为为前percentile%的特征 （参数取值通常为10，5等 代表前10% 5%）

        :return: df_selected: 经过筛选后的df_feature / sorted_result: 在该筛选方法下，各个特征对应取值的大小
        """
        glog.info(f"Feature Selecting variance_selector threshold {threshold} "
                  f"k_highest {k_highest} percentile {percentile}")

        if (threshold is None) and (k_highest is None) and (percentile is None):
            raise Exception(f"至少输入threshold & k_highest & percentile参数中的一个")

        if threshold:
            selector = VarianceThreshold(threshold=threshold)
            result = self.__get_selector_result(selector, df_feature, df_label, scores="variances_")
            return result

        if k_highest:
            selector = SelectKBest(score_func=lambda df, y: df.var(axis=0), k=k_highest)
        if percentile:
            selector = SelectPercentile(score_func=lambda df, y: df.var(axis=0), percentile=percentile)

        result = self.__get_selector_result(selector, df_feature, df_label, scores="variances_")
        return result

    def corr_selector(self, df_feature: pd.DataFrame, threshold: float = None):
        """
        相关性筛选器：当任意两特征之间相关性超过阈值时，删除靠前的特征。
        :param df_feature: 包括所有特征因子的dataframe
        :param threshold:  取方差大小为为前k_highest的特征

        :return: df_selected: 经过筛选后的df_feature
        """

        glog.info(f"Feature Selecting corr_selector threshold {threshold}")

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
        df_selected = df_feature.drop(columns=drop_list)

        return df_selected

    def pearsonr_corr_selector(self, df_feature: pd.DataFrame, df_label: pd.Series,
                               threshold: float = None, k_highest: int = None, percentile: int = None):
        """
        pearson相关系数筛选器：当某特征与标签之间相关系数小于一定阈值，删除该特征。
        :param df_feature: 包括所有特征因子的dataframe
        :param df_label:   标签列Series
        :param threshold:  取方差大小为为前k_highest的特征
        :param k_highest:  取方差大小为为前k_highest的特征
        :param percentile: 取方差大小为为前percentile%的特征 （参数取值通常为10，5等 代表前10% 5%）

        :return: df_selected: 经过筛选后的df_feature / sorted_result: 在该筛选方法下，各个特征对应取值的大小
        """
        glog.info(f"Feature Selecting pearsonr_corr_selector threshold {threshold} "
                  f"k_highest {k_highest} percentile {percentile}")

        if (threshold is None) and (k_highest is None) and (percentile is None):
            raise Exception(f"至少输入threshold & k_highest & percentile参数中的一个")

        pearsonr_label = partial(pearsonr, y=df_label)

        def udf_pcorr(df, *args):
            df = pd.DataFrame(df)
            df = df.apply(pearsonr_label).iloc[0]
            return df.abs()

        if threshold:
            pcorr_series = df_feature.apply(pearsonr_label).loc[0]
            drop_list = df_feature.columns[pcorr_series.abs() > threshold]
            df_dropped = df_feature.drop(columns=drop_list)

            sorted_result = pcorr_series.abs().sort_values().to_dict()

            return df_dropped, sorted_result

        if k_highest:
            selector = SelectKBest(score_func=udf_pcorr, k=k_highest)
        if percentile:
            selector = SelectPercentile(score_func=udf_pcorr, percentile=percentile)

        result = self.__get_selector_result(selector, df_feature, df_label)
        return result

    def fscore_selector(self, df_feature: pd.DataFrame, df_label: pd.Series,
                        k_highest: int = None, percentile: int = None):
        """
        F-Score系数筛选器：当某特征与标签之间的F统计量小于一定阈值，删除该特征。
        :param df_feature: 包括所有特征因子的dataframe
        :param df_label:   标签列Series
        :param k_highest:  取方差大小为为前k_highest的特征
        :param percentile: 取方差大小为为前percentile%的特征 （参数取值通常为10，5等 代表前10% 5%）

        :return: df_selected: 经过筛选后的df_feature / sorted_result: 在该筛选方法下，各个特征对应取值的大小
        """
        glog.info(f"Feature Selecting fscore_selector"
                  f"k_highest {k_highest} percentile {percentile}")

        if (k_highest is None) and (percentile is None):
            raise Exception(f"至少输入threshold & k_highest & percentile参数中的一个")

        if k_highest:
            selector = SelectKBest(score_func=f_regression, k=k_highest)

        if percentile:
            selector = SelectPercentile(score_func=f_regression, percentile=percentile)

        result = self.__get_selector_result(selector, df_feature, df_label)

        return result

    def mi_selector(self, df_feature: pd.DataFrame, df_label: pd.Series,
                    k_highest: int = None, percentile: int = None, n_neighbors=3):
        """
        Mutual Info 互信息筛选器：当某特征与标签之间的F统计量小于一定阈值，删除该特征。
        互信息（Mutual Information）衡量变量间的相互依赖性。其本质为熵差，即 𝐻(𝑋)−𝐻(𝑋|𝑌)，即知道另一个变量信息后混乱的降低程度 。
        当且仅当两个随机变量独立时MI等于零。MI值越高，两变量之间的相关性则越强。与Pearson相关和F统计量相比，它还捕获了非线性关系。

        :param df_feature: 包括所有特征因子的dataframe
        :param df_label:   标签列Series
        :param k_highest:  取方差大小为为前k_highest的特征
        :param percentile: 取方差大小为为前percentile%的特征 （参数取值通常为10，5等 代表前10% 5%）

        :return: df_selected: 经过筛选后的df_feature / sorted_result: 在该筛选方法下，各个特征对应取值的大小
        """
        glog.info(f"Feature Selecting mi_selector"
                  f"k_highest {k_highest} percentile {percentile}")

        if (k_highest is None) and (percentile is None):
            raise Exception(f"至少输入threshold & k_highest & percentile参数中的一个")

        mutual_info_regression_n_neighbors = partial(mutual_info_regression, n_neighbors=n_neighbors)

        if k_highest:
            selector = SelectKBest(score_func=mutual_info_regression_n_neighbors, k=k_highest)

        if percentile:
            selector = SelectPercentile(score_func=mutual_info_regression_n_neighbors, percentile=percentile)

        result = self.__get_selector_result(selector, df_feature, df_label)
        return result


if __name__ == "__main__":
    fs = FeatureSelector()
    df_selected, sorted_result = fs.mi_selector(df_feature=fs.data.iloc[:, :-1], df_label=fs.data.iloc[:, -1], percentile=1)
    print(df_selected, sorted_result)
