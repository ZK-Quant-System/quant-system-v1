import pandas as pd
import numpy as np
from scipy.stats import pearsonr
import glog
from functools import partial
from sklearn.feature_selection import VarianceThreshold, SelectKBest, \
    SelectPercentile, f_regression, mutual_info_regression, SelectFromModel
from sklearn.linear_model import Ridge, Lasso, ElasticNet
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import AdaBoostRegressor, RandomForestRegressor
from lightgbm.sklearn import LGBMRegressor


def get_sorted_values(df, selector, scores):
    values = eval(f"selector.{scores}")
    keys = df.columns
    dictionary = {key: value for key, value in zip(keys, values)}
    sorted_result = dict(sorted(dictionary.items(), key=lambda dictionary: dictionary[1], reverse=True))
    return sorted_result


def get_selector_result(selector, df_feature, df_label, scores='scores_'):
    df_selected = pd.DataFrame(selector.fit_transform(df_feature, df_label))

    remained_columns = df_feature.columns[selector.get_support(indices=True)]
    df_selected.index = df_feature.index
    df_selected.columns = remained_columns
    sorted_result = get_sorted_values(df_feature, selector, scores)

    return df_selected, sorted_result


def embedded_selector(df_feature, df_label, estimator, k_highest, percentile,
                      threshold, prefit, scores):
    if k_highest:
        max_features = k_highest
        threshold = float("-inf")
    if percentile:
        max_features = round((percentile / 100) * df_feature.shape[1])
        threshold = float("-inf")

    selector = SelectFromModel(estimator, threshold=threshold, max_features=max_features, prefit=prefit)
    result = get_selector_result(selector, df_feature, df_label, scores="estimator_." + scores)

    return result


def variance_selector(df_feature: pd.DataFrame, df_label: pd.Series,
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
        result = get_selector_result(selector, df_feature, df_label, scores="variances_")
        return result

    if k_highest:
        selector = SelectKBest(score_func=lambda df, y: df.var(axis=0), k=k_highest)
    if percentile:
        selector = SelectPercentile(score_func=lambda df, y: df.var(axis=0), percentile=percentile)

    result = get_selector_result(selector, df_feature, df_label, scores="variances_")
    return result


def corr_selector(df_feature: pd.DataFrame, threshold: float = None):
    """
    相关性筛选器：当任意两特征之间相关性超过阈值时，删除靠前的特征。
    :param df_feature: 包括所有特征因子的dataframe
    :param threshold:  相关性阈值

    :return: df_selected: 经过筛选后的df_feature
    """

    glog.info(f"Feature Selecting corr_selector threshold {threshold}")

    df_feature = df_feature.astype('float')
    df_corr = df_feature.corr()
    triu_select = np.triu(np.ones(df_corr.shape), k=1).astype('bool')
    df_corr_stack = df_corr.where(triu_select).stack()
    threshold_select = df_corr_stack > threshold
    df_corr_drop = df_corr_stack[threshold_select]

    drop_list = list(df_corr_drop.index.get_level_values(1))
    df_selected = df_feature.drop(columns=drop_list)

    return df_selected


def pearsonr_corr_selector(df_feature: pd.DataFrame, df_label: pd.Series,
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

    def udf_pcorr(df):
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

    result = get_selector_result(selector, df_feature, df_label)
    return result


def fscore_selector(df_feature: pd.DataFrame, df_label: pd.Series,
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

    result = get_selector_result(selector, df_feature, df_label)

    return result


def mi_selector(df_feature: pd.DataFrame, df_label: pd.Series,
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

    result = get_selector_result(selector, df_feature, df_label)
    return result


def embedded_ridge(df_feature: pd.DataFrame, df_label: pd.Series, alpha: float = 1.0, fit_intercept=False,
                   random_state=None, normalize=False, k_highest=None, percentile=None, threshold='mean'):
    estimator = Ridge(alpha=alpha,
                      normalize=normalize,
                      random_state=random_state,
                      fit_intercept=fit_intercept)

    result = embedded_selector(df_feature, df_label,
                               estimator=estimator,
                               k_highest=k_highest,
                               percentile=percentile,
                               threshold=threshold,
                               prefit=False,
                               scores="coef_")
    return result


def embedded_lasso(df_feature: pd.DataFrame, df_label: pd.Series, alpha: float = 1.0, fit_intercept=False,
                   random_state=None, normalize=False, k_highest=None, percentile=None, threshold='mean'):
    estimator = Lasso(alpha=alpha,
                      fit_intercept=fit_intercept,
                      normalize=normalize,
                      random_state=random_state)

    result = embedded_selector(df_feature, df_label,
                               estimator=estimator,
                               k_highest=k_highest,
                               percentile=percentile,
                               threshold=threshold,
                               prefit=False,
                               scores="coef_")
    return result


def embedded_elastic_net(df_feature: pd.DataFrame, df_label: pd.Series, alpha: float = 1.0,
                         l1_ratio: float = 0.5, fit_intercept=False, random_state=None,
                         normalize=False, k_highest=None, percentile=None, threshold='mean'):
    estimator = ElasticNet(alpha=alpha,
                           l1_ratio=l1_ratio,
                           fit_intercept=fit_intercept,
                           normalize=normalize,
                           random_state=random_state)

    result = embedded_selector(df_feature, df_label,
                               estimator=estimator,
                               k_highest=k_highest,
                               percentile=percentile,
                               threshold=threshold,
                               prefit=False,
                               scores="coef_")
    return result


def embedded_decision_tree_regressor(df_feature: pd.DataFrame, df_label: pd.Series, criterion='mse',
                                     splitter='best', max_depth=None, min_samples_split=2,
                                     min_samples_leaf=1, random_state=None, max_leaf_nodes=None,
                                     k_highest=None, percentile=None, threshold='mean'):
    estimator = DecisionTreeRegressor(criterion=criterion, splitter=splitter, max_depth=max_depth,
                                      min_samples_split=min_samples_split, min_samples_leaf=min_samples_leaf,
                                      random_state=random_state, max_leaf_nodes=max_leaf_nodes,
                                      )

    result = embedded_selector(df_feature, df_label,
                               estimator=estimator,
                               k_highest=k_highest,
                               percentile=percentile,
                               threshold=threshold,
                               prefit=False,
                               scores="feature_importances_")
    return result


def embedded_adaboost_regressor(df_feature: pd.DataFrame, df_label: pd.Series, loss='square',
                                learning_rate=1.0, n_estimators=50, random_state=None,
                                k_highest=None, percentile=None, threshold='mean'):
    estimator = AdaBoostRegressor(n_estimators=n_estimators, learning_rate=learning_rate, loss=loss,
                                  random_state=random_state)

    result = embedded_selector(df_feature, df_label,
                               estimator=estimator,
                               k_highest=k_highest,
                               percentile=percentile,
                               threshold=threshold,
                               prefit=False,
                               scores="feature_importances_")
    return result


def embedded_random_forest_regressor(df_feature: pd.DataFrame, df_label: pd.Series,
                                     criterion='mse', min_samples_leaf=1, min_samples_split=2,
                                     max_depth=None, n_estimators=50, random_state=None, k_highest=None,
                                     percentile=None, threshold='mean'):
    estimator = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth, criterion=criterion,
                                      min_samples_split=min_samples_split, min_samples_leaf=min_samples_leaf,
                                      random_state=random_state)

    result = embedded_selector(df_feature, df_label,
                               estimator=estimator,
                               k_highest=k_highest,
                               percentile=percentile,
                               threshold=threshold,
                               prefit=False,
                               scores="feature_importances_")
    return result


def embedded_lgb_regressor(df_feature: pd.DataFrame, df_label: pd.Series,
                           boosting_type="gbdt", num_leaves=31, n_estimators=100, learning_rate=1.0,
                           random_state=None, objective="regression", importance_type="gain",
                           k_highest=None, percentile=None, threshold='mean'):
    df_new = df_feature.copy(deep=True)
    df_new.columns = np.arange(df_new.shape[1])

    estimator = LGBMRegressor(boosting_type=boosting_type, num_leaves=num_leaves,
                              learning_rate=learning_rate, n_estimators=n_estimators,
                              objective=objective, random_state=random_state,
                              importance_type=importance_type)

    result = embedded_selector(df_feature, df_label,
                               estimator=estimator,
                               k_highest=k_highest,
                               percentile=percentile,
                               threshold=threshold,
                               prefit=False,
                               scores="feature_importances_")
    return result

