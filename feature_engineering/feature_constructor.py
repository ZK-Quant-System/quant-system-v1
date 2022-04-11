import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import MaxAbsScaler
from sklearn.preprocessing import Normalizer
from sklearn.cluster import KMeans
from sklearn.preprocessing import Binarizer
from scipy import stats
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
from gplearn.genetic import SymbolicTransformer
from config import feature_pipeline_config


def label_ret(df:pd.DataFrame, n):
    df_label = df.pct_change(periods=n)
    return df_label


def standardized(df_feature: pd.DataFrame):
    df_new = pd.DataFrame(StandardScaler().fit_transform(df_feature))
    df_new.columns = 'Standardized_' + df_feature.columns
    df_new.index = df_feature.index
    return df_new


def minmax(df_feature: pd.DataFrame):
    df_new = pd.DataFrame(MinMaxScaler().fit_transform(df_feature))
    df_new.columns = 'MinMax_' + df_feature.columns
    df_new.index = df_feature.index
    return df_new


def maxabs(df_feature: pd.DataFrame):
    df_new = pd.DataFrame(MaxAbsScaler().fit_transform(df_feature))
    df_new.columns = 'MaxAbs_' + df_feature.columns
    df_new.index = df_feature.index
    return df_new


def normalization(df_feature):
    df_new = pd.DataFrame(Normalizer().fit_transform(df_feature))
    df_new.columns = 'Norm_' + df_feature.columns
    df_new.index = df_feature.index
    return df_new


def isometry_cut(df_feature: pd.DataFrame, num):
    df_new = pd.DataFrame(pd.cut(df_feature.stack(), num))
    df_new.columns = 'IsometryCut_' + df_feature.columns
    df_new.index = df_feature.index
    return df_new


def equifreq_cut(df_feature: pd.DataFrame, num):
    df_new = pd.DataFrame(pd.qcut(df_feature.stack(), num))
    df_new.columns = 'EquifreqCut_' + df_feature.columns
    df_new.index = df_feature.index
    return df_new


def binarizer(df_feature: pd.DataFrame, thre: float = None):
    df_new = pd.DataFrame(Binarizer(threshold=thre).fit_transform(df_feature))
    df_new.columns = 'binarizer_' + df_feature.columns
    df_new.index = df_feature.index
    return df_new


def feature_log(df_feature: pd.DataFrame):
    data = pd.DataFrame(np.log(df_feature))
    data.columns = 'log_' + df_feature.columns
    data.index = df_feature.index
    return data


def feature_power(df_feature: pd.DataFrame, power: int = None):
    data = pd.DataFrame(df_feature ** power)
    data.columns = df_feature.columns + 'power_' + str(power)
    data.index = df_feature.index
    return data


def feature_sqrt(df_feature: pd.DataFrame):
    data = pd.DataFrame(np.sqrt(df_feature))
    data.columns = 'sqrt_' + df_feature.columns
    data.index = df_feature.index
    return data


def feature_exp(df_feature: pd.DataFrame):
    data = pd.DataFrame(np.exp(df_feature))
    data.columns = 'exp_' + df_feature.columns
    data.index = df_feature.index
    return data


def feature_diff(df_feature: pd.DataFrame):
    data = df_feature.diff()
    data.columns = 'diff_'+ df_feature.columns
    data.index = df_feature.index
    return data


def symbolictransformer(features: pd.DataFrame, target: pd.DataFrame, prop: float = None):
    """
    基于gplearn的符号转化器
    :param features: 输入特征值
    :param target: 目标预测值
    :param prop: 训练集的比例
    :return: features_df: 生成新特征
    """
    features = np.array(features)
    target = np.array(target)
    gp = SymbolicTransformer(generations=10, population_size=1000,
                             hall_of_fame=100, n_components=10,
                             function_set=feature_config.function_set,
                             parsimony_coefficient=0.0005,
                             max_samples=0.9, verbose=1,
                             random_state=0, n_jobs=3)
    num = int(len(features) * prop)
    gp.fit(features[:num, :], target[:num])
    gp_features = gp.transform(features)
    features_df = pd.DataFrame(gp_features)
    features_df.columns = ['SymbolicTransformer_' + str(i) for i in range(features_df.shape[0]) ]
    return features_df

def kmeans(df_feature: pd.DataFrame, feature, k):
    """
    基于k均值聚类的分箱：k均值聚类法将观测值聚为k类
    :param df_feature: 包括所有特征因子的dataframe
    :param feature: 须进行聚类分箱的因子列名
    :param k: 聚类数量
    :return: df_new: 对指定列的分箱结果
    """
    length = len(df_feature)
    kmodel = KMeans(n_clusters=k)  # 聚成几类
    kmodel.fit(df_feature.values.reshape(length, -1))  # 训练模型
    center = pd.DataFrame(kmodel.cluster_centers_, columns=[feature])  # 求聚类中心
    center = center.sort_values(by=feature)  # 排序
    x = center.rolling(2).mean().iloc[1:, ]  # 滑窗计算均值
    w = [df_feature[feature].min() - 1] + list(x.loc[:, feature]) + [df_feature[feature].max()]  # 划分依据 (a,b]形式
    df_new = pd.DataFrame(pd.cut(df_feature[feature], w))  # 划分
    df_new.columns = ['kmeans_' + feature]
    df_new.index = df_feature.index
    return df_new


def BoxCox(df_feature: pd.DataFrame):
    """
    Box-Cox变换：须输入正数
    """
    a_feature = np.array(df_feature)
    feature_clean = a_feature[~np.isnan(a_feature)]  # 剔除零值
    l, opt_lambda = stats.boxcox(feature_clean)  # 计算最佳lamba
    df_new = pd.DataFrame(stats.boxcox(feature_clean.flatten(), lmbda=opt_lambda))
    df_new.columns = ['BoxCox_' + df_feature.columns]
    df_new.index = df_feature.index
    return df_new


def cartesian(feature_df, feature1_name, feature2_name):
    feature1_df = pd.get_dummies(feature_df[feature1_name])
    feature1_columns = feature1_df.columns
    feature2_df = pd.get_dummies(feature_df[feature2_name])
    feature2_columns = feature2_df.columns
    combine_df = pd.concat([feature1_df, feature2_df], axis=1)

    crosses_columns = []
    for feature1 in feature1_columns:
        for feature2 in feature2_columns:
            crosses_feature = '{}&{}'.format(feature1, feature2)
            crosses_columns.append(crosses_feature)
            combine_df[crosses_feature] = combine_df[feature1] * combine_df[feature2]
    combine_df = combine_df.loc[:, crosses_columns]

    return combine_df


def labelencoder(df_feature):
    feature_array = np.array(df_feature.stack())
    feature_list = feature_array.tolist()
    le = LabelEncoder()
    le.fit(feature_list)
    df_new = pd.DataFrame(le.transform(feature_list))
    df_new.columns = ['LabelEncoder']
    return df_new


def onehotencoder(factor):
    pass
