from functools import partial, reduce

import glog
import pandas as pd
from typing import List
from feature_selector import corr_selector
from data_cleaner import outlier_replace, timestamp_matching, data_replace
from feature_constructor import symbolictransformer
from label_constructor import calc_return


class DemoFeaturePipeline:
    def __init__(self, data: pd.DataFrame, label: str, feature_list: List[str]):
        self.data = data
        self.data_cleaned = self.clean_data(data)
        self.label = label
        self.feature_list = feature_list

        self.df_label = calc_return(df=self.data_cleaned, price_type='close', time_lag=1, label=self.label)
        self.df_feature = self.data_cleaned.copy(deep=True)[self.feature_list]

    @staticmethod
    def compose(*functions):
        return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)

    def clean_data(self, data):
        outlier_replace_func = partial(outlier_replace, min_value=-99999)
        timestamp_matching_func = partial(timestamp_matching)
        data_replace_func = partial(data_replace, method='ffill', null_scale=0.9, handle_constant=False)
        clean_func = self.compose(outlier_replace_func, timestamp_matching_func, data_replace_func)
        return clean_func(data)

    def generate_feature(self):
        df_feature_generated = symbolictransformer(features=self.df_feature.copy(deep=True),
                                                   target=self.df_label.copy(deep=True), prop=0.8)
        return df_feature_generated

    @staticmethod
    def select_feature(df_feature):
        df_feature_selected = corr_selector(df_feature, threshold=0.7)
        return df_feature_selected

    @staticmethod
    def concatenate_data(data_list: List[pd.DataFrame]):
        return pd.concat(data_list, join='outer')

    def run(self):
        df_feature_generated = self.generate_feature()
        df_selected = self.select_feature(self.concatenate_data(data_list=[self.df_feature, df_feature_generated]))
        data_concat = self.concatenate_data(data_list=[self.df_label, df_selected])
        return self.clean_data(data_concat)


if __name__ == "__main__":
    df_test = pd.read_pickle('../data/data_center/stock_daily_data.pkl')
    demo_pipeline = DemoFeaturePipeline(df_test, label='return', feature_list=df_test.columns)
    glog.info("Starting run DataPipeline")
    df_featured = demo_pipeline.run()
    glog.info("Ending run DataPipeline")
    print(df_featured)
