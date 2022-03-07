from functools import partial, reduce
import pandas as pd
from feature_selector import corr_selector
from feature_cleaner import outlier_replace, timestamp_matching, data_replace
from feature_constructor import symbolictransformer
from data_pipeline import DataPipeline


class DemoFeaturePipeline(DataPipeline):
    def __init__(self, data, label=None, feature_list=None):
        super().__init__(self.data, self.clean_func,  self.generate_func, self.select_func)

        self.data = data
        self.df_label = data[label]
        self.df_feature = data[feature_list]

    @staticmethod
    def compose(*functions):
        return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)

    @property
    def clean_func(self):
        outlier_replace_func = partial(outlier_replace, max_value=9999, min_value=-9999)
        timestamp_matching_func = partial(timestamp_matching)
        data_replace_func = partial(data_replace, method='ffill', null_scale=0.9)
        return self.compose(outlier_replace_func, timestamp_matching_func, data_replace_func)

    @property
    def generate_func(self):
        return partial(symbolictransformer, features=self.df_feature, target=self.df_label, prop=0.8)

    @property
    def select_func(self):
        return partial(corr_selector, threshold=0.8)

    def feature_flow(self):
        self.run_feature()


if __name__ == "__main__":
    df_test = pd.read_pickle('../feature_engineering/test_dataset.pkl')
    df_test = df_test.reset_index(drop=True)
    demo_pipeline = DemoFeaturePipeline(df_test, label='return', feature_list=[])
