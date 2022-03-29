from functools import partial, reduce
import pandas as pd
from feature_selector import corr_selector
from feature_cleaner import outlier_replace, timestamp_matching, data_replace
from feature_constructor import symbolictransformer
from label_constructor import calc_return


class DemoFeaturePipeline:
    def __init__(self, data, label=None, feature_list=None):
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
        outlier_replace_func = partial(outlier_replace, max_value=9999, min_value=-9999)
        timestamp_matching_func = partial(timestamp_matching)
        data_replace_func = partial(data_replace, method='ffill', null_scale=0.9)
        clean_func = self.compose(outlier_replace_func, timestamp_matching_func, data_replace_func)
        return clean_func(data)

    def generate_feature(self):
        self.df_feature_generated = symbolictransformer(features=self.df_feature.copy(deep=True),
                                                        target=self.df_label.copy(deep=True), prop=0.8)

    def select_feature(self):
        self.df_feature = corr_selector(self.df_feature, threshold=0.9)

    def concatenate_data(self):
        return pd.concat([self.df_label, self.df_feature, self.df_feature_generated], join='outer')

    def run(self):
        self.generate_feature()
        self.select_feature()
        self.data_concat = self.concatenate_data()
        return self.clean_data(self.data_concat)


if __name__ == "__main__":
    df_test = pd.read_pickle('./test_dataset.pkl')
    # hack for now
    df_test['close'] = df_test['return']
    demo_pipeline = DemoFeaturePipeline(df_test, label='return', feature_list=df_test.columns)
    demo_pipeline.run()
