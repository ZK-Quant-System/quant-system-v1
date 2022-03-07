from functools import partial, reduce
import pandas as pd
from data_pipeline import DataPipeline

from feature_selector import corr_selector
from feature_cleaner import outlier_replace, timestamp_matching, data_replace
from feature_constructor import symbolictransformer
from data_pipeline import DataPipeline


class DemoLabelPipeline(DataPipeline):
    def __init__(self, data, label=None, feature_list=None):
        super().__init__(data=self.data, clean_func=self.clean_func, generate_func=self.generate_label_func)

        self.data = data
        self.df_label = data[label]
        self.df_feature = data[feature_list]

    @property
    def generate_label_func(self):
        def foo(df):
            return df

        return foo

    @property
    def feature_select_func(self):
        return partial(corr_selector, threshold=0.8)

    def feature_flow(self):
        self.run_feature()


if __name__ == "__main__":
    df_test = pd.read_pickle('../feature_engineering/test_dataset.pkl')
    df_test = df_test.reset_index(drop=True)
    demo_pipeline = DemoFeaturePipeline(df_test, label='return', feature_list=[])
