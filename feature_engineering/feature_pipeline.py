import pandas as pd
from config import feature_config
from PartialFunc import corr_selector_p, feature_power_p, outlier_replace_p


class FeaturePipeline:
    def __init__(self, df, label=None, select_func=None, generate_func=None, clean_func=None):
        self.config = feature_config
        self.df = df
        self.label = label
        self.df_feature = pd.DataFrame()
        self.df_label = pd.DataFrame()
        self.select_func = select_func
        self.generate_func = generate_func
        self.clean_func = clean_func

    def clean_data(self):
        df_cleaned = self.clean_func(self.df)
        self.df_feature = df_cleaned.drop(columns=self.label)
        self.df_label = df_cleaned[self.label]

    def generate_feature(self):
        self.df_feature = self.generate_func(self.df_feature)

    def clean_feature(self):
        self.df_feature = self.clean_func(self.df_feature)

    def select_feature(self):
        self.df_feature = self.select_func(self.df_feature)

    def run(self):
        self.clean_data()
        self.generate_feature()
        self.select_feature()
        return self.df_feature


if __name__ == "__main__":
    df_test = pd.read_pickle('../feature_engineering/test_dataset.pkl')
    df_test = df_test.reset_index(drop=True)
    DP = FeaturePipeline(df_test, label=['return'],
                         select_func=corr_selector_p,
                         generate_func=feature_power_p,
                         clean_func=outlier_replace_p)
    print(DP.run())
