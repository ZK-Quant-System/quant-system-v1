
import qlib
import pandas as pd
from config import alpha_config
from qlib.data import D
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.data.handler import Alpha360
import click


class FactorAlpha:
    def __init__(self):
        self.start_time=alpha_config.start_time
        self.end_time=alpha_config.end_time
        self.fit_start_time=alpha_config.fit_start_time
        self.fit_end_time=alpha_config.fit_end_time
        self.instruments=None
        print('init')

    def get_data(self):
        qlib.init(provider_uri=alpha_config.provider_uri)
        self.instruments=D.instruments(market='csi100')
        print('get_data')
        return self.instruments

    def get_alpha101(self):
        pass

    def get_alpha158(self):
        data_handler_config={
            "start_time":self.start_time,
            "end_time":self.end_time,
            "fit_start_time":self.fit_start_time,
            "fit_end_time":self.fit_end_time,
            "instruments":self.instruments
        }

        h=Alpha158(**data_handler_config)
        Alpha158_df_feature=h.fetch(col_set="feature")
        new_columns_list=['alpha158_'+str(i) for i, column_str in enumerate(Alpha158_df_feature.columns,start=1)]
        new_columns=pd.core.indexes.base.Index(new_columns_list)
        Alpha158_df_feature.columns=new_columns
        print('get_alpha158')
        return Alpha158_df_feature

    def get_alpha360(self):
        data_handler_config={
            "start_time":self.start_time,
            "end_time":self.end_time,
            "fit_start_time":self.fit_start_time,
            "fit_end_time":self.fit_end_time,
            "instruments":self.instruments
        }

        h=Alpha360(**data_handler_config)
        Alpha360_df_feature=h.fetch(col_set="feature")
        new_columns_list = ['alpha360_' + str(i) for i, column_str in enumerate(Alpha360_df_feature.columns,start=1)]
        new_columns = pd.core.indexes.base.Index(new_columns_list)
        Alpha360_df_feature.columns = new_columns
        print('get_alpha360')
        return Alpha360_df_feature



def main():
    alpha=FactorAlpha()
    alpha.get_data()
    factor_alpha=pd.merge(alpha.get_alpha158(),alpha.get_alpha360(),left_index=True,right_index=True)
    print(factor_alpha)
    #print(alpha.get_alpha158())
    #print(alpha.get_alpha360())

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

