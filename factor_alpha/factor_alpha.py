import os.path

import qlib
import pandas as pd
from config import alpha_config
from qlib.data import D
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.data.handler import Alpha360
import os
import shutil


class FactorAlpha:
    def __init__(self):
        self.start_time=alpha_config.start_time
        self.end_time=alpha_config.end_time
        self.fit_start_time=alpha_config.fit_start_time
        self.fit_end_time=alpha_config.fit_end_time
        self.instruments=None
        print('init')

    def get_data(self):
        os.makedirs('./csv_data')
        os.makedirs('./my_data')
        df1=pd.read_pickle('./market_data_with_double_index.pkl')
        df2=df1.sort_index(level=1)
        market_data=df2.swaplevel('date','code')
        grouped=market_data.groupby('code')
        for name,group in grouped:
            csv_data=group.reset_index().drop(columns='code')
            csv_data.to_csv(os.path.join(r'./csv_data',name.replace('.','')+'.csv'),sep=',',header=True,index=False)
        os.system('python D:/Anaconda3/Lib/site-packages/qlib/scripts/dump_bin.py dump_all --csv_path ./csv_data --qlib_dir ./my_data include_fields open,close,high,volume,money --date_field_name date')

        qlib.init(provider_uri='./my_data')
        self.instruments=D.instruments(market='all')
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
    #factor_alpha=pd.merge(alpha.get_alpha158(),alpha.get_alpha360(),left_index=True,right_index=True)
    #print(factor_alpha)
    print(alpha.get_alpha158())
    #print(alpha.get_alpha360())
    shutil.rmtree("./csv_data")
    shutil.rmtree("./my_data")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

