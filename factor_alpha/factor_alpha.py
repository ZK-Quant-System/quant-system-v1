import os.path
import glog
import os
import shutil
import pandas as pd
from config import alpha_config
import qlib
from qlib.data import D
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.data.handler import Alpha360



class FactorAlpha:
    def __init__(self):
        self.start_time=alpha_config.start_time
        self.end_time=alpha_config.end_time
        self.fit_start_time=alpha_config.fit_start_time
        self.fit_end_time=alpha_config.fit_end_time
        self.instruments=None
        glog.info("init finished")

    def get_data(self):
        if os.path.exists('./csv_data'):
            shutil.rmtree('./csv_data')
        if os.path.exists('./my_data'):
            shutil.rmtree('./my_data')
        os.makedirs('./csv_data')
        os.makedirs('./my_data')

        df_pickle=pd.read_pickle(alpha_config.market_data)
        market_data=df_pickle.sort_index(level=1).swaplevel('date','code')
        grouped=market_data.groupby('code')
        for name,group in grouped:
            csv_data=group.reset_index().drop(columns='code')
            csv_data.to_csv(os.path.join(r'./csv_data',name.replace('.','')+'.csv'),sep=',',header=True,index=False)
        os.system('python D:/Anaconda3/Lib/site-packages/qlib/scripts/dump_bin.py dump_all --csv_path ./csv_data --qlib_dir ./my_data include_fields open,close,high,volume,money --date_field_name date')
        #调用dump_bin.py将csv格式数据转化为qlib格式，dump_bin.py的路径一般为”qlib/scripts/dump_bin.py“
        #--csv_path --qlib_dir分别后跟csv所在路径、qlib格式数据输出所在路径；include_fields 后跟待转换的列
        #--date_field_name date 数据必须含有时间序列并标注列名；--symbol_field_name symbol 数据若含有股票代码列须标注并且代码不可含‘.’等字符

        qlib.init(provider_uri='./my_data')
        self.instruments=D.instruments(market='all')
        glog.info("get_data finished")

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
        glog.info("get_alpha158 finished")
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
        glog.info("get_alpha360 finished")
        return Alpha360_df_feature



def main():
    alpha=FactorAlpha()
    alpha.get_data()
    factor_alpha=pd.merge(alpha.get_alpha158(),alpha.get_alpha360(),left_index=True,right_index=True)
    factor_alpha.to_pickle(alpha_config.factor_alpha)
    shutil.rmtree("./csv_data")
    shutil.rmtree("./my_data")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

