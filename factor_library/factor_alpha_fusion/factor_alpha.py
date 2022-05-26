import os.path
import glog
import os
import shutil
import pandas as pd
import qlib
from qlib.data import D
from qlib.contrib.data.handler import Alpha158
from qlib.contrib.data.handler import Alpha360
from config import factor_config, data_config
from utils import path_wrapper
import sys
from factor_library.factor_alpha101 import alpha101


work_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(work_path)


class FactorAlpha:
    def __init__(self):
        self.config = factor_config.alpha_fusion_factor_config
        self.start_time = self.config["start_time"]
        self.end_time = self.config["end_time"]
        self.fit_start_time = self.config["fit_start_time"]
        self.fit_end_time = self.config["fit_end_time"]
        self.instruments = None
        self.df_data = pd.read_pickle(data_config.market_data_path)
        glog.info("init finished")

    def get_qlib_data(self):
        """
        调用dump_bin.py将csv格式数据转化为qlib格式，dump_bin.py的路径一般为”qlib/scripts/dump_bin.py“
        --csv_path --qlib_dir分别后跟csv所在路径、qlib格式数据输出所在路径；include_fields 后跟待转换的列
        --date_field_name date 数据必须含有时间序列并标注列名；
        --symbol_field_name symbol 数据若含有股票代码列须标注并且代码不可含‘.’等字符
        """
        csv_dir = path_wrapper.wrap_path(self.config["csv_dir"])
        qlib_dir = path_wrapper.wrap_path(self.config["qlib_dir"])

        # df_pickle = pd.read_pickle(data_config.market_data_path)
        market_data = self.df_data.sort_index(level=1).swaplevel('date', 'code')
        grouped = market_data.groupby('code')
        for name, group in grouped:
            csv_data = group.reset_index().drop(columns='code')
            csv_data.to_csv(os.path.join(self.config["csv_dir"], name.replace('.', '') + '.csv'), sep=',', header=True,
                            index=False)

        dump_bin_script_path = self.config['dump_bin_script_path']
        os.system(
            f'python {dump_bin_script_path} dump_all --csv_path '
            f'{csv_dir} --qlib_dir {qlib_dir} include_fields {self.config["include_fields"]} '
            f'--date_field_name {self.config["column_date"]}')  # python3

        qlib.init(provider_uri=qlib_dir)
        self.instruments = D.instruments(market='all')
        glog.info("get_data finished")

    def get_alpha158(self):
        data_handler_config = {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "fit_start_time": self.fit_start_time,
            "fit_end_time": self.fit_end_time,
            "instruments": self.instruments
        }

        h = Alpha158(**data_handler_config)
        df_alpha158 = h.fetch(col_set="feature")
        new_columns_list = ['alpha158_' + str(i) for i, column_str in enumerate(df_alpha158.columns, start=1)]
        new_columns = pd.Index(new_columns_list)
        df_alpha158.columns = new_columns
        glog.info("get_alpha158 finished")
        return df_alpha158

    def get_alpha360(self):
        data_handler_config = {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "fit_start_time": self.fit_start_time,
            "fit_end_time": self.fit_end_time,
            "instruments": self.instruments
        }

        h = Alpha360(**data_handler_config)
        df_alpha360 = h.fetch(col_set="feature")
        new_columns_list = ['alpha360_' + str(i) for i, column_str in enumerate(df_alpha360.columns, start=1)]
        new_columns = pd.Index(new_columns_list)
        df_alpha360.columns = new_columns
        glog.info("get_alpha360 finished")
        return df_alpha360

    def get_alpha101(self):
        market_data = self.df_data.sort_index(level=1).swaplevel('date', 'code')
        grouped = market_data.groupby('code')
        df_alpha101 = pd.DataFrame()
        for name, group in grouped:
            df = pd.DataFrame()
            for i in range(1, 102):
                factor_function = getattr(alpha101.Alpha101(group), 'alpha' + str(i))
                df_alpha = factor_function()
                df = pd.concat([df, df_alpha], axis=1)
            df_alpha101 = pd.concat([df_alpha101, df])
        df_alpha101.columns = ['alpha101_' + str(i) for i, column_str in enumerate(df_alpha101.columns, start=1)]
        print(df_alpha101)
        return df_alpha101

    def run(self):
        self.get_qlib_data()
        factor_alpha = pd.merge(self.get_alpha158(), self.get_alpha360(), left_index=True, right_index=True)
        factor_alpha = pd.merge(factor_alpha, self.get_alpha101(), left_index=True, right_index=True)
        factor_alpha.to_pickle(self.config['alpha_fusion_factor_path'])
        shutil.rmtree(self.config['csv_dir'])
        shutil.rmtree(self.config['qlib_dir'])


def main():
    # qlib.init()
    factor_alpha = FactorAlpha()
    factor_alpha.run()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
