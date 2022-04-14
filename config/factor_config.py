from . import base_config
import qlib
import os
import sys

sys.path.append("../..")
from utils import path_wrapper

factor_base_path = path_wrapper.wrap_path(f"{base_config.base_data_path}/factors")

tech_factor_config = {
    "tech_factor_path":  f"{factor_base_path}/tech_factors.pkl",
}

alpha_fusion_factor_config = {
    "alpha_fusion_factor_path":  f"{factor_base_path}/factor_alpha_fusion.pkl",
    "start_time": '2021-04-06',
    "end_time": '2021-04-01',
    "fit_start_time": '2021-04-06',
    "fit_end_time": '2022-04-01',
    "freq": "day",
    "csv_dir": f"{factor_base_path}/csv_data",
    "qlib_dir": f"{factor_base_path}/qlib_data",
    "dump_bin_script_path": f"{os.path.dirname(qlib.__file__)}/scripts/dump_bin.py",
    "include_fields": 'open,close,high,volume,money',
    "column_date": 'date',
    "column_stock_id": '',
}

