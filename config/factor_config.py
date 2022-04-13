from . import base_config
import qlib
import os

factor_base_path = f"{base_config.base_data_path}/factors"

tech_factor_config = {
    "tech_factor_path":  f"{factor_base_path}/tech_factors.pkl",
}

alpha_fusion_factor_config = {
    "alpha_fusion_factor_path":  f"{factor_base_path}/factor_alpha_fusion.pkl",
    "start_time": '2020-01-01',
    "end_time": '2020-01-30',
    "fit_start_time": '2020-01-01',
    "fit_end_time": '2020-01-30',
    "freq": "day",
    "csv_dir": f"{factor_base_path}/csv_data",
    "qlib_dir": f"{factor_base_path}/qlib_data",
    "dump_bin_script_path": f"{os.path.dirname(qlib.__file__)}/scripts/dump_bin.py",
    "include_fields": 'open,close,high,volume,money',
    "column_date": 'date',
    "column_stock_id": '',
}

