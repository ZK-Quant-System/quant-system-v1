import base_config, data_config, factor_config
import sys

sys.path.append("../..")
from utils import path_wrapper

data_center_path = path_wrapper.wrap_path(f"{base_config.base_data_path}/data_center/")
stock_daily_data_output_path = f'{data_center_path}/stock_daily_data.pkl'

register_info = {
    "stock_data": {
        "market_data": {
            "data_path": data_config.stock_config['market_data_file'],
            "other_info": None
        },
        "factor_alpha": {
            "data_path": factor_config.alpha_fusion_factor_config['alpha_fusion_factor_path'],
            "other_info": None
        },
        "other_data": None
    },

    "cb_data": {
        "market_data": {
            "data_path": data_config.cb_config['cb_market_data_file'],
            "other_info": None
        }
    }
}
