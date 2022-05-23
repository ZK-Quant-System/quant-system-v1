from config import base_config
from utils import path_wrapper
import sys

sys.path.append("../..")


market_data_path = path_wrapper.wrap_path(f"{base_config.base_data_path}/market_data")

stock_config = {
    "stock_index_list": ['000905.XSHG'],
    # stock_index_list: List[str] ['000300.XSHG', '000905.XSHG', '000852.XSHG']
    "legal_type_list": [float, int],
    "frequency": 'daily',
    "time_span": 1 * 365 * 24 * 60 * 60,
    "fields": ['open', 'close', 'high', 'low', 'volume', 'money', 'avg', 'factor'],
    # 'pre': 前复权;'post': 后复权;None: 不复权
    "fq": 'pre',
    # 'avg': 等权重;'mktcap': 按总市值加权;'ln_mktcap': 按总市值的对数加权;'cmktcap': 按流通市值加权;'ln_cmktcap': 按流通市值的对数加权
    "weight_method": 'avg',
    "market_data_file": f"{market_data_path}/market_data.pkl",
    "groupby_data_file": f"{market_data_path}/groupby_data.pkl",
    "market_cap_data_file": f"{market_data_path}/market_cap_data.pkl",
    "circulating_market_cap_data_file":  f"{market_data_path}/circulating_market_cap_data.pkl",
    "weight_data_file": f"{market_data_path}/weight_data.pkl",

}


cb_config = {
    "legal_type_list": [float, int],
    "list_status": '正常上市',
    "time_span": 5 * 250,
    "fields_market": ['code', 'date', 'open', 'close', 'high', 'low', 'volume', 'money'],
    "fields_basic": ['code', 'list_date', 'list_status', 'convert_code', 'convert_start_date', 'convert_end_date',
                     'convert_price'],
    "fields_price_adjust": ['code', 'adjust_date', 'new_convert_price'],
    "clear_list": ['open', 'close', 'high', 'low', 'volume', 'money', 'convert_price'],
    "cb_market_data_file": f"{market_data_path}/conbond_market_data.pkl"


}
