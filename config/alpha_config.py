data_path='/ZK-quant-system/data/'
market_data=data_path+'market_data_with_double_index.pkl'
factor_alpha=data_path+'factor_alpha.pkl'
csv_dir=data_path+'csv_data/'
qlib_dir=data_path+'my_data/'

#转换为qlib格式
dump_bin_path='/usr/local/lib/python3.7/site-packages/qlib/scripts/'
dump_bin=dump_bin_path+'dump_bin.py'
include_fields='open,close,high,volume,money'
column_date='date'
column_stockid=''
#
start_time='2020-01-01'
end_time='2020-01-30'
fit_start_time='2020-01-01'
fit_end_time='2020-01-30'
freq='day'
