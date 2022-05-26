import pandas as pd
import glog
import talib
from config import factor_config, data_config
from feature_engineering import data_cleaner
import sys

sys.path.append('../..')


class TechFactorsCalculator:
    def __init__(self, df_data):
        self.df_data = df_data

    @staticmethod
    def get_tech_factor(df_market_data):
        _close = df_market_data['close']
        _open = df_market_data['open']
        _high = df_market_data['high']
        _low = df_market_data['low']
        _volume = df_market_data['volume']
        _code = df_market_data['code']
        _date = df_market_data['date']

        tech_factor_map = dict()

        tech_factor_map['date'] = _date
        tech_factor_map['code'] = _code

        glog.info(f'{_code.iloc[0]} Get cycle indicators.')
        tech_factor_map['HT_DCPERIOD'] = talib.HT_DCPERIOD(_close)  # Cycle Indicators
        tech_factor_map['HT_DCPHASE'] = talib.HT_DCPHASE(_close)  # Cycle Indicators
        tech_factor_map['HT_PHASOR_inphase'], tech_factor_map['HT_PHASOR_quadrature'] = talib.HT_PHASOR(
            _close)  # Cycle Indicators
        tech_factor_map['HT_SINE_sine'], tech_factor_map['HT_SINE_leadsine'] = talib.HT_SINE(
            _close)  # Cycle Indicators
        tech_factor_map['HT_TRENDMODE'] = talib.HT_TRENDMODE(_close)  # Cycle Indicators

        glog.info(f'{_code.iloc[0]} Get overlap studies indicators.')
        tech_factor_map['HT_TRENDLINE'] = talib.HT_TRENDLINE(_close)  # Overlap Studies
        tech_factor_map['MAMA_mama'], tech_factor_map['MAMA_fama'] = talib.MAMA(_close, fastlimit=0.5,
                                                                                slowlimit=0.05)
        tech_factor_map['SAR'] = talib.SAR(_high, _low, acceleration=0.02, maximum=0.2)  # Overlap Studies
        tech_factor_map['SAREXT'] = talib.SAREXT(_high, _low, startvalue=0, offsetonreverse=0,
                                                 accelerationinitlong=0.02,
                                                 accelerationlong=0.2, accelerationmaxlong=0.02,
                                                 accelerationinitshort=0.02, accelerationshort=0.02,
                                                 accelerationmaxshort=0.2)  # Overlap Studies
        tech_factor_map['BBANDS_upperband'], tech_factor_map['BBANDS_middleband'], tech_factor_map[
            'BBANDS__lowerband'] = talib.BBANDS(_close, timeperiod=5, nbdevup=2, nbdevdn=2,
                                                matype=0)  # Overlap Studies
        tech_factor_map['DEMA'] = talib.DEMA(_close, timeperiod=30)  # Overlap Studies
        tech_factor_map['EMA'] = talib.EMA(_close, timeperiod=30)  # Overlap Studies
        tech_factor_map['KAMA'] = talib.KAMA(_close, timeperiod=30)  # Overlap Studies
        tech_factor_map['MA'] = talib.MA(_close, timeperiod=30, matype=0)  # Overlap Studies
        tech_factor_map['SMA'] = talib.SMA(_close, timeperiod=30)  # Overlap Studies
        tech_factor_map['TEMA'] = talib.TEMA(_close, timeperiod=30)  # Overlap Studies
        tech_factor_map['TRIMA'] = talib.TRIMA(_close, timeperiod=30)  # Overlap Studies
        tech_factor_map['WMA'] = talib.WMA(_close, timeperiod=30)  # Overlap Studies
        tech_factor_map['MIDPOINT'] = talib.MIDPOINT(_close, timeperiod=14)  # Overlap Studies
        tech_factor_map['MIDPRICE'] = talib.MIDPRICE(_high, _low, timeperiod=14)  # Overlap Studies
        tech_factor_map['T3'] = talib.T3(_close, timeperiod=5, vfactor=0.7)  # Overlap Studies

        glog.info(f'{_code.iloc[0]} Get volatility indicators.')
        tech_factor_map['TRANGE'] = talib.TRANGE(_high, _low, _close)  # Volatility Indicators
        tech_factor_map['ATR'] = talib.ATR(_high, _low, _close, timeperiod=14)  # Volatility Indicators
        tech_factor_map['NATR'] = talib.NATR(_high, _low, _close,
                                             timeperiod=14)  # Volatility Indicators

        tech_factor_map['AD'] = talib.AD(_high, _low, _close, _volume)  # _volume Indicators
        tech_factor_map['OBV'] = talib.OBV(_close, _volume)  # _volume Indicators
        tech_factor_map['ADOSC'] = talib.ADOSC(_high, _low, _close, _volume, fastperiod=3,
                                               slowperiod=10)  # _volume Indicators

        glog.info(f'{_code.iloc[0]} Get momentum indicators.')
        tech_factor_map['BOP'] = talib.BOP(_open, _high, _low, _close)  # Momentum Indicators
        tech_factor_map['ADX'] = talib.ADX(_high, _low, _close, timeperiod=14)  # Momentum Indicators
        tech_factor_map['ADXR'] = talib.ADXR(_high, _low, _close, timeperiod=14)  # Momentum Indicators
        tech_factor_map['AROON_aroondown'], tech_factor_map['AROON_aroondown'] = talib.AROON(_high,
                                                                                             _low,
                                                                                             timeperiod=14)
        tech_factor_map['AROONOSC'] = talib.AROONOSC(_high, _low, timeperiod=14)  # Momentum Indicators
        tech_factor_map['CCI'] = talib.CCI(_high, _low, _close, timeperiod=14)  # Momentum Indicators
        tech_factor_map['CMO'] = talib.CMO(_close, timeperiod=14)  # Momentum Indicators
        tech_factor_map['DX'] = talib.DX(_high, _low, _close, timeperiod=14)  # Momentum Indicators
        tech_factor_map['MFI'] = talib.MFI(_high, _low, _close, _volume,
                                           timeperiod=14)  # Momentum Indicators
        tech_factor_map['MINUS_DI'] = talib.MINUS_DI(_high, _low, _close,
                                                     timeperiod=14)  # Momentum Indicators
        tech_factor_map['MINUS_DM'] = talib.MINUS_DM(_high, _low, timeperiod=14)  # Momentum Indicators
        tech_factor_map['PLUS_DI'] = talib.PLUS_DI(_high, _low, _close,
                                                   timeperiod=14)  # Momentum Indicators
        tech_factor_map['PLUS_DM'] = talib.PLUS_DM(_high, _low, timeperiod=14)  # Momentum Indicators
        tech_factor_map['RSI'] = talib.RSI(_close, timeperiod=14)  # Momentum Indicators
        tech_factor_map['WILLR'] = talib.WILLR(_high, _low, _close,
                                               timeperiod=14)  # Momentum Indicators
        tech_factor_map['APO'] = talib.APO(_close, fastperiod=12, slowperiod=26,
                                           matype=0)  # Momentum Indicators
        tech_factor_map['PPO'] = talib.PPO(_close, fastperiod=12, slowperiod=26,
                                           matype=0)  # Momentum Indicators
        tech_factor_map['MACD_macd'], tech_factor_map[
            'MACD_macdsignal'], tech_factor_map[
            'MACD_macdhist'] = talib.MACD(_close, fastperiod=12, slowperiod=26,
                                          signalperiod=9)  # Momentum Indicators
        tech_factor_map['MACDEXT_macd'], tech_factor_map[
            'MACDEXT_macdsignal'], tech_factor_map[
            'MACDEXT_macdhist'] = talib.MACDEXT(_close, fastperiod=12,
                                                fastmatype=0, slowperiod=26,
                                                slowmatype=0,
                                                signalperiod=9,
                                                signalmatype=0)  # Momentum Indicators
        tech_factor_map['MACDFIX_macd'], tech_factor_map['MACDFIX_macdsignal'], tech_factor_map[
            'MACDFIX_macdhist'] = talib.MACDFIX(_close, signalperiod=9)  # Momentum Indicators
        tech_factor_map['MOM'] = talib.MOM(_close, timeperiod=10)  # Momentum Indicators
        tech_factor_map['ROC'] = talib.ROC(_close, timeperiod=10)  # Momentum Indicators
        tech_factor_map['ROCP'] = talib.ROCP(_close, timeperiod=10)  # Momentum Indicators
        tech_factor_map['ROCR'] = talib.ROCR(_close, timeperiod=10)  # Momentum Indicators
        tech_factor_map['ROCR100'] = talib.ROCR100(_close, timeperiod=10)  # Momentum Indicators
        tech_factor_map['TRIX'] = talib.TRIX(_close, timeperiod=30)  # Momentum Indicators
        tech_factor_map['STOCH_slowk'], tech_factor_map[
            'STOCH_slowd'] = talib.STOCH(_high, _low, _close,
                                         fastk_period=5, slowk_period=3,
                                         slowk_matype=0, slowd_period=3,
                                         slowd_matype=0)  # Momentum Indicators
        tech_factor_map['STOCHF_fastk'], tech_factor_map[
            'STOCHF_fastd'] = talib.STOCHF(_high, _low, _close,
                                           fastk_period=5, fastd_period=3,
                                           fastd_matype=0)  # Momentum Indicators
        tech_factor_map['STOCHRSI_fastk'], tech_factor_map[
            'STOCHRSI_fastd'] = talib.STOCHRSI(_close, timeperiod=14,
                                               fastk_period=5,
                                               fastd_period=3,
                                               fastd_matype=0)  # Momentum Indicators
        tech_factor_map['ULTOSC'] = talib.ULTOSC(_high, _low, _close,
                                                 timeperiod1=7, timeperiod2=14,
                                                 timeperiod3=28)  # Momentum Indicators

        glog.info(f'{_code.iloc[0]} Get pattern recognition indicators.')
        tech_factor_map['CDL2CROWS'] = talib.CDL2CROWS(_open, _high, _low,
                                                       _close)  # Pattern Recognition
        tech_factor_map['CDL3BLACKCROWS'] = talib.CDL3BLACKCROWS(_open, _high, _low,
                                                                 _close)  # Pattern Recognition
        tech_factor_map['CDL3INSIDE'] = talib.CDL3INSIDE(_open, _high, _low,
                                                         _close)  # Pattern Recognition
        tech_factor_map['CDL3LINESTRIKE'] = talib.CDL3LINESTRIKE(_open, _high, _low,
                                                                 _close)  # Pattern Recognition
        tech_factor_map['CDL3OUTSIDE'] = talib.CDL3OUTSIDE(_open, _high, _low,
                                                           _close)  # Pattern Recognition
        tech_factor_map['CDL3STARSINSOUTH'] = talib.CDL3STARSINSOUTH(_open, _high, _low,
                                                                     _close)  # Pattern Recognition
        tech_factor_map['CDL3WHITESOLDIERS'] = talib.CDL3WHITESOLDIERS(_open, _high, _low,
                                                                       _close)  # Pattern Recognition
        tech_factor_map['CDLADVANCEBLOCK'] = talib.CDLADVANCEBLOCK(_open, _high, _low,
                                                                   _close)  # Pattern Recognition
        tech_factor_map['CDLBELTHOLD'] = talib.CDLBELTHOLD(_open, _high, _low,
                                                           _close)  # Pattern Recognition
        tech_factor_map['CDLBREAKAWAY'] = talib.CDLBREAKAWAY(_open, _high, _low,
                                                             _close)  # Pattern Recognition
        tech_factor_map['CDLCLOSINGMARUBOZU'] = talib.CDLCLOSINGMARUBOZU(_open, _high, _low,
                                                                         _close)  # Pattern Recognition
        tech_factor_map['CDLCONCEALBABYSWALL'] = talib.CDLCONCEALBABYSWALL(_open, _high, _low,
                                                                           _close)  # Pattern Recognition
        tech_factor_map['CDLCOUNTERATTACK'] = talib.CDLCOUNTERATTACK(_open, _high, _low,
                                                                     _close)  # Pattern Recognition
        tech_factor_map['CDLDOJI'] = talib.CDLDOJI(_open, _high, _low,
                                                   _close)  # Pattern Recognition
        tech_factor_map['CDLDOJISTAR'] = talib.CDLDOJISTAR(_open, _high, _low,
                                                           _close)  # Pattern Recognition
        tech_factor_map['CDLDRAGONFLYDOJI'] = talib.CDLDRAGONFLYDOJI(_open, _high, _low,
                                                                     _close)  # Pattern Recognition
        tech_factor_map['CDLENGULFING'] = talib.CDLENGULFING(_open, _high, _low,
                                                             _close)  # Pattern Recognition
        tech_factor_map['CDLGAPSIDESIDEWHITE'] = talib.CDLGAPSIDESIDEWHITE(_open, _high, _low,
                                                                           _close)  # Pattern Recognition
        tech_factor_map['CDLGRAVESTONEDOJI'] = talib.CDLGRAVESTONEDOJI(_open, _high, _low,
                                                                       _close)  # Pattern Recognition
        tech_factor_map['CDLHAMMER'] = talib.CDLHAMMER(_open, _high, _low,
                                                       _close)  # Pattern Recognition
        tech_factor_map['CDLHANGINGMAN'] = talib.CDLHANGINGMAN(_open, _high, _low,
                                                               _close)  # Pattern Recognition
        tech_factor_map['CDLHARAMI'] = talib.CDLHARAMI(_open, _high, _low,
                                                       _close)  # Pattern Recognition
        tech_factor_map['CDLHARAMICROSS'] = talib.CDLHARAMICROSS(_open, _high, _low,
                                                                 _close)  # Pattern Recognition
        tech_factor_map['CDLHIKKAKE'] = talib.CDLHIKKAKE(_open, _high, _low,
                                                         _close)  # Pattern Recognition
        tech_factor_map['CDLHIKKAKEMOD'] = talib.CDLHIKKAKEMOD(_open, _high, _low,
                                                               _close)  # Pattern Recognition
        tech_factor_map['CDLHOMINGPIGEON'] = talib.CDLHOMINGPIGEON(_open, _high, _low,
                                                                   _close)  # Pattern Recognition
        tech_factor_map['CDLIDENTICAL3CROWS'] = talib.CDLIDENTICAL3CROWS(_open, _high, _low,
                                                                         _close)  # Pattern Recognition
        tech_factor_map['CDLINNECK'] = talib.CDLINNECK(_open, _high, _low,
                                                       _close)  # Pattern Recognition
        tech_factor_map['CDLINVERTEDHAMMER'] = talib.CDLINVERTEDHAMMER(_open, _high, _low,
                                                                       _close)  # Pattern Recognition
        tech_factor_map['CDLKICKING'] = talib.CDLKICKING(_open, _high, _low,
                                                         _close)  # Pattern Recognition
        tech_factor_map['CDLKICKINGBYLENGTH'] = talib.CDLKICKINGBYLENGTH(_open, _high, _low,
                                                                         _close)  # Pattern Recognition
        tech_factor_map['CDLLADDERBOTTOM'] = talib.CDLLADDERBOTTOM(_open, _high, _low,
                                                                   _close)  # Pattern Recognition
        tech_factor_map['CDLLONGLEGGEDDOJI'] = talib.CDLLONGLEGGEDDOJI(_open, _high, _low,
                                                                       _close)  # Pattern Recognition
        tech_factor_map['CDLLONGLINE'] = talib.CDLLONGLINE(_open, _high, _low,
                                                           _close)  # Pattern Recognition
        tech_factor_map['CDLMARUBOZU'] = talib.CDLMARUBOZU(_open, _high, _low,
                                                           _close)  # Pattern Recognition
        tech_factor_map['CDLONNECK'] = talib.CDLONNECK(_open, _high, _low,
                                                       _close)  # Pattern Recognition
        tech_factor_map['CDLPIERCING'] = talib.CDLPIERCING(_open, _high, _low,
                                                           _close)  # Pattern Recognition
        tech_factor_map['CDLRICKSHAWMAN'] = talib.CDLRICKSHAWMAN(_open, _high, _low,
                                                                 _close)  # Pattern Recognition
        tech_factor_map['CDLRISEFALL3METHODS'] = talib.CDLRISEFALL3METHODS(_open, _high, _low,
                                                                           _close)  # Pattern Recognition
        tech_factor_map['CDLSEPARATINGLINES'] = talib.CDLSEPARATINGLINES(_open, _high, _low,
                                                                         _close)  # Pattern Recognition
        tech_factor_map['CDLSHOOTINGSTAR'] = talib.CDLSHOOTINGSTAR(_open, _high, _low,
                                                                   _close)  # Pattern Recognition
        tech_factor_map['CDLSHORTLINE'] = talib.CDLSHORTLINE(_open, _high, _low,
                                                             _close)  # Pattern Recognition
        tech_factor_map['CDLSPINNINGTOP'] = talib.CDLSPINNINGTOP(_open, _high, _low,
                                                                 _close)  # Pattern Recognition
        tech_factor_map['CDLSTALLEDPATTERN'] = talib.CDLSTALLEDPATTERN(_open, _high, _low,
                                                                       _close)  # Pattern Recognition
        tech_factor_map['CDLSTICKSANDWICH'] = talib.CDLSTICKSANDWICH(_open, _high, _low,
                                                                     _close)  # Pattern Recognition
        tech_factor_map['CDLTAKURI'] = talib.CDLTAKURI(_open, _high, _low,
                                                       _close)  # Pattern Recognition
        tech_factor_map['CDLTASUKIGAP'] = talib.CDLTASUKIGAP(_open, _high, _low,
                                                             _close)  # Pattern Recognition
        tech_factor_map['CDLTHRUSTING'] = talib.CDLTHRUSTING(_open, _high, _low,
                                                             _close)  # Pattern Recognition
        tech_factor_map['CDLTRISTAR'] = talib.CDLTRISTAR(_open, _high, _low,
                                                         _close)  # Pattern Recognition
        tech_factor_map['CDLUNIQUE3RIVER'] = talib.CDLUNIQUE3RIVER(_open, _high, _low,
                                                                   _close)  # Pattern Recognition
        tech_factor_map['CDLUPSIDEGAP2CROWS'] = talib.CDLUPSIDEGAP2CROWS(_open, _high, _low,
                                                                         _close)  # Pattern Recognition
        tech_factor_map['CDLXSIDEGAP3METHODS'] = talib.CDLXSIDEGAP3METHODS(_open, _high, _low,
                                                                           _close)  # Pattern Recognition
        tech_factor_map['CDLABANDONEDBABY'] = talib.CDLABANDONEDBABY(_open, _high, _low, _close,
                                                                     penetration=0.3)  # Pattern Recognition
        tech_factor_map['CDLDARKCLOUDCOVER'] = talib.CDLDARKCLOUDCOVER(_open, _high, _low,
                                                                       _close,
                                                                       penetration=0.3)  # Pattern Recognition
        tech_factor_map['CDLEVENINGDOJISTAR'] = talib.CDLEVENINGDOJISTAR(_open, _high, _low,
                                                                         _close,
                                                                         penetration=0.3)  # Pattern Recognition
        tech_factor_map['CDLEVENINGSTAR'] = talib.CDLEVENINGSTAR(_open, _high, _low, _close,
                                                                 penetration=0.3)  # Pattern Recognition
        tech_factor_map['CDLMATHOLD'] = talib.CDLMATHOLD(_open, _high, _low, _close,
                                                         penetration=0.3)  # Pattern Recognition
        tech_factor_map['CDLMORNINGDOJISTAR'] = talib.CDLMORNINGDOJISTAR(_open, _high, _low,
                                                                         _close,
                                                                         penetration=0.3)  # Pattern Recognition
        tech_factor_map['CDLMORNINGSTAR'] = talib.CDLMORNINGSTAR(_open, _high, _low, _close,
                                                                 penetration=0.3)  # Pattern Recognition
        return pd.DataFrame(tech_factor_map)

    def run(self):
        df_tech_factor = self.df_data.groupby('code').apply(self.get_tech_factor)
        return df_tech_factor


def main():
    glog.info(f"Start calculating tech factor.")
    df_data = pd.read_pickle(data_config.stock_config['market_data_file'])

    df_data = data_cleaner.data_replace(df_data, handle_constant=False)

    df_data = df_data.reset_index()
    tfc = TechFactorsCalculator(df_data)
    df_tech_factor = tfc.run()
    df_tech_factor = df_tech_factor.sort_values(by=['date', 'code'], ascending=[True, True])
    df_tech_factor = df_tech_factor.set_index(['date', 'code'])
    df_tech_factor.to_pickle(factor_config.tech_factor_config['tech_factor_path'])

    glog.info(f"End calculating tech factor.")

if __name__ == "__main__":
    main()
