import pandas as pd
import glog
import talib
from config import factor_config, data_config
import sys

sys.path.append('../..')


class TechFactors:
    def __init__(self, df_data):
        self.df_data = df_data
        self.close = df_data['close']
        self.open = df_data['open']
        self.high = df_data['high']
        self.low = df_data['low']
        self.volume = df_data['volume']
        self.money = df_data['money']
        self.code = df_data['code']
        self.date = df_data['date']
        self.df_tech_factor = pd.DataFrame()

    def get_tech_factor(self):
        self.df_tech_factor['date'] = self.date
        self.df_tech_factor['code'] = self.code

        glog.info('Get cycle indicators.')
        self.df_tech_factor['HT_DCPERIOD'] = talib.HT_DCPERIOD(self.close)  # Cycle Indicators
        self.df_tech_factor['HT_DCPHASE'] = talib.HT_DCPHASE(self.close)  # Cycle Indicators
        self.df_tech_factor['HT_PHASOR_inphase'], self.df_tech_factor['HT_PHASOR_quadrature'] = talib.HT_PHASOR(
            self.close)  # Cycle Indicators
        self.df_tech_factor['HT_SINE_sine'], self.df_tech_factor['HT_SINE_leadsine'] = talib.HT_SINE(
            self.close)  # Cycle Indicators
        self.df_tech_factor['HT_TRENDMODE'] = talib.HT_TRENDMODE(self.close)  # Cycle Indicators

        glog.info('Get overlap studies indicators.')
        self.df_tech_factor['HT_TRENDLINE'] = talib.HT_TRENDLINE(self.close)  # Overlap Studies
        self.df_tech_factor['MAMA_mama'], self.df_tech_factor['MAMA_fama'] = talib.MAMA(self.close, fastlimit=0.5,
                                                                                        slowlimit=0.05)
        self.df_tech_factor['SAR'] = talib.SAR(self.high, self.low, acceleration=0.02, maximum=0.2)  # Overlap Studies
        self.df_tech_factor['SAREXT'] = talib.SAREXT(self.high, self.low, startvalue=0, offsetonreverse=0,
                                                     accelerationinitlong=0.02,
                                                     accelerationlong=0.2, accelerationmaxlong=0.02,
                                                     accelerationinitshort=0.02, accelerationshort=0.02,
                                                     accelerationmaxshort=0.2)  # Overlap Studies
        self.df_tech_factor['BBANDS_upperband'], self.df_tech_factor['BBANDS_middleband'], self.df_tech_factor[
            'BBANDS_lowerband'] = talib.BBANDS(self.close, timeperiod=5, nbdevup=2, nbdevdn=2,
                                               matype=0)  # Overlap Studies
        self.df_tech_factor['DEMA'] = talib.DEMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_tech_factor['EMA'] = talib.EMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_tech_factor['KAMA'] = talib.KAMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_tech_factor['MA'] = talib.MA(self.close, timeperiod=30, matype=0)  # Overlap Studies
        self.df_tech_factor['SMA'] = talib.SMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_tech_factor['TEMA'] = talib.TEMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_tech_factor['TRIMA'] = talib.TRIMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_tech_factor['WMA'] = talib.WMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_tech_factor['MIDPOINT'] = talib.MIDPOINT(self.close, timeperiod=14)  # Overlap Studies
        self.df_tech_factor['MIDPRICE'] = talib.MIDPRICE(self.high, self.low, timeperiod=14)  # Overlap Studies
        self.df_tech_factor['T3'] = talib.T3(self.close, timeperiod=5, vfactor=0.7)  # Overlap Studies

        glog.info('Get volatility indicators.')
        self.df_tech_factor['TRANGE'] = talib.TRANGE(self.high, self.low, self.close)  # Volatility Indicators
        self.df_tech_factor['ATR'] = talib.ATR(self.high, self.low, self.close, timeperiod=14)  # Volatility Indicators
        self.df_tech_factor['NATR'] = talib.NATR(self.high, self.low, self.close,
                                                 timeperiod=14)  # Volatility Indicators

        self.df_tech_factor['AD'] = talib.AD(self.high, self.low, self.close, self.volume)  # Volume Indicators
        self.df_tech_factor['OBV'] = talib.OBV(self.close, self.volume)  # Volume Indicators
        self.df_tech_factor['ADOSC'] = talib.ADOSC(self.high, self.low, self.close, self.volume, fastperiod=3,
                                                   slowperiod=10)  # Volume Indicators

        glog.info('Get momentum indicators.')
        self.df_tech_factor['BOP'] = talib.BOP(self.open, self.high, self.low, self.close)  # Momentum Indicators
        self.df_tech_factor['ADX'] = talib.ADX(self.high, self.low, self.close, timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['ADXR'] = talib.ADXR(self.high, self.low, self.close, timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['AROON_aroondown'], self.df_tech_factor['AROON_aroondown'] = talib.AROON(self.high,
                                                                                                     self.low,
                                                                                                     timeperiod=14)
        self.df_tech_factor['AROONOSC'] = talib.AROONOSC(self.high, self.low, timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['CCI'] = talib.CCI(self.high, self.low, self.close, timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['CMO'] = talib.CMO(self.close, timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['DX'] = talib.DX(self.high, self.low, self.close, timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['MFI'] = talib.MFI(self.high, self.low, self.close, self.volume,
                                               timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['MINUS_DI'] = talib.MINUS_DI(self.high, self.low, self.close,
                                                         timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['MINUS_DM'] = talib.MINUS_DM(self.high, self.low, timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['PLUS_DI'] = talib.PLUS_DI(self.high, self.low, self.close,
                                                       timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['PLUS_DM'] = talib.PLUS_DM(self.high, self.low, timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['RSI'] = talib.RSI(self.close, timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['WILLR'] = talib.WILLR(self.high, self.low, self.close,
                                                   timeperiod=14)  # Momentum Indicators
        self.df_tech_factor['APO'] = talib.APO(self.close, fastperiod=12, slowperiod=26,
                                               matype=0)  # Momentum Indicators
        self.df_tech_factor['PPO'] = talib.PPO(self.close, fastperiod=12, slowperiod=26,
                                               matype=0)  # Momentum Indicators
        self.df_tech_factor['MACD_macd'], self.df_tech_factor[
            'MACD_macdsignal'], self.df_tech_factor[
            'MACD_macdhist'] = talib.MACD(self.close, fastperiod=12, slowperiod=26,
                                          signalperiod=9)  # Momentum Indicators
        self.df_tech_factor['MACDEXT_macd'], self.df_tech_factor[
            'MACDEXT_macdsignal'], self.df_tech_factor[
            'MACDEXT_macdhist'] = talib.MACDEXT(self.close, fastperiod=12,
                                                fastmatype=0, slowperiod=26,
                                                slowmatype=0,
                                                signalperiod=9,
                                                signalmatype=0)  # Momentum Indicators
        self.df_tech_factor['MACDFIX_macd'], self.df_tech_factor['MACDFIX_macdsignal'], self.df_tech_factor[
            'MACDFIX_macdhist'] = talib.MACDFIX(self.close, signalperiod=9)  # Momentum Indicators
        self.df_tech_factor['MOM'] = talib.MOM(self.close, timeperiod=10)  # Momentum Indicators
        self.df_tech_factor['ROC'] = talib.ROC(self.close, timeperiod=10)  # Momentum Indicators
        self.df_tech_factor['ROCP'] = talib.ROCP(self.close, timeperiod=10)  # Momentum Indicators
        self.df_tech_factor['ROCR'] = talib.ROCR(self.close, timeperiod=10)  # Momentum Indicators
        self.df_tech_factor['ROCR100'] = talib.ROCR100(self.close, timeperiod=10)  # Momentum Indicators
        self.df_tech_factor['TRIX'] = talib.TRIX(self.close, timeperiod=30)  # Momentum Indicators
        self.df_tech_factor['STOCH_slowk'], self.df_tech_factor[
            'STOCH_slowd'] = talib.STOCH(self.high, self.low, self.close,
                                         fastk_period=5, slowk_period=3,
                                         slowk_matype=0, slowd_period=3,
                                         slowd_matype=0)  # Momentum Indicators
        self.df_tech_factor['STOCHF_fastk'], self.df_tech_factor[
            'STOCHF_fastd'] = talib.STOCHF(self.high, self.low, self.close,
                                           fastk_period=5, fastd_period=3,
                                           fastd_matype=0)  # Momentum Indicators
        self.df_tech_factor['STOCHRSI_fastk'], self.df_tech_factor[
            'STOCHRSI_fastd'] = talib.STOCHRSI(self.close, timeperiod=14,
                                               fastk_period=5,
                                               fastd_period=3,
                                               fastd_matype=0)  # Momentum Indicators
        self.df_tech_factor['ULTOSC'] = talib.ULTOSC(self.high, self.low, self.close,
                                                     timeperiod1=7, timeperiod2=14,
                                                     timeperiod3=28)  # Momentum Indicators

        glog.info('Get pattern recognition indicators.')
        self.df_tech_factor['CDL2CROWS'] = talib.CDL2CROWS(self.open, self.high, self.low,
                                                           self.close)  # Pattern Recognition
        self.df_tech_factor['CDL3BLACKCROWS'] = talib.CDL3BLACKCROWS(self.open, self.high, self.low,
                                                                     self.close)  # Pattern Recognition
        self.df_tech_factor['CDL3INSIDE'] = talib.CDL3INSIDE(self.open, self.high, self.low,
                                                             self.close)  # Pattern Recognition
        self.df_tech_factor['CDL3LINESTRIKE'] = talib.CDL3LINESTRIKE(self.open, self.high, self.low,
                                                                     self.close)  # Pattern Recognition
        self.df_tech_factor['CDL3OUTSIDE'] = talib.CDL3OUTSIDE(self.open, self.high, self.low,
                                                               self.close)  # Pattern Recognition
        self.df_tech_factor['CDL3STARSINSOUTH'] = talib.CDL3STARSINSOUTH(self.open, self.high, self.low,
                                                                         self.close)  # Pattern Recognition
        self.df_tech_factor['CDL3WHITESOLDIERS'] = talib.CDL3WHITESOLDIERS(self.open, self.high, self.low,
                                                                           self.close)  # Pattern Recognition
        self.df_tech_factor['CDLADVANCEBLOCK'] = talib.CDLADVANCEBLOCK(self.open, self.high, self.low,
                                                                       self.close)  # Pattern Recognition
        self.df_tech_factor['CDLBELTHOLD'] = talib.CDLBELTHOLD(self.open, self.high, self.low,
                                                               self.close)  # Pattern Recognition
        self.df_tech_factor['CDLBREAKAWAY'] = talib.CDLBREAKAWAY(self.open, self.high, self.low,
                                                                 self.close)  # Pattern Recognition
        self.df_tech_factor['CDLCLOSINGMARUBOZU'] = talib.CDLCLOSINGMARUBOZU(self.open, self.high, self.low,
                                                                             self.close)  # Pattern Recognition
        self.df_tech_factor['CDLCONCEALBABYSWALL'] = talib.CDLCONCEALBABYSWALL(self.open, self.high, self.low,
                                                                               self.close)  # Pattern Recognition
        self.df_tech_factor['CDLCOUNTERATTACK'] = talib.CDLCOUNTERATTACK(self.open, self.high, self.low,
                                                                         self.close)  # Pattern Recognition
        self.df_tech_factor['CDLDOJI'] = talib.CDLDOJI(self.open, self.high, self.low,
                                                       self.close)  # Pattern Recognition
        self.df_tech_factor['CDLDOJISTAR'] = talib.CDLDOJISTAR(self.open, self.high, self.low,
                                                               self.close)  # Pattern Recognition
        self.df_tech_factor['CDLDRAGONFLYDOJI'] = talib.CDLDRAGONFLYDOJI(self.open, self.high, self.low,
                                                                         self.close)  # Pattern Recognition
        self.df_tech_factor['CDLENGULFING'] = talib.CDLENGULFING(self.open, self.high, self.low,
                                                                 self.close)  # Pattern Recognition
        self.df_tech_factor['CDLGAPSIDESIDEWHITE'] = talib.CDLGAPSIDESIDEWHITE(self.open, self.high, self.low,
                                                                               self.close)  # Pattern Recognition
        self.df_tech_factor['CDLGRAVESTONEDOJI'] = talib.CDLGRAVESTONEDOJI(self.open, self.high, self.low,
                                                                           self.close)  # Pattern Recognition
        self.df_tech_factor['CDLHAMMER'] = talib.CDLHAMMER(self.open, self.high, self.low,
                                                           self.close)  # Pattern Recognition
        self.df_tech_factor['CDLHANGINGMAN'] = talib.CDLHANGINGMAN(self.open, self.high, self.low,
                                                                   self.close)  # Pattern Recognition
        self.df_tech_factor['CDLHARAMI'] = talib.CDLHARAMI(self.open, self.high, self.low,
                                                           self.close)  # Pattern Recognition
        self.df_tech_factor['CDLHARAMICROSS'] = talib.CDLHARAMICROSS(self.open, self.high, self.low,
                                                                     self.close)  # Pattern Recognition
        self.df_tech_factor['CDLHIGHWAVE'] = talib.CDLHIGHWAVE(self.open, self.high, self.low,
                                                               self.close)  # Pattern Recognition
        self.df_tech_factor['CDLHIKKAKE'] = talib.CDLHIKKAKE(self.open, self.high, self.low,
                                                             self.close)  # Pattern Recognition
        self.df_tech_factor['CDLHIKKAKEMOD'] = talib.CDLHIKKAKEMOD(self.open, self.high, self.low,
                                                                   self.close)  # Pattern Recognition
        self.df_tech_factor['CDLHOMINGPIGEON'] = talib.CDLHOMINGPIGEON(self.open, self.high, self.low,
                                                                       self.close)  # Pattern Recognition
        self.df_tech_factor['CDLIDENTICAL3CROWS'] = talib.CDLIDENTICAL3CROWS(self.open, self.high, self.low,
                                                                             self.close)  # Pattern Recognition
        self.df_tech_factor['CDLINNECK'] = talib.CDLINNECK(self.open, self.high, self.low,
                                                           self.close)  # Pattern Recognition
        self.df_tech_factor['CDLINVERTEDHAMMER'] = talib.CDLINVERTEDHAMMER(self.open, self.high, self.low,
                                                                           self.close)  # Pattern Recognition
        self.df_tech_factor['CDLKICKING'] = talib.CDLKICKING(self.open, self.high, self.low,
                                                             self.close)  # Pattern Recognition
        self.df_tech_factor['CDLKICKINGBYLENGTH'] = talib.CDLKICKINGBYLENGTH(self.open, self.high, self.low,
                                                                             self.close)  # Pattern Recognition
        self.df_tech_factor['CDLLADDERBOTTOM'] = talib.CDLLADDERBOTTOM(self.open, self.high, self.low,
                                                                       self.close)  # Pattern Recognition
        self.df_tech_factor['CDLLONGLEGGEDDOJI'] = talib.CDLLONGLEGGEDDOJI(self.open, self.high, self.low,
                                                                           self.close)  # Pattern Recognition
        self.df_tech_factor['CDLLONGLINE'] = talib.CDLLONGLINE(self.open, self.high, self.low,
                                                               self.close)  # Pattern Recognition
        self.df_tech_factor['CDLMARUBOZU'] = talib.CDLMARUBOZU(self.open, self.high, self.low,
                                                               self.close)  # Pattern Recognition
        self.df_tech_factor['CDLMATCHINGLOW'] = talib.CDLMATCHINGLOW(self.open, self.high, self.low,
                                                                     self.close)  # Pattern Recognition
        self.df_tech_factor['CDLONNECK'] = talib.CDLONNECK(self.open, self.high, self.low,
                                                           self.close)  # Pattern Recognition
        self.df_tech_factor['CDLPIERCING'] = talib.CDLPIERCING(self.open, self.high, self.low,
                                                               self.close)  # Pattern Recognition
        self.df_tech_factor['CDLRICKSHAWMAN'] = talib.CDLRICKSHAWMAN(self.open, self.high, self.low,
                                                                     self.close)  # Pattern Recognition
        self.df_tech_factor['CDLRISEFALL3METHODS'] = talib.CDLRISEFALL3METHODS(self.open, self.high, self.low,
                                                                               self.close)  # Pattern Recognition
        self.df_tech_factor['CDLSEPARATINGLINES'] = talib.CDLSEPARATINGLINES(self.open, self.high, self.low,
                                                                             self.close)  # Pattern Recognition
        self.df_tech_factor['CDLSHOOTINGSTAR'] = talib.CDLSHOOTINGSTAR(self.open, self.high, self.low,
                                                                       self.close)  # Pattern Recognition
        self.df_tech_factor['CDLSHORTLINE'] = talib.CDLSHORTLINE(self.open, self.high, self.low,
                                                                 self.close)  # Pattern Recognition
        self.df_tech_factor['CDLSPINNINGTOP'] = talib.CDLSPINNINGTOP(self.open, self.high, self.low,
                                                                     self.close)  # Pattern Recognition
        self.df_tech_factor['CDLSTALLEDPATTERN'] = talib.CDLSTALLEDPATTERN(self.open, self.high, self.low,
                                                                           self.close)  # Pattern Recognition
        self.df_tech_factor['CDLSTICKSANDWICH'] = talib.CDLSTICKSANDWICH(self.open, self.high, self.low,
                                                                         self.close)  # Pattern Recognition
        self.df_tech_factor['CDLTAKURI'] = talib.CDLTAKURI(self.open, self.high, self.low,
                                                           self.close)  # Pattern Recognition
        self.df_tech_factor['CDLTASUKIGAP'] = talib.CDLTASUKIGAP(self.open, self.high, self.low,
                                                                 self.close)  # Pattern Recognition
        self.df_tech_factor['CDLTHRUSTING'] = talib.CDLTHRUSTING(self.open, self.high, self.low,
                                                                 self.close)  # Pattern Recognition
        self.df_tech_factor['CDLTRISTAR'] = talib.CDLTRISTAR(self.open, self.high, self.low,
                                                             self.close)  # Pattern Recognition
        self.df_tech_factor['CDLUNIQUE3RIVER'] = talib.CDLUNIQUE3RIVER(self.open, self.high, self.low,
                                                                       self.close)  # Pattern Recognition
        self.df_tech_factor['CDLUPSIDEGAP2CROWS'] = talib.CDLUPSIDEGAP2CROWS(self.open, self.high, self.low,
                                                                             self.close)  # Pattern Recognition
        self.df_tech_factor['CDLXSIDEGAP3METHODS'] = talib.CDLXSIDEGAP3METHODS(self.open, self.high, self.low,
                                                                               self.close)  # Pattern Recognition
        self.df_tech_factor['CDLABANDONEDBABY'] = talib.CDLABANDONEDBABY(self.open, self.high, self.low, self.close,
                                                                         penetration=0.3)  # Pattern Recognition
        self.df_tech_factor['CDLDARKCLOUDCOVER'] = talib.CDLDARKCLOUDCOVER(self.open, self.high, self.low,
                                                                           self.close,
                                                                           penetration=0.3)  # Pattern Recognition
        self.df_tech_factor['CDLEVENINGDOJISTAR'] = talib.CDLEVENINGDOJISTAR(self.open, self.high, self.low,
                                                                             self.close,
                                                                             penetration=0.3)  # Pattern Recognition
        self.df_tech_factor['CDLEVENINGSTAR'] = talib.CDLEVENINGSTAR(self.open, self.high, self.low, self.close,
                                                                     penetration=0.3)  # Pattern Recognition
        self.df_tech_factor['CDLMATHOLD'] = talib.CDLMATHOLD(self.open, self.high, self.low, self.close,
                                                             penetration=0.3)  # Pattern Recognition
        self.df_tech_factor['CDLMORNINGDOJISTAR'] = talib.CDLMORNINGDOJISTAR(self.open, self.high, self.low,
                                                                             self.close,
                                                                             penetration=0.3)  # Pattern Recognition
        self.df_tech_factor['CDLMORNINGSTAR'] = talib.CDLMORNINGSTAR(self.open, self.high, self.low, self.close,
                                                                     penetration=0.3)  # Pattern Recognition
        return self.df_tech_factor

    def run(self):
        self.df_tech_factor = self.df_data.groupby(self.code).apply(self.get_tech_factor)


def main():
    df_data = pd.read_pickle(data_config.stock_config["market_data_file"])
    df_data = df_data.reset_index()
    tf = TechFactors(df_data)
    tf.run()
    df_tech_factor = tf.df_tech_factor.sort_values(by=['date', 'code'], ascending=[True, True])
    df_tech_factor = df_tech_factor.set_index(['date', 'code'])
    df_tech_factor.to_pickle(factor_config.tech_factor_config['tech_factor_path'])
    glog.info('End.')


if __name__ == "__main__":
    glog.info('Start program execution.')
    main()
