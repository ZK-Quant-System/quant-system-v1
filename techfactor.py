import pickle
import pandas as pd
import glog
import talib
import tech_config


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

    def get_techfactor(self):
        self.df_techfactor = pd.DataFrame()
        self.df_techfactor['date'] = self.date
        self.df_techfactor['code'] = self.code

        glog.info('Get cycle indicators.')
        self.df_techfactor['HT_DCPERIOD'] = talib.HT_DCPERIOD(self.close)  # Cycle Indicators
        self.df_techfactor['HT_DCPHASE'] = talib.HT_DCPHASE(self.close)  # Cycle Indicators
        self.df_techfactor['HT_PHASOR_inphase'], self.df_techfactor['HT_PHASOR_quadrature'] = talib.HT_PHASOR(
            self.close)  # Cycle Indicators
        self.df_techfactor['HT_SINE_sine'], self.df_techfactor['HT_SINE_leadsine'] = talib.HT_SINE(
            self.close)  # Cycle Indicators
        self.df_techfactor['HT_TRENDMODE'] = talib.HT_TRENDMODE(self.close)  # Cycle Indicators

        glog.info('Get overlap studies indicators.')
        self.df_techfactor['HT_TRENDLINE'] = talib.HT_TRENDLINE(self.close)  # Overlap Studies
        self.df_techfactor['MAMA_mama'], self.df_techfactor['MAMA_fama'] = talib.MAMA(self.close, fastlimit=0.5,
                                                                                      slowlimit=0.05)  # Overlap Studies
        self.df_techfactor['SAR'] = talib.SAR(self.high, self.low, acceleration=0.02, maximum=0.2)  # Overlap Studies
        self.df_techfactor['SAREXT'] = talib.SAREXT(self.high, self.low, startvalue=0, offsetonreverse=0,
                                                    accelerationinitlong=0.02,
                                                    accelerationlong=0.2, accelerationmaxlong=0.02,
                                                    accelerationinitshort=0.02, accelerationshort=0.02,
                                                    accelerationmaxshort=0.2)  # Overlap Studies
        self.df_techfactor['BBANDS_upperband'], self.df_techfactor['BBANDS_middleband'], self.df_techfactor[
            'BBANDS_lowerband'] = talib.BBANDS(self.close, timeperiod=5, nbdevup=2, nbdevdn=2,
                                               matype=0)  # Overlap Studies
        self.df_techfactor['DEMA'] = talib.DEMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_techfactor['EMA'] = talib.EMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_techfactor['KAMA'] = talib.KAMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_techfactor['MA'] = talib.MA(self.close, timeperiod=30, matype=0)  # Overlap Studies
        self.df_techfactor['SMA'] = talib.SMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_techfactor['TEMA'] = talib.TEMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_techfactor['TRIMA'] = talib.TRIMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_techfactor['WMA'] = talib.WMA(self.close, timeperiod=30)  # Overlap Studies
        self.df_techfactor['MIDPOINT'] = talib.MIDPOINT(self.close, timeperiod=14)  # Overlap Studies
        self.df_techfactor['MIDPRICE'] = talib.MIDPRICE(self.high, self.low, timeperiod=14)  # Overlap Studies
        self.df_techfactor['T3'] = talib.T3(self.close, timeperiod=5, vfactor=0.7)  # Overlap Studies

        glog.info('Get volatility indicators.')
        self.df_techfactor['TRANGE'] = talib.TRANGE(self.high, self.low, self.close)  # Volatility Indicators
        self.df_techfactor['ATR'] = talib.ATR(self.high, self.low, self.close, timeperiod=14)  # Volatility Indicators
        self.df_techfactor['NATR'] = talib.NATR(self.high, self.low, self.close, timeperiod=14)  # Volatility Indicators

        self.df_techfactor['AD'] = talib.AD(self.high, self.low, self.close, self.volume)  # Volume Indicators
        self.df_techfactor['OBV'] = talib.OBV(self.close, self.volume)  # Volume Indicators
        self.df_techfactor['ADOSC'] = talib.ADOSC(self.high, self.low, self.close, self.volume, fastperiod=3,
                                                  slowperiod=10)  # Volume Indicators

        glog.info('Get momentum indicators.')
        self.df_techfactor['BOP'] = talib.BOP(self.open, self.high, self.low, self.close)  # Momentum Indicators
        self.df_techfactor['ADX'] = talib.ADX(self.high, self.low, self.close, timeperiod=14)  # Momentum Indicators
        self.df_techfactor['ADXR'] = talib.ADXR(self.high, self.low, self.close, timeperiod=14)  # Momentum Indicators
        self.df_techfactor['AROON_aroondown'], self.df_techfactor['AROON_aroondown'] = talib.AROON(self.high, self.low,
                                                                                                   timeperiod=14)  # Momentum Indicators
        self.df_techfactor['AROONOSC'] = talib.AROONOSC(self.high, self.low, timeperiod=14)  # Momentum Indicators
        self.df_techfactor['CCI'] = talib.CCI(self.high, self.low, self.close, timeperiod=14)  # Momentum Indicators
        self.df_techfactor['CMO'] = talib.CMO(self.close, timeperiod=14)  # Momentum Indicators
        self.df_techfactor['DX'] = talib.DX(self.high, self.low, self.close, timeperiod=14)  # Momentum Indicators
        self.df_techfactor['MFI'] = talib.MFI(self.high, self.low, self.close, self.volume,
                                              timeperiod=14)  # Momentum Indicators
        self.df_techfactor['MINUS_DI'] = talib.MINUS_DI(self.high, self.low, self.close,
                                                        timeperiod=14)  # Momentum Indicators
        self.df_techfactor['MINUS_DM'] = talib.MINUS_DM(self.high, self.low, timeperiod=14)  # Momentum Indicators
        self.df_techfactor['PLUS_DI'] = talib.PLUS_DI(self.high, self.low, self.close,
                                                      timeperiod=14)  # Momentum Indicators
        self.df_techfactor['PLUS_DM'] = talib.PLUS_DM(self.high, self.low, timeperiod=14)  # Momentum Indicators
        self.df_techfactor['RSI'] = talib.RSI(self.close, timeperiod=14)  # Momentum Indicators
        self.df_techfactor['WILLR'] = talib.WILLR(self.high, self.low, self.close, timeperiod=14)  # Momentum Indicators
        self.df_techfactor['APO'] = talib.APO(self.close, fastperiod=12, slowperiod=26,
                                              matype=0)  # Momentum Indicators
        self.df_techfactor['PPO'] = talib.PPO(self.close, fastperiod=12, slowperiod=26,
                                              matype=0)  # Momentum Indicators
        self.df_techfactor['MACD_macd'], self.df_techfactor[
            'MACD_macdsignal'], self.df_techfactor[
            'MACD_macdhist'] = talib.MACD(self.close, fastperiod=12, slowperiod=26,
                                          signalperiod=9)  # Momentum Indicators
        self.df_techfactor['MACDEXT_macd'], self.df_techfactor[
            'MACDEXT_macdsignal'], self.df_techfactor[
            'MACDEXT_macdhist'] = talib.MACDEXT(self.close, fastperiod=12,
                                                fastmatype=0, slowperiod=26,
                                                slowmatype=0,
                                                signalperiod=9,
                                                signalmatype=0)  # Momentum Indicators
        self.df_techfactor['MACDFIX_macd'], self.df_techfactor['MACDFIX_macdsignal'], self.df_techfactor[
            'MACDFIX_macdhist'] = talib.MACDFIX(self.close, signalperiod=9)  # Momentum Indicators
        self.df_techfactor['MOM'] = talib.MOM(self.close, timeperiod=10)  # Momentum Indicators
        self.df_techfactor['ROC'] = talib.ROC(self.close, timeperiod=10)  # Momentum Indicators
        self.df_techfactor['ROCP'] = talib.ROCP(self.close, timeperiod=10)  # Momentum Indicators
        self.df_techfactor['ROCR'] = talib.ROCR(self.close, timeperiod=10)  # Momentum Indicators
        self.df_techfactor['ROCR100'] = talib.ROCR100(self.close, timeperiod=10)  # Momentum Indicators
        self.df_techfactor['TRIX'] = talib.TRIX(self.close, timeperiod=30)  # Momentum Indicators
        self.df_techfactor['STOCH_slowk'], self.df_techfactor[
            'STOCH_slowd'] = talib.STOCH(self.high, self.low, self.close,
                                         fastk_period=5, slowk_period=3,
                                         slowk_matype=0, slowd_period=3,
                                         slowd_matype=0)  # Momentum Indicators
        self.df_techfactor['STOCHF_fastk'], self.df_techfactor[
            'STOCHF_fastd'] = talib.STOCHF(self.high, self.low, self.close,
                                           fastk_period=5, fastd_period=3,
                                           fastd_matype=0)  # Momentum Indicators
        self.df_techfactor['STOCHRSI_fastk'], self.df_techfactor[
            'STOCHRSI_fastd'] = talib.STOCHRSI(self.close, timeperiod=14,
                                               fastk_period=5,
                                               fastd_period=3,
                                               fastd_matype=0)  # Momentum Indicators
        self.df_techfactor['ULTOSC'] = talib.ULTOSC(self.high, self.low, self.close,
                                                    timeperiod1=7, timeperiod2=14,
                                                    timeperiod3=28)  # Momentum Indicators

        glog.info('Get pattern recognition indicators.')
        self.df_techfactor['CDL2CROWS'] = talib.CDL2CROWS(self.open, self.high, self.low,
                                                          self.close)  # Pattern Recognition
        self.df_techfactor['CDL3BLACKCROWS'] = talib.CDL3BLACKCROWS(self.open, self.high, self.low,
                                                                    self.close)  # Pattern Recognition
        self.df_techfactor['CDL3INSIDE'] = talib.CDL3INSIDE(self.open, self.high, self.low,
                                                            self.close)  # Pattern Recognition
        self.df_techfactor['CDL3LINESTRIKE'] = talib.CDL3LINESTRIKE(self.open, self.high, self.low,
                                                                    self.close)  # Pattern Recognition
        self.df_techfactor['CDL3OUTSIDE'] = talib.CDL3OUTSIDE(self.open, self.high, self.low,
                                                              self.close)  # Pattern Recognition
        self.df_techfactor['CDL3STARSINSOUTH'] = talib.CDL3STARSINSOUTH(self.open, self.high, self.low,
                                                                        self.close)  # Pattern Recognition
        self.df_techfactor['CDL3WHITESOLDIERS'] = talib.CDL3WHITESOLDIERS(self.open, self.high, self.low,
                                                                          self.close)  # Pattern Recognition
        self.df_techfactor['CDLADVANCEBLOCK'] = talib.CDLADVANCEBLOCK(self.open, self.high, self.low,
                                                                      self.close)  # Pattern Recognition
        self.df_techfactor['CDLBELTHOLD'] = talib.CDLBELTHOLD(self.open, self.high, self.low,
                                                              self.close)  # Pattern Recognition
        self.df_techfactor['CDLBREAKAWAY'] = talib.CDLBREAKAWAY(self.open, self.high, self.low,
                                                                self.close)  # Pattern Recognition
        self.df_techfactor['CDLCLOSINGMARUBOZU'] = talib.CDLCLOSINGMARUBOZU(self.open, self.high, self.low,
                                                                            self.close)  # Pattern Recognition
        self.df_techfactor['CDLCONCEALBABYSWALL'] = talib.CDLCONCEALBABYSWALL(self.open, self.high, self.low,
                                                                              self.close)  # Pattern Recognition
        self.df_techfactor['CDLCOUNTERATTACK'] = talib.CDLCOUNTERATTACK(self.open, self.high, self.low,
                                                                        self.close)  # Pattern Recognition
        self.df_techfactor['CDLDOJI'] = talib.CDLDOJI(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.df_techfactor['CDLDOJISTAR'] = talib.CDLDOJISTAR(self.open, self.high, self.low,
                                                              self.close)  # Pattern Recognition
        self.df_techfactor['CDLDRAGONFLYDOJI'] = talib.CDLDRAGONFLYDOJI(self.open, self.high, self.low,
                                                                        self.close)  # Pattern Recognition
        self.df_techfactor['CDLENGULFING'] = talib.CDLENGULFING(self.open, self.high, self.low,
                                                                self.close)  # Pattern Recognition
        self.df_techfactor['CDLGAPSIDESIDEWHITE'] = talib.CDLGAPSIDESIDEWHITE(self.open, self.high, self.low,
                                                                              self.close)  # Pattern Recognition
        self.df_techfactor['CDLGRAVESTONEDOJI'] = talib.CDLGRAVESTONEDOJI(self.open, self.high, self.low,
                                                                          self.close)  # Pattern Recognition
        self.df_techfactor['CDLHAMMER'] = talib.CDLHAMMER(self.open, self.high, self.low,
                                                          self.close)  # Pattern Recognition
        self.df_techfactor['CDLHANGINGMAN'] = talib.CDLHANGINGMAN(self.open, self.high, self.low,
                                                                  self.close)  # Pattern Recognition
        self.df_techfactor['CDLHARAMI'] = talib.CDLHARAMI(self.open, self.high, self.low,
                                                          self.close)  # Pattern Recognition
        self.df_techfactor['CDLHARAMICROSS'] = talib.CDLHARAMICROSS(self.open, self.high, self.low,
                                                                    self.close)  # Pattern Recognition
        self.df_techfactor['CDLHIGHWAVE'] = talib.CDLHIGHWAVE(self.open, self.high, self.low,
                                                              self.close)  # Pattern Recognition
        self.df_techfactor['CDLHIKKAKE'] = talib.CDLHIKKAKE(self.open, self.high, self.low,
                                                            self.close)  # Pattern Recognition
        self.df_techfactor['CDLHIKKAKEMOD'] = talib.CDLHIKKAKEMOD(self.open, self.high, self.low,
                                                                  self.close)  # Pattern Recognition
        self.df_techfactor['CDLHOMINGPIGEON'] = talib.CDLHOMINGPIGEON(self.open, self.high, self.low,
                                                                      self.close)  # Pattern Recognition
        self.df_techfactor['CDLIDENTICAL3CROWS'] = talib.CDLIDENTICAL3CROWS(self.open, self.high, self.low,
                                                                            self.close)  # Pattern Recognition
        self.df_techfactor['CDLINNECK'] = talib.CDLINNECK(self.open, self.high, self.low,
                                                          self.close)  # Pattern Recognition
        self.df_techfactor['CDLINVERTEDHAMMER'] = talib.CDLINVERTEDHAMMER(self.open, self.high, self.low,
                                                                          self.close)  # Pattern Recognition
        self.df_techfactor['CDLKICKING'] = talib.CDLKICKING(self.open, self.high, self.low,
                                                            self.close)  # Pattern Recognition
        self.df_techfactor['CDLKICKINGBYLENGTH'] = talib.CDLKICKINGBYLENGTH(self.open, self.high, self.low,
                                                                            self.close)  # Pattern Recognition
        self.df_techfactor['CDLLADDERBOTTOM'] = talib.CDLLADDERBOTTOM(self.open, self.high, self.low,
                                                                      self.close)  # Pattern Recognition
        self.df_techfactor['CDLLONGLEGGEDDOJI'] = talib.CDLLONGLEGGEDDOJI(self.open, self.high, self.low,
                                                                          self.close)  # Pattern Recognition
        self.df_techfactor['CDLLONGLINE'] = talib.CDLLONGLINE(self.open, self.high, self.low,
                                                              self.close)  # Pattern Recognition
        self.df_techfactor['CDLMARUBOZU'] = talib.CDLMARUBOZU(self.open, self.high, self.low,
                                                              self.close)  # Pattern Recognition
        self.df_techfactor['CDLMATCHINGLOW'] = talib.CDLMATCHINGLOW(self.open, self.high, self.low,
                                                                    self.close)  # Pattern Recognition
        self.df_techfactor['CDLONNECK'] = talib.CDLONNECK(self.open, self.high, self.low,
                                                          self.close)  # Pattern Recognition
        self.df_techfactor['CDLPIERCING'] = talib.CDLPIERCING(self.open, self.high, self.low,
                                                              self.close)  # Pattern Recognition
        self.df_techfactor['CDLRICKSHAWMAN'] = talib.CDLRICKSHAWMAN(self.open, self.high, self.low,
                                                                    self.close)  # Pattern Recognition
        self.df_techfactor['CDLRISEFALL3METHODS'] = talib.CDLRISEFALL3METHODS(self.open, self.high, self.low,
                                                                              self.close)  # Pattern Recognition
        self.df_techfactor['CDLSEPARATINGLINES'] = talib.CDLSEPARATINGLINES(self.open, self.high, self.low,
                                                                            self.close)  # Pattern Recognition
        self.df_techfactor['CDLSHOOTINGSTAR'] = talib.CDLSHOOTINGSTAR(self.open, self.high, self.low,
                                                                      self.close)  # Pattern Recognition
        self.df_techfactor['CDLSHORTLINE'] = talib.CDLSHORTLINE(self.open, self.high, self.low,
                                                                self.close)  # Pattern Recognition
        self.df_techfactor['CDLSPINNINGTOP'] = talib.CDLSPINNINGTOP(self.open, self.high, self.low,
                                                                    self.close)  # Pattern Recognition
        self.df_techfactor['CDLSTALLEDPATTERN'] = talib.CDLSTALLEDPATTERN(self.open, self.high, self.low,
                                                                          self.close)  # Pattern Recognition
        self.df_techfactor['CDLSTICKSANDWICH'] = talib.CDLSTICKSANDWICH(self.open, self.high, self.low,
                                                                        self.close)  # Pattern Recognition
        self.df_techfactor['CDLTAKURI'] = talib.CDLTAKURI(self.open, self.high, self.low,
                                                          self.close)  # Pattern Recognition
        self.df_techfactor['CDLTASUKIGAP'] = talib.CDLTASUKIGAP(self.open, self.high, self.low,
                                                                self.close)  # Pattern Recognition
        self.df_techfactor['CDLTHRUSTING'] = talib.CDLTHRUSTING(self.open, self.high, self.low,
                                                                self.close)  # Pattern Recognition
        self.df_techfactor['CDLTRISTAR'] = talib.CDLTRISTAR(self.open, self.high, self.low,
                                                            self.close)  # Pattern Recognition
        self.df_techfactor['CDLUNIQUE3RIVER'] = talib.CDLUNIQUE3RIVER(self.open, self.high, self.low,
                                                                      self.close)  # Pattern Recognition
        self.df_techfactor['CDLUPSIDEGAP2CROWS'] = talib.CDLUPSIDEGAP2CROWS(self.open, self.high, self.low,
                                                                            self.close)  # Pattern Recognition
        self.df_techfactor['CDLXSIDEGAP3METHODS'] = talib.CDLXSIDEGAP3METHODS(self.open, self.high, self.low,
                                                                              self.close)  # Pattern Recognition
        self.df_techfactor['CDLABANDONEDBABY'] = talib.CDLABANDONEDBABY(self.open, self.high, self.low, self.close,
                                                                        penetration=0.3)  # Pattern Recognition
        self.df_techfactor['CDLDARKCLOUDCOVER'] = talib.CDLDARKCLOUDCOVER(self.open, self.high, self.low,
                                                                          self.close,
                                                                          penetration=0.3)  # Pattern Recognition
        self.df_techfactor['CDLEVENINGDOJISTAR'] = talib.CDLEVENINGDOJISTAR(self.open, self.high, self.low,
                                                                            self.close,
                                                                            penetration=0.3)  # Pattern Recognition
        self.df_techfactor['CDLEVENINGSTAR'] = talib.CDLEVENINGSTAR(self.open, self.high, self.low, self.close,
                                                                    penetration=0.3)  # Pattern Recognition
        self.df_techfactor['CDLMATHOLD'] = talib.CDLMATHOLD(self.open, self.high, self.low, self.close,
                                                            penetration=0.3)  # Pattern Recognition
        self.df_techfactor['CDLMORNINGDOJISTAR'] = talib.CDLMORNINGDOJISTAR(self.open, self.high, self.low,
                                                                            self.close,
                                                                            penetration=0.3)  # Pattern Recognition
        self.df_techfactor['CDLMORNINGSTAR'] = talib.CDLMORNINGSTAR(self.open, self.high, self.low, self.close,
                                                                    penetration=0.3)  # Pattern Recognition
        return self.df_techfactor

    def run(self):
        self.df_techfactor = self.df_data.groupby(self.code).apply(TechFactors.get_techfactor)


def main():
    with open(tech_config.df_market_data, 'rb') as f:
        df_data = pickle.load(f)
    df_data = df_data.reset_index()
    tf = TechFactors(df_data)
    tf.run()
    tf.df_techfactor = tf.df_techfactor.sort_values(by=['date', 'code'], ascending=[True, True])
    tf.df_techfactor = tf.df_techfactor.set_index(['date', 'code'])
    with open(tech_config.df_techfactor_data, 'ab') as F:
        pickle.dump(tf.df_techfactor, F)
    glog.info('End.')


if __name__ == "__main__":
    glog.info('Start program execution.')
    main()
