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

    def _cycle_indicators(self):
        self.cycle_indicators = pd.DataFrame()
        # self.cycle_indicators['date'] = self.date #写上的话读文件直观，不写省内存
        # self.cycle_indicators['code'] = self.code
        self.cycle_indicators['HT_DCPERIOD'] = talib.HT_DCPERIOD(self.close)  # Cycle Indicators
        self.cycle_indicators['HT_DCPHASE'] = talib.HT_DCPHASE(self.close)  # Cycle Indicators
        self.cycle_indicators['HT_PHASOR_inphase'], self.cycle_indicators['HT_PHASOR_quadrature'] = talib.HT_PHASOR(self.close)  # Cycle Indicators
        self.cycle_indicators['HT_SINE_sine'], self.cycle_indicators['HT_SINE_leadsine'] = talib.HT_SINE(self.close)  # Cycle Indicators
        self.cycle_indicators['HT_TRENDMODE'] = talib.HT_TRENDMODE(self.close)  # Cycle Indicators
        #self.cycle_indicators.to_pickle('cycle_indicators.pkl') #无法追加写入 #默认w，写入最后一只股票，不能加参数
        # with open('cycle_indicators.pkl', 'ab') as F:
        #     pickle.dump(self.cycle_indicators, F) #无法追加写入 #wb覆盖重写，最后保留最后一支股票 #ab追加重写，无法追加，所以保留第一只股票
        self.cycle_indicators.to_csv('cycle_indicators.csv', mode='a+')

    def _overlap_studies(self):
        self.overlap_studies = pd.DataFrame()
        # self.overlap_studies['date'] = self.date
        # self.overlap_studies['code'] = self.code
        self.overlap_studies['HT_TRENDLINE'] = talib.HT_TRENDLINE(self.close)  # Overlap Studies
        for f in [0.1, 0.5, 0.9]:
            for s in [0.01, 0.05, 0.09]:
                self.overlap_studies[f'MAMA_mama{f}_{s}'], self.overlap_studies[f'MAMA_fama{f}_{s}'] = talib.MAMA(self.close, fastlimit=f,
                                                                                slowlimit=s)  # Overlap Studies
        for a in [0.01, 0.02, 0.03]:
            for m in [0.1, 0.2, 0.3]:
                self.overlap_studies[f'SAR{a}_{m}'] = talib.SAR(self.high, self.low, acceleration=a, maximum=m)  # Overlap Studies
        for a1 in [0.01, 0.02, 0.03]:
            for a2 in [0.1, 0.2, 0.3]:
                self.overlap_studies[f'SAREXT{a1}_{a2}_{a1}_{a1}_{a1}_{a2}'] = talib.SAREXT(self.high, self.low,
                                                                                       startvalue=0, offsetonreverse=0,
                                                                                       accelerationinitlong=a1,
                                                                                       accelerationlong=a2,
                                                                                       accelerationmaxlong=a1,
                                                                                       accelerationinitshort=a1,
                                                                                       accelerationshort=a1,
                                                                                       accelerationmaxshort=a2)  # Overlap Studies
        for t in [5, 10, 15, 30, 60, 120]:
            for up in [1.5, 2, 2.5]:
                self.overlap_studies[f'BBANDS_upperband_{t}t_{up}up'], self.overlap_studies[f'BBANDS_middleband_{t}t_{up}up'], \
                self.overlap_studies[f'BBANDS_lowerband_{t}t_{up}up'] = talib.BBANDS(self.close, timeperiod=t, nbdevup=up,
                                                                                 nbdevdn=up,
                                                                                 matype=0)  # Overlap Studies
        for t in [15, 30, 60, 90, 120]:
            self.overlap_studies[f'DEMA_{t}t'] = talib.DEMA(self.close, timeperiod=t)  # Overlap Studies
            self.overlap_studies[f'EMA_{t}t'] = talib.EMA(self.close, timeperiod=t)  # Overlap Studies
            self.overlap_studies[f'KAMA_{t}t'] = talib.KAMA(self.close, timeperiod=t)  # Overlap Studies
            self.overlap_studies[f'MA_{t}t'] = talib.MA(self.close, timeperiod=t, matype=0)  # Overlap Studies
            self.overlap_studies[f'SMA_{t}t'] = talib.SMA(self.close, timeperiod=t)  # Overlap Studies
            self.overlap_studies[f'TEMA_{t}t'] = talib.TEMA(self.close, timeperiod=t)  # Overlap Studies
            self.overlap_studies[f'TRIMA_{t}t'] = talib.TRIMA(self.close, timeperiod=t)  # Overlap Studies
            self.overlap_studies[f'WMA_{t}t'] = talib.WMA(self.close, timeperiod=t)  # Overlap Studies
        for t in [7, 14, 21, 28, 35]:
            self.overlap_studies[f'MIDPOINT_{t}t'] = talib.MIDPOINT(self.close, timeperiod=t)  # Overlap Studies
            self.overlap_studies[f'MIDPRICE_{t}t'] = talib.MIDPRICE(self.high, self.low, timeperiod=t)  # Overlap Studies
        for t in [3, 5, 7]:
            for v in [0.5, 0.7, 0.9]:
                self.overlap_studies[f'T3_{t}t_{v}v'] = talib.T3(self.close, timeperiod=t, vfactor=v)  # Overlap Studies
        self.overlap_studies.to_csv('overlap_studies.csv', mode='a+')

    def _volatility_indicators(self):
        self.volatility_indicators = pd.DataFrame()
        # self.volatility_indicators['date'] = self.date
        # self.volatility_indicators['code'] = self.code
        self.volatility_indicators['TRANGE'] = talib.TRANGE(self.high, self.low, self.close)  # Volatility Indicators
        for t in [7, 14, 21, 28, 35]:
            self.volatility_indicators[f'ATR_{t}t'] = talib.ATR(self.high, self.low, self.close,
                                                   timeperiod=t)  # Volatility Indicators
            self.volatility_indicators[f'NATR_{t}t'] = talib.NATR(self.high, self.low, self.close,
                                                     timeperiod=t)  # Volatility Indicators
        self.volatility_indicators.to_csv('volatility_indicators.csv', mode='a+')

    def _volume_indicators(self):
        self.volume_indicators = pd.DataFrame()
        # self.volume_indicators['date'] = self.date
        # self.volume_indicators['code'] = self.code
        self.volume_indicators['AD'] = talib.AD(self.high, self.low, self.close, self.volume)  # Volume Indicators
        self.volume_indicators['OBV'] = talib.OBV(self.close, self.volume)  # Volume Indicators
        for f in [3, 5, 9]:
            for s in [10, 20, 30]:
                self.volume_indicators[f'ADOSC_{f}f_{s}s'] = talib.ADOSC(self.high, self.low, self.close, self.volume,
                                                                   fastperiod=f, slowperiod=s)  # Volume Indicators
        self.volume_indicators.to_csv('volume_indicators.csv', mode='a+')

    def _momentum_indicators(self):
        self.momentum_indicators = pd.DataFrame()
        # self.momentum_indicators['date'] = self.date
        # self.momentum_indicators['code'] = self.code
        self.momentum_indicators['BOP'] = talib.BOP(self.open, self.high, self.low, self.close)  # Momentum Indicators
        for t in [7, 14, 28]:
            self.momentum_indicators[f'ADX_{t}t'] = talib.ADX(self.high, self.low, self.close, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'ADXR_{t}t'] = talib.ADXR(self.high, self.low, self.close,
                                                     timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'AROON_aroondown_{t}t'], self.momentum_indicators[f'AROON_aroondown_{t}t'] = talib.AROON(self.high, self.low, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'AROONOSC_{t}t'] = talib.AROONOSC(self.high, self.low, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'CCI_{t}t'] = talib.CCI(self.high, self.low, self.close, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'CMO_{t}t'] = talib.CMO(self.close, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'DX_{t}t'] = talib.DX(self.high, self.low, self.close, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'MFI_{t}t'] = talib.MFI(self.high, self.low, self.close, self.volume,
                                                   timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'MINUS_DI_{t}t'] = talib.MINUS_DI(self.high, self.low, self.close,
                                                             timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'MINUS_DM_{t}t'] = talib.MINUS_DM(self.high, self.low, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'PLUS_DI_{t}t'] = talib.PLUS_DI(self.high, self.low, self.close,
                                                           timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'PLUS_DM_{t}t'] = talib.PLUS_DM(self.high, self.low, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'RSI_{t}t'] = talib.RSI(self.close, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'WILLR_{t}t'] = talib.WILLR(self.high, self.low, self.close,
                                                       timeperiod=t)  # Momentum Indicators
        for f in [12]:
            for s in [26]:
                self.momentum_indicators[f'APO_{f}f_{s}s'] = talib.APO(self.close, fastperiod=f, slowperiod=s,
                                                               matype=0)  # Momentum Indicators
                self.momentum_indicators[f'PPO_{f}f_{s}s'] = talib.PPO(self.close, fastperiod=f, slowperiod=s,
                                                               matype=0)  # Momentum Indicators
        for f in [12]:
            for s in [26]:
                for si in [9]:
                    self.momentum_indicators[f'MACD_macd_{f}f_{s}s_{si}si'], self.momentum_indicators[
                        f'MACD_macdsignal_{f}f_{s}s_{si}si'], self.momentum_indicators[
                        f'MACD_macdhist_{f}f_{s}s_{si}si'] = talib.MACD(self.close, fastperiod=f,
                                                                                       slowperiod=s,
                                                                                       signalperiod=si)  # Momentum Indicators
                    self.momentum_indicators[f'MACDEXT_macd_{f}f_{s}s_{si}si'], self.momentum_indicators[
                        f'MACDEXT_macdsignal_{f}f_{s}s_{si}si'], self.momentum_indicators[
                        f'MACDEXT_macdhist_{f}f_{s}s_{si}si'] = talib.MACDEXT(self.close, fastperiod=f,
                                                                                             fastmatype=0, slowperiod=s,
                                                                                             slowmatype=0,
                                                                                             signalperiod=si,
                                                                                             signalmatype=0)  # Momentum Indicators
        for si in [9]:
            self.momentum_indicators[f'MACDFIX_macd_{si}si'], self.momentum_indicators[f'MACDFIX_macdsignal_{si}si'], self.momentum_indicators[
                f'MACDFIX_macdhist_{si}si'] = talib.MACDFIX(self.close, signalperiod=si)  # Momentum Indicators
        for t in [5, 10, 15, 20, 30]:
            self.momentum_indicators[f'MOM_{t}t'] = talib.MOM(self.close, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'ROC_{t}t'] = talib.ROC(self.close, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'ROCP_{t}t'] = talib.ROCP(self.close, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'ROCR_{t}t'] = talib.ROCR(self.close, timeperiod=t)  # Momentum Indicators
            self.momentum_indicators[f'ROCR100_{t}t'] = talib.ROCR100(self.close, timeperiod=t)  # Momentum Indicators
        for t in [15, 30, 60, 90, 120]:
            self.momentum_indicators[f'TRIX_{t}t'] = talib.TRIX(self.close, timeperiod=t)  # Momentum Indicators
        for f in [5]:
            for s in [3]:
                self.momentum_indicators[f'STOCH_slowk_{f}f_{s}s'], self.momentum_indicators[
                        f'STOCH_slowd_{f}f_{s}s'] = talib.STOCH(self.high, self.low, self.close,
                                                                                        fastk_period=f, slowk_period=s,
                                                                                        slowk_matype=0, slowd_period=s,
                                                                                        slowd_matype=0)  # Momentum Indicators
        for f1 in [5]:
            for f2 in [3]:
                self.momentum_indicators[f'STOCHF_fastk_{f1}f1_{f2}f2'], self.momentum_indicators[
                    f'STOCHF_fastd_{f1}f1_{f2}f2'] = talib.STOCHF(self.high, self.low, self.close,
                                                                              fastk_period=f1, fastd_period=f2,
                                                                              fastd_matype=0)  # Momentum Indicators
        for t in [14]:
            for f1 in [5]:
                for f2 in [3]:
                    self.momentum_indicators[f'STOCHRSI_fastk_{t}t_{f1}f1_{f2}f2'], self.momentum_indicators[
                        f'STOCHRSI_fastd_{t}t_{f1}f1_{f2}f2'] = talib.STOCHRSI(self.close, timeperiod=t,
                                                                                              fastk_period=f1,
                                                                                              fastd_period=f2,
                                                                                              fastd_matype=0)  # Momentum Indicators
        for t1 in [7]:
            for t2 in [14]:
                for t3 in [28]:
                    self.momentum_indicators[f'ULTOSC_{t1}t1_{t2}t2_{t3}t3'] = talib.ULTOSC(self.high, self.low, self.close,
                                                                                       timeperiod1=t1, timeperiod2=t2,
                                                                                       timeperiod3=t3)  # Momentum Indicators
        self.momentum_indicators.to_csv('momentum_indicators.csv', mode='a+')

    def _pattern_recognition(self):
        self.pattern_recognition = pd.DataFrame()
        # self.pattern_recognition['date'] = self.date
        # self.pattern_recognition['code'] = self.code
        self.pattern_recognition['CDL2CROWS'] = talib.CDL2CROWS(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDL3BLACKCROWS'] = talib.CDL3BLACKCROWS(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDL3INSIDE'] = talib.CDL3INSIDE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDL3LINESTRIKE'] = talib.CDL3LINESTRIKE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDL3OUTSIDE'] = talib.CDL3OUTSIDE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDL3STARSINSOUTH'] = talib.CDL3STARSINSOUTH(self.open, self.high, self.low,
                                                       self.close)  # Pattern Recognition
        self.pattern_recognition['CDL3WHITESOLDIERS'] = talib.CDL3WHITESOLDIERS(self.open, self.high, self.low,
                                                         self.close)  # Pattern Recognition
        self.pattern_recognition['CDLADVANCEBLOCK'] = talib.CDLADVANCEBLOCK(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLBELTHOLD'] = talib.CDLBELTHOLD(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLBREAKAWAY'] = talib.CDLBREAKAWAY(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLCLOSINGMARUBOZU'] = talib.CDLCLOSINGMARUBOZU(self.open, self.high, self.low,
                                                           self.close)  # Pattern Recognition
        self.pattern_recognition['CDLCONCEALBABYSWALL'] = talib.CDLCONCEALBABYSWALL(self.open, self.high, self.low,
                                                             self.close)  # Pattern Recognition
        self.pattern_recognition['CDLCOUNTERATTACK'] = talib.CDLCOUNTERATTACK(self.open, self.high, self.low,
                                                       self.close)  # Pattern Recognition
        self.pattern_recognition['CDLDOJI'] = talib.CDLDOJI(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLDOJISTAR'] = talib.CDLDOJISTAR(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLDRAGONFLYDOJI'] = talib.CDLDRAGONFLYDOJI(self.open, self.high, self.low,
                                                       self.close)  # Pattern Recognition
        self.pattern_recognition['CDLENGULFING'] = talib.CDLENGULFING(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLGAPSIDESIDEWHITE'] = talib.CDLGAPSIDESIDEWHITE(self.open, self.high, self.low,
                                                             self.close)  # Pattern Recognition
        self.pattern_recognition['CDLGRAVESTONEDOJI'] = talib.CDLGRAVESTONEDOJI(self.open, self.high, self.low,
                                                         self.close)  # Pattern Recognition
        self.pattern_recognition['CDLHAMMER'] = talib.CDLHAMMER(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLHANGINGMAN'] = talib.CDLHANGINGMAN(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLHARAMI'] = talib.CDLHARAMI(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLHARAMICROSS'] = talib.CDLHARAMICROSS(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLHIGHWAVE'] = talib.CDLHIGHWAVE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLHIKKAKE'] = talib.CDLHIKKAKE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLHIKKAKEMOD'] = talib.CDLHIKKAKEMOD(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLHOMINGPIGEON'] = talib.CDLHOMINGPIGEON(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLIDENTICAL3CROWS'] = talib.CDLIDENTICAL3CROWS(self.open, self.high, self.low,
                                                           self.close)  # Pattern Recognition
        self.pattern_recognition['CDLINNECK'] = talib.CDLINNECK(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLINVERTEDHAMMER'] = talib.CDLINVERTEDHAMMER(self.open, self.high, self.low,
                                                         self.close)  # Pattern Recognition
        self.pattern_recognition['CDLKICKING'] = talib.CDLKICKING(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLKICKINGBYLENGTH'] = talib.CDLKICKINGBYLENGTH(self.open, self.high, self.low,
                                                           self.close)  # Pattern Recognition
        self.pattern_recognition['CDLLADDERBOTTOM'] = talib.CDLLADDERBOTTOM(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLLONGLEGGEDDOJI'] = talib.CDLLONGLEGGEDDOJI(self.open, self.high, self.low,
                                                         self.close)  # Pattern Recognition
        self.pattern_recognition['CDLLONGLINE'] = talib.CDLLONGLINE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLMARUBOZU'] = talib.CDLMARUBOZU(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLMATCHINGLOW'] = talib.CDLMATCHINGLOW(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLONNECK'] = talib.CDLONNECK(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLPIERCING'] = talib.CDLPIERCING(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLRICKSHAWMAN'] = talib.CDLRICKSHAWMAN(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLRISEFALL3METHODS'] = talib.CDLRISEFALL3METHODS(self.open, self.high, self.low,
                                                             self.close)  # Pattern Recognition
        self.pattern_recognition['CDLSEPARATINGLINES'] = talib.CDLSEPARATINGLINES(self.open, self.high, self.low,
                                                           self.close)  # Pattern Recognition
        self.pattern_recognition['CDLSHOOTINGSTAR'] = talib.CDLSHOOTINGSTAR(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLSHORTLINE'] = talib.CDLSHORTLINE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLSPINNINGTOP'] = talib.CDLSPINNINGTOP(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLSTALLEDPATTERN'] = talib.CDLSTALLEDPATTERN(self.open, self.high, self.low,
                                                         self.close)  # Pattern Recognition
        self.pattern_recognition['CDLSTICKSANDWICH'] = talib.CDLSTICKSANDWICH(self.open, self.high, self.low,
                                                       self.close)  # Pattern Recognition
        self.pattern_recognition['CDLTAKURI'] = talib.CDLTAKURI(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLTASUKIGAP'] = talib.CDLTASUKIGAP(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLTHRUSTING'] = talib.CDLTHRUSTING(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLTRISTAR'] = talib.CDLTRISTAR(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLUNIQUE3RIVER'] = talib.CDLUNIQUE3RIVER(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.pattern_recognition['CDLUPSIDEGAP2CROWS'] = talib.CDLUPSIDEGAP2CROWS(self.open, self.high, self.low,
                                                           self.close)  # Pattern Recognition
        self.pattern_recognition['CDLXSIDEGAP3METHODS'] = talib.CDLXSIDEGAP3METHODS(self.open, self.high, self.low,
                                                             self.close)  # Pattern Recognition
        for p in [0.1, 0.3, 0.7]:
            self.pattern_recognition[f'CDLABANDONEDBABY_{p}p'] = talib.CDLABANDONEDBABY(self.open, self.high, self.low, self.close,
                                                                             penetration=p)  # Pattern Recognition
            self.pattern_recognition[f'CDLDARKCLOUDCOVER_{p}p'] = talib.CDLDARKCLOUDCOVER(self.open, self.high, self.low,
                                                                               self.close,
                                                                               penetration=p)  # Pattern Recognition
            self.pattern_recognition[f'CDLEVENINGDOJISTAR_{p}p'] = talib.CDLEVENINGDOJISTAR(self.open, self.high, self.low,
                                                                                 self.close,
                                                                                 penetration=p)  # Pattern Recognition
            self.pattern_recognition[f'CDLEVENINGSTAR_{p}p'] = talib.CDLEVENINGSTAR(self.open, self.high, self.low, self.close,
                                                                         penetration=p)  # Pattern Recognition
            self.pattern_recognition[f'CDLMATHOLD_{p}p'] = talib.CDLMATHOLD(self.open, self.high, self.low, self.close,
                                                                 penetration=p)  # Pattern Recognition
            self.pattern_recognition[f'CDLMORNINGDOJISTAR_{p}p'] = talib.CDLMORNINGDOJISTAR(self.open, self.high, self.low,
                                                                                 self.close,
                                                                                 penetration=p)  # Pattern Recognition
            self.pattern_recognition[f'CDLMORNINGSTAR_{p}p'] = talib.CDLMORNINGSTAR(self.open, self.high, self.low, self.close,
                                                                         penetration=p)  # Pattern Recognition

        self.pattern_recognition.to_csv('pattern_recognition.csv', mode='a+')

    def run(self):
        self.df_data.groupby(self.code).apply(TechFactors._cycle_indicators)
        # self.df_data.groupby(self.code).apply(TechFactors._overlap_studies)
        # self.df_data.groupby(self.code).apply(TechFactors._volatility_indicators)
        # self.df_data.groupby(self.code).apply(TechFactors._volume_indicators)
        # self.df_data.groupby(self.code).apply(TechFactors._momentum_indicators) #非常占内存
        # self.df_data.groupby(self.code).apply(TechFactors._pattern_recognition)


def main():
    with open(tech_config.df_data_file,'rb') as f:
        df_data = pickle.load(f)
    df_data = df_data.reset_index()
    tf = TechFactors(df_data)
    tf.run()
    glog.info('End.')


if __name__ == "__main__":
    glog.info('Start program execution.')
    main()
