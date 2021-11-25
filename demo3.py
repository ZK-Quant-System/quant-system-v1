import pickle
import pandas as pd
import glog
import talib

class TechFactors:
    def __init__(self,close,open,high,low,volume,money):
        self.close = df_data['close']
        self.open = df_data['open']
        self.high = df_data['high']
        self.low = df_data['low']
        self.volume = df_data['volume']
        self.money = df_data['money']
    def _cycle_indicators(self):
        self.HT_DCPERIOD = talib.HT_DCPERIOD(self.close)  # Cycle Indicators
        self.HT_DCPHASE = talib.HT_DCPHASE(self.close)  # Cycle Indicators
        self.HT_PHASOR_inphase, self.HT_PHASOR_quadrature = talib.HT_PHASOR(self.close)  # Cycle Indicators
        self.HT_SINE_sine, self.HT_SINE_leadsine = talib.HT_SINE(self.close)  # Cycle Indicators
        self.HT_TRENDMODE = talib.HT_TRENDMODE(self.close)  # Cycle Indicators
    def _overlap_studies(self):
        self.HT_TRENDLINE = talib.HT_TRENDLINE(self.close) # Overlap Studies
        for f in [0.1,0.3,0.5,0.7,0.9]:
            for s in [0.01,0.03,0.05,0.07,0.09]:
                self[f'MAMA_mama{f}{s}'],self[f'MAMA_fama{f}{s}'] = talib.MAMA(self.close,fastlimit=f,slowlimit=s)#Overlap Studies
        for a in [0.01,0.02,0.03]:
            for m in [0.1,0.2,0.3]:
                self[f'SAR{a}{m}'] = talib.SAR(self.high, self.low, acceleration=a, maximum=m)  # Overlap Studies
        for a1 in [0.01,0.02,0.03]:
            for a2 in [0.1,0.2,0.3]:
                for a3 in [0.01,0.02,0.03]:
                    for a4 in [0.01, 0.02, 0.03]:
                        for a5 in [0.01, 0.02, 0.03]:
                            for a6 in [0.1, 0.2, 0.3]:
                                self[f'SAREXT{a1}{a2}{a3}{a4}{a5}{a6}'] = talib.SAREXT(self.high, self.low, startvalue = 0, offsetonreverse = 0, accelerationinitlong = a1, accelerationlong = a2, accelerationmaxlong = a3, accelerationinitshort = a4, accelerationshort = a5, accelerationmaxshort = a6)  # Overlap Studies
        for t in [5,10,15,20,30,60,120]:
            for up in [1.5,2,2.5]:
                self['BBANDS_upperband_{0}t_{1}up'.format(t, up)], self['BBANDS_middleband_{0}t_{1}up'.format(t,up)], self['BBANDS_lowerband_{0}t_{1}up'.format(t,up)] = talib.BBANDS(self.close, timeperiod = t, nbdevup = up, nbdevdn = up, matype = 0)  # Overlap Studies
        for t in [15,30,60,90,120]:
            self['DEMA_{0}t'.format(t)] = talib.DEMA(self.close, timeperiod = t)  # Overlap Studies
            self['EMA_{0}t'.format(t)] = talib.EMA(self.close, timeperiod = t)  # Overlap Studies
            self['KAMA_{0}t'.format(t)] = talib.KAMA(self.close, timeperiod = t)  # Overlap Studies
            self['MA_{0}t'.format(t)] = talib.MA(self.close, timeperiod = t, matype = 0)  # Overlap Studies
            self['SMA_{0}t'.format(t)] = talib.SMA(self.close, timeperiod = t)  # Overlap Studies
            self['TEMA_{0}t'.format(t)] = talib.TEMA(self.close, timeperiod = t)  # Overlap Studies
            self['TRIMA_{0}t'.format(t)] = talib.TRIMA(self.close, timeperiod = t)  # Overlap Studies
            self['WMA_{0}t'.format(t)] = talib.WMA(self.close, timeperiod = t)  # Overlap Studies
        for t in [7, 14, 21, 28, 35]:
            self['MIDPOINT_{0}t'.format(t)] = talib.MIDPOINT(self.close, timeperiod=t)  # Overlap Studies
            self['MIDPRICE_{0}t'.format(t)] = talib.MIDPRICE(self.high, self.low, timeperiod=t)  # Overlap Studies
        for t in [1,3, 5, 7, 9]:
            for v in [0.3,0.5,0.7,0.9]:
                self['T3_{0}t_{1}v'.format(t,v)] = talib.T3(self.close, timeperiod=t, vfactor=v)  # Overlap Studies
    def _volatility_indicators(self):
        self.TRANGE = talib.TRANGE(self.high, self.low, self.close) #Volatility Indicators
        for t in [7, 14, 21, 28, 35]:
            self['ATR_{0}t'.format(t)] = talib.ATR(self.high, self.low, self.close, timeperiod=t)  # Volatility Indicators
            self['NATR_{0}t'.format(t)] = talib.NATR(self.high, self.low, self.close, timeperiod=t)  # Volatility Indicators
    def _volume_indicators(self):
        self.AD = talib.AD(self.high, self.low, self.close, self.volume)  # Volume Indicators
        self.OBV = talib.OBV(self.close, self.volume)  # Volume Indicators
        for f in [1,3,5,7,9]:
            for s in [10,15,20,25,30]:
                self['ADOSC_{0}f_{1}s'.format(f,s)] = talib.ADOSC(self.high, self.low, self.close, self.volume, fastperiod=f, slowperiod=s)  # Volume Indicators
    def _momentum_indicators(self):
        self.BOP = talib.BOP(self.open, self.high, self.low, self.close)  # Momentum Indicators
        for t in [7, 14, 21, 28, 35]:
            self['ADX_{0}t'.format(t)] = talib.ADX(self.high, self.low, self.close, timeperiod=t)  # Momentum Indicators
            self['ADXR_{0}t'.format(t)] = talib.ADXR(self.high, self.low, self.close, timeperiod=t)  # Momentum Indicators
            self['AROON_aroondown_{0}t'.format(t)], self['AROON_aroondown_{0}t'.format(t)] = talib.AROON(self.high, self.low, timeperiod=t)  # Momentum Indicators
            self['AROONOSC_{0}t'.format(t)] = talib.AROONOSC(self.high, self.low, timeperiod=t)  # Momentum Indicators
            self['CCI_{0}t'.format(t)] = talib.CCI(self.high, self.low, self.close, timeperiod=t)  # Momentum Indicators
            self['CMO_{0}t'.format(t)] = talib.CMO(self.close, timeperiod=t)  # Momentum Indicators
            self['DX_{0}t'.format(t)] = talib.DX(self.high, self.low, self.close, timeperiod=t)  # Momentum Indicators
            self['MFI_{0}t'.format(t)] = talib.MFI(self.high, self.low, self.close, self.volume, timeperiod=t)  # Momentum Indicators
            self['MINUS_DI_{0}t'.format(t)] = talib.MINUS_DI(self.high, self.low, self.close, timeperiod=t)  # Momentum Indicators
            self['MINUS_DM_{0}t'.format(t)] = talib.MINUS_DM(self.high, self.low, timeperiod=t)  # Momentum Indicators
            self['PLUS_DI_{0}t'.format(t)] = talib.PLUS_DI(self.high, self.low, self.close,timeperiod=t)  # Momentum Indicators
            self['PLUS_DM_{0}t'.format(t)] = talib.PLUS_DM(self.high, self.low, timeperiod=t)  # Momentum Indicators
            self['RSI_{0}t'.format(t)] = talib.RSI(self.close, timeperiod=t)  # Momentum Indicators
            self['WILLR_{0}t'.format(t)] = talib.WILLR(self.high, self.low, self.close, timeperiod=t)  # Momentum Indicators
        for f in [6, 12, 18]:
            for s in [20, 26, 32]:
                self['APO_{0}f_{1}s'.format(f,s)] = talib.APO(self.close, fastperiod=f, slowperiod=s, matype=0)  # Momentum Indicators
                self['PPO_{0}f_{1}s'.format(f,s)] = talib.PPO(self.close, fastperiod=f, slowperiod=s, matype=0)  # Momentum Indicators
        for f in [6, 12, 18]:
            for s in [20, 26, 32]:
                for si in [3,6,9,12,15]:
                    self['MACD_macd_{0}f_{1}s_{2}si'.format(f,s,si)], self['MACD_macdsignal_{0}f_{1}s_{2}si'.format(f,s,si)], self['MACD_macdhist_{0}f_{1}s_{2}si'.format(f,s,si)] = talib.MACD(self.close,fastperiod=f, slowperiod=s, signalperiod=si)  # Momentum Indicators
                    self['MACDEXT_macd_{0}f_{1}s_{2}si'.format(f,s,si)], self['MACDEXT_macdsignal_{0}f_{1}s_{2}si'.format(f,s,si)], self['MACDEXT_macdhist_{0}f_{1}s_{2}si'.format(f,s,si)] = talib.MACDEXT(self.close, fastperiod=f, fastmatype=0, slowperiod=s, slowmatype=0, signalperiod=si, signalmatype=0)  # Momentum Indicators
        for si in [3, 6, 9, 12, 15]:
            self['MACDFIX_macd_{0}si'.format(si)], self['MACDFIX_macdsignal_{0}si'.format(si)], self['MACDFIX_macdhist_{0}si'.format(si)] = talib.MACDFIX(self.close, signalperiod=si)  # Momentum Indicators
        for t in [5,10,15,20,30]:
            self['MOM_{0}t'.format(t)] = talib.MOM(self.close, timeperiod=t)  # Momentum Indicators
            self['ROC_{0}t'.format(t)] = talib.ROC(self.close, timeperiod=t)  # Momentum Indicators
            self['ROCP_{0}t'.format(t)] = talib.ROCP(self.close, timeperiod=t)  # Momentum Indicators
            self['ROCR_{0}t'.format(t)] = talib.ROCR(self.close, timeperiod=t)  # Momentum Indicators
            self['ROCR100_{0}t'.format(t)] = talib.ROCR100(self.close, timeperiod=t)  # Momentum Indicators
        for t in [15,30,60,90,120]:
            self['TRIX_{0}t'.format(t)] = talib.TRIX(self.close, timeperiod=t)  # Momentum Indicators
        for f in [5,10,15]:
            for s1 in [1,2,3]:
                for s2 in [1,2,3]:
                    self['STOCH_slowk_{0}f_{1}s1_{2}s2'.format(f,s1,s2)], self['STOCH_slowd_{0}f_{1}s1_{2}s2'.format(f,s1,s2)] = talib.STOCH(self.high, self.low, self.close, fastk_period=f, slowk_period=s1, slowk_matype=0, slowd_period=s2, slowd_matype=0)  # Momentum Indicators
        for f1 in [5,10,15]:
            for f2 in [1,2,3]:
                self['STOCHF_fastk_{0}f1_{1}f2'.format(f1,f2)], self['STOCHF_fastd_{0}f1_{1}f2'.format(f1,f2)] = talib.STOCHF(self.high, self.low, self.close, fastk_period=f1, fastd_period=f2, fastd_matype=0)  # Momentum Indicators
        for t in [7, 14, 21, 28, 35]:
            for f1 in [5, 10, 15]:
                for f2 in [1, 2, 3]:
                    self['STOCHRSI_fastk_{0}t_{1}f1_{2}f2'.format(t,f1,f2)], self['STOCHRSI_fastd_{0}t_{1}f1_{2}f2'.format(t,f1,f2)] = talib.STOCHRSI(self.close, timeperiod=t, fastk_period=f1, fastd_period=f2, fastd_matype=0)  # Momentum Indicators
        for t1 in [3,5,7,9]:
            for t2 in [12,14,16,18]:
                for t3 in [24,26,28,30,35]:
                    self['ULTOSC_{0}t1_{1}t2_{2}t3'.format(t1,t2,t3)] = talib.ULTOSC(self.high,self.low, self.close, timeperiod1=t1, timeperiod2=t2, timeperiod3=t3)  # Momentum Indicators
    def _pattern_recognition(self):
        self.CDL2CROWS = talib.CDL2CROWS(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDL3BLACKCROWS = talib.CDL3BLACKCROWS(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDL3INSIDE = talib.CDL3INSIDE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDL3LINESTRIKE = talib.CDL3LINESTRIKE(self.open, self.high, self.low,self.close)  # Pattern Recognition
        self.CDL3OUTSIDE = talib.CDL3OUTSIDE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDL3STARSINSOUTH = talib.CDL3STARSINSOUTH(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDL3WHITESOLDIERS = talib.CDL3WHITESOLDIERS(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLADVANCEBLOCK = talib.CDLADVANCEBLOCK(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLBELTHOLD = talib.CDLBELTHOLD(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLBREAKAWAY = talib.CDLBREAKAWAY(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLCLOSINGMARUBOZU = talib.CDLCLOSINGMARUBOZU(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLCONCEALBABYSWALL = talib.CDLCONCEALBABYSWALL(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLCOUNTERATTACK = talib.CDLCOUNTERATTACK(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLDOJI = talib.CDLDOJI(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLDOJISTAR = talib.CDLDOJISTAR(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLDRAGONFLYDOJI = talib.CDLDRAGONFLYDOJI(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLENGULFING = talib.CDLENGULFING(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLGAPSIDESIDEWHITE = talib.CDLGAPSIDESIDEWHITE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLGRAVESTONEDOJI = talib.CDLGRAVESTONEDOJI(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLHAMMER = talib.CDLHAMMER(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLHANGINGMAN = talib.CDLHANGINGMAN(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLHARAMI = talib.CDLHARAMI(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLHARAMICROSS = talib.CDLHARAMICROSS(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLHIGHWAVE = talib.CDLHIGHWAVE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLHIKKAKE = talib.CDLHIKKAKE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLHIKKAKEMOD = talib.CDLHIKKAKEMOD(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLHOMINGPIGEON = talib.CDLHOMINGPIGEON(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLIDENTICAL3CROWS = talib.CDLIDENTICAL3CROWS(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLINNECK = talib.CDLINNECK(self.open, self.high, self.low, self.close) # Pattern Recognition
        self.CDLINVERTEDHAMMER = talib.CDLINVERTEDHAMMER(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLKICKING = talib.CDLKICKING(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLKICKINGBYLENGTH = talib.CDLKICKINGBYLENGTH(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLLADDERBOTTOM = talib.CDLLADDERBOTTOM(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLLONGLEGGEDDOJI = talib.CDLLONGLEGGEDDOJI(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLLONGLINE = talib.CDLLONGLINE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLMARUBOZU = talib.CDLMARUBOZU(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLMATCHINGLOW = talib.CDLMATCHINGLOW(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLONNECK = talib.CDLONNECK(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLPIERCING = talib.CDLPIERCING(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLRICKSHAWMAN = talib.CDLRICKSHAWMAN(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLRISEFALL3METHODS = talib.CDLRISEFALL3METHODS(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLSEPARATINGLINES = talib.CDLSEPARATINGLINES(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLSHOOTINGSTAR = talib.CDLSHOOTINGSTAR(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLSHORTLINE = talib.CDLSHORTLINE(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLSPINNINGTOP = talib.CDLSPINNINGTOP(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLSTALLEDPATTERN = talib.CDLSTALLEDPATTERN(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLSTICKSANDWICH = talib.CDLSTICKSANDWICH(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLTAKURI = talib.CDLTAKURI(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLTASUKIGAP = talib.CDLTASUKIGAP(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLTHRUSTING = talib.CDLTHRUSTING(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLTRISTAR = talib.CDLTRISTAR(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLUNIQUE3RIVER = talib.CDLUNIQUE3RIVER(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLUPSIDEGAP2CROWS = talib.CDLUPSIDEGAP2CROWS(self.open, self.high, self.low, self.close)  # Pattern Recognition
        self.CDLXSIDEGAP3METHODS = talib.CDLXSIDEGAP3METHODS(self.open, self.high, self.low, self.close)  # Pattern Recognition
        for p in [0.1,0.3,0.5,0.7]:
            self['CDLABANDONEDBABY_{0}p'.format(p)] = talib.CDLABANDONEDBABY(self.open, self.high, self.low, self.close, penetration=p)  # Pattern Recognition
            self['CDLDARKCLOUDCOVER_{0}p'.format(p)] = talib.CDLDARKCLOUDCOVER(self.open, self.high, self.low, self.close, penetration=p)  # Pattern Recognition
            self['CDLEVENINGDOJISTAR_{0}p'.format(p)] = talib.CDLEVENINGDOJISTAR(self.open, self.high, self.low, self.close, penetration=p)  # Pattern Recognition
            self['CDLEVENINGSTAR_{0}p'.format(p)] = talib.CDLEVENINGSTAR(self.open, self.high, self.low, self.close, penetration=p)  # Pattern Recognition
            self['CDLMATHOLD_{0}p'.format(p)] = talib.CDLMATHOLD(self.open, self.high, self.low, self.close, penetration=p)  # Pattern Recognition
            self['CDLMORNINGDOJISTAR_{0}p'.format(p)] = talib.CDLMORNINGDOJISTAR(self.open, self.high, self.low, self.close, penetration=p)  # Pattern Recognition
            self['CDLMORNINGSTAR_{0}p'.format(p)] = talib.CDLMORNINGSTAR(self.open, self.high, self.low, self.close, penetration=p)  # Pattern Recognition

def main():
    glog.info('Start ')
    df_data = pd.read_pickle('market_data_with_double_index.pkl')
    df_data = df_data.reset_index()
    df_data.groupby(df_data['code']).apply(TechFactors._cycle_indicators(df_data))
    df_data.groupby(df_data['code']).apply(TechFactors._overlap_studies(df_data))
    df_data.groupby(df_data['code']).apply(TechFactors._volatility_indicators(df_data))
    df_data.groupby(df_data['code']).apply(TechFactors._volume_indicators(df_data))
    df_data.groupby(df_data['code']).apply(TechFactors._momentum_indicators(df_data))
    df_data.groupby(df_data['code']).apply(TechFactors._pattern_recognition(df_data))
    glog.info('End.')
    df_data.to_csv('tech_factors.csv', mode='a+')

if __name__ == "__main__":
    glog.info('Start program execution.')
    main()

