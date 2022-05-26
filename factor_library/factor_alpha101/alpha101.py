import sys
sys.path.append('../factor_alpha101')
from alpha101_utils import *


class Alpha101():
    def __init__(self, df):
        self.open = df.open
        self.high = df.high
        self.low = df.low
        self.close = df.close
        self.volume = df.volume
        self.returns = df.close.pct_change()
        self.vwap = None
        self.ind = None

    def alpha1(self):
        x = self.close
        x[self.returns < 0] = stddev(self.returns, 20)
        alpha = rank(ts_argmax(x ** 2, 5)) - 0.5
        return alpha.fillna(value=0)

    def alpha2(self):
        r1 = rank(delta(np.log(self.volume), 2))
        r2 = rank((self.close - self.open) / self.open)
        alpha = -1 * correlation(r1, r2, 6)
        return alpha.fillna(value=0)

    def alpha3(self):
        r1 = rank(self.open)
        r2 = rank(self.volume)
        alpha = -1 * correlation(r1, r2, 10)
        return alpha.replace([-np.inf, np.inf], 0).fillna(value=0)

    def alpha4(self):
        r = rank(self.low)
        alpha = -1 * ts_rank(r, 9)
        return alpha.fillna(value=0)

    def alpha5(self):
        alpha = (rank((self.open - (ts_sum(self.vwap, 10) / 10)))
                 * (-1 * abs(rank((self.close - self.vwap)))))
        return alpha.fillna(value=0)

    def alpha6(self):
        alpha = -1 * correlation(self.open, self.volume, 10)
        return alpha.replace([-np.inf, np.inf], 0).fillna(value=0)

    def alpha7(self):
        adv20 = sma(self.volume, 20)
        alpha = -1 * ts_rank(abs(delta(self.close, 7)), 60) * np.sign(delta(self.close, 7))
        alpha[adv20 >= self.volume] = -1
        return alpha.fillna(value=0)

    def alpha8(self):
        x1 = (ts_sum(self.open, 5) * ts_sum(self.returns, 5))
        x2 = delay((ts_sum(self.open, 5) * ts_sum(self.returns, 5)), 10)
        alpha = -1 * rank(x1 - x2)
        return alpha.fillna(value=0)

    def alpha9(self):
        delta_close = delta(self.close, 1)
        x1 = ts_min(delta_close, 5) > 0
        x2 = ts_max(delta_close, 5) < 0
        alpha = -1 * delta_close
        alpha[x1 | x2] = delta_close
        return alpha.fillna(value=0)

    def alpha10(self):
        delta_close = delta(self.close, 1)
        x1 = ts_min(delta_close, 4) > 0
        x2 = ts_max(delta_close, 4) < 0
        x = -1 * delta_close
        x[x1 | x2] = delta_close
        alpha = rank(x)
        return alpha.fillna(value=0)

    def alpha11(self):
        x1 = rank(ts_max((self.vwap - self.close), 3))
        x2 = rank(ts_min((self.vwap - self.close), 3))
        x3 = rank(delta(self.volume, 3))
        alpha = (x1 + x2) * x3
        return alpha.fillna(value=0)

    def alpha12(self):
        alpha = np.sign(delta(self.volume, 1)) * (-1 * delta(self.close, 1))
        return alpha.fillna(value=0)

    def alpha13(self):
        alpha = -1 * rank(covariance(rank(self.close), rank(self.volume), 5))
        return alpha.fillna(value=0)

    def alpha14(self):
        x1 = correlation(self.open, self.volume, 10).replace(
            [-np.inf, np.inf], 0).fillna(value=0)
        x2 = -1 * rank(delta(self.returns, 3))
        alpha = x1 * x2
        return alpha.fillna(value=0)

    def alpha15(self):
        x1 = correlation(rank(self.high), rank(self.volume), 3).replace(
            [-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * ts_sum(rank(x1), 3)
        return alpha.fillna(value=0)

    def alpha16(self):
        alpha = -1 * rank(covariance(rank(self.high), rank(self.volume), 5))
        return alpha.fillna(value=0)

    def alpha17(self):
        adv20 = sma(self.volume, 20)
        x1 = rank(ts_rank(self.close, 10))
        x2 = rank(delta(delta(self.close, 1), 1))
        x3 = rank(ts_rank((self.volume / adv20), 5))
        alpha = -1 * (x1 * x2 * x3)
        return alpha.fillna(value=0)

    def alpha18(self):
        x = correlation(self.close, self.open, 10).replace(
            [-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * (rank((stddev(abs((self.close - self.open)), 5) + (self.close - self.open)) + x))
        return alpha.fillna(value=0)

    def alpha19(self):
        x1 = (-1 * np.sign((self.close - delay(self.close, 7)) + delta(self.close, 7)))
        x2 = (1 + rank(1 + ts_sum(self.returns, 250)))
        alpha = x1 * x2
        return alpha.fillna(value=0)

    def alpha20(self):
        alpha = -1 * (rank(self.open - delay(self.high, 1)) * rank(self.open - delay(self.close, 1)) * rank(
            self.open - delay(self.low, 1)))
        return alpha.fillna(value=0)

    def alpha21(self):
        x1 = sma(self.open, 8) + stddev(self.open, 8) < sma(self.open, 2)
        x2 = sma(self.open, 8) - stddev(self.open, 8) > sma(self.open, 2)
        x3 = sma(self.volume, 20) / self.volume < 1
        alpha = pd.DataFrame(np.ones_like(self.open), index=self.open.index, columns=self.open.columns)
        alpha[x1 | x3] = -1 * alpha
        return alpha

    def alpha22(self):
        x = correlation(self.high, self.volume, 5).replace(
            [-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * delta(x, 5) * rank(stddev(self.close, 20))
        return alpha.fillna(value=0)

    def alpha23(self):
        x = sma(self.high, 20) < self.high
        alpha = pd.DataFrame(np.zeros_like(
            self.close), index=self.close.index, columns=self.close.columns)
        alpha[x] = -1 * delta(self.high, 2).fillna(value=0)
        return alpha

    def alpha24(self):
        x = delta(sma(self.close, 100), 100) / delay(self.close, 100) <= 0.05
        alpha = -1 * delta(self.close, 3)
        alpha[x] = -1 * (self.close - ts_min(self.close, 100))
        return alpha.fillna(value=0)

    def alpha25(self):
        adv20 = sma(self.volume, 20)
        alpha = rank((((-1 * self.returns) * adv20) * self.vwap) * (self.high - self.close))
        return alpha.fillna(value=0)

    def alpha26(self):
        x = correlation(ts_rank(self.volume, 5), ts_rank(self.high, 5), 5).replace(
            [-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * ts_max(x, 3)
        return alpha.fillna(value=0)

    def alpha27(self):
        alpha = rank((ts_sum(correlation(rank(self.volume), rank(self.vwap), 6), 2) / 2.0))  # TODO
        alpha[alpha > 0.5] = -1
        alpha[alpha <= 0.5] = 1
        return alpha.fillna(value=0)

    def alpha28(self):
        adv20 = sma(self.volume, 20)
        x = correlation(adv20, self.low, 5).replace(
            [-np.inf, np.inf], 0).fillna(value=0)
        alpha = scale(((x + ((self.high + self.low) / 2)) - self.close))
        return alpha.fillna(value=0)

    def alpha29(self):
        x1 = ts_min(rank(
            rank(scale(np.log(ts_min(rank(rank(-1 * rank(delta((self.close - 1), 5)))), 2))))), 5)  # TODO
        x2 = ts_rank(delay((-1 * self.returns), 6), 5)
        alpha = x1 + x2
        return alpha.fillna(value=0)

    def alpha30(self):
        delta_close = delta(self.close, 1)
        x = np.sign(delta_close) + np.sign(delay(delta_close, 1)) + \
            np.sign(delay(delta_close, 2))
        alpha = ((1.0 - rank(x)) * ts_sum(self.volume, 5)) / ts_sum(self.volume, 20)
        return alpha.fillna(value=0)

    def alpha31(self):
        adv20 = sma(self.volume, 20)
        x1 = rank(rank(rank(decay_linear((-1 * rank(rank(delta(self.close, 10)))), 10))))
        x2 = rank((-1 * delta(self.close, 3)))
        x3 = np.sign(scale(correlation(adv20, self.low, 12).replace(
            [-np.inf, np.inf], 0).fillna(value=0)))
        alpha = x1 + x2 + x3
        return alpha.fillna(value=0)

    def alpha32(self):
        x = correlation(self.vwap, delay(self.close, 5), 230).replace(
            [-np.inf, np.inf], 0).fillna(value=0)
        alpha = scale(((sma(self.close, 7)) - self.close)) + 20 * scale(x)
        return alpha.fillna(value=0)

    def alpha33(self):
        alpha = rank(-1 + (self.open / self.close))
        return alpha

    def alpha34(self):
        x = (stddev(self.returns, 2) / stddev(self.returns, 5)).fillna(value=0)
        alpha = rank(2 - rank(x) - rank(delta(self.close, 1)))
        return alpha.fillna(value=0)

    def alpha35(self):
        x1 = ts_rank(self.volume, 32)
        x2 = 1 - ts_rank(self.close + self.high - self.low, 16)
        x3 = 1 - ts_rank(self.returns, 32)
        alpha = (x1 * x2 * x3).fillna(value=0)
        return alpha

    def alpha36(self):
        adv20 = sma(self.volume, 20)
        x1 = 2.21 * rank(correlation((self.close - self.open), delay(self.volume, 1), 15))
        x2 = 0.7 * rank((self.open - self.close))
        x3 = 0.73 * rank(ts_rank(delay((-1 * self.returns), 6), 5))
        x4 = rank(abs(correlation(self.vwap, adv20, 6)))
        x5 = 0.6 * rank((sma(self.close, 200) - self.open) * (self.close - self.open))
        alpha = x1 + x2 + x3 + x4 + x5
        return alpha.fillna(value=0)

    def alpha37(self):
        alpha = rank(correlation(delay(self.open - self.close, 1),
                                 self.close, 200)) + rank(self.open - self.close)
        return alpha.fillna(value=0)

    def alpha38(self):
        x = (self.close / self.open).replace([-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * rank(ts_rank(self.close, 10)) * rank(x)  # TODO
        return alpha.fillna(value=0)

    def alpha39(self):
        adv20 = sma(self.volume, 20)
        x = -1 * rank(delta(self.close, 7)) * \
            (1 - rank(decay_linear((self.volume / adv20), 9)))
        alpha = x * (1 + rank(ts_sum(self.returns, 250)))
        return alpha.fillna(value=0)

    def alpha40(self):
        alpha = -1 * rank(stddev(self.high, 10)) * correlation(self.high, self.volume, 10)
        return alpha.fillna(value=0)

    def alpha41(self):
        alpha = pow((self.high * self.low), 0.5) - self.vwap
        return alpha

    def alpha42(self):
        alpha = rank((self.vwap - self.close)) / rank((self.vwap + self.close))
        return alpha

    # fillna--前38天为NA
    def alpha43(self):
        adv20 = sma(self.volume, 20)
        alpha = ts_rank(self.volume / adv20, 20) * ts_rank((-1 * delta(self.close, 7)), 8)
        return alpha.fillna(value=0)

    def alpha44(self):
        alpha = -1 * correlation(self.high, rank(self.volume),
                                 5).replace([-np.inf, np.inf], 0).fillna(value=0)
        return alpha

    def alpha45(self):
        x = correlation(self.close, self.volume, 2).replace(
            [-np.inf, np.inf], 0).fillna(value=0)
        alpha = -1 * (rank(sma(delay(self.close, 5), 20)) * x *
                      rank(correlation(ts_sum(self.close, 5), ts_sum(self.close, 20), 2)))
        return alpha.fillna(value=0)

    def alpha46(self):
        x = ((delay(self.close, 20) - delay(self.close, 10)) / 10) - ((delay(self.close, 10) - self.close) / 10)
        alpha = (-1 * (self.close - delay(self.close, 1)))
        alpha[x < 0] = 1
        alpha[x > 0.25] = -1
        return alpha.fillna(value=0)

    def alpha47(self):
        adv20 = sma(self.volume, 20)
        alpha = ((rank((1 / self.close)) * self.volume) / adv20) * ((self.high *
                                                                     rank((self.high - self.close))) / sma(self.high,
                                                                                                           5)) - rank(
            (self.vwap - delay(self.vwap, 5)))
        return alpha.fillna(value=0)

    # TODO ind--指数类的子行业
    def alpha48(self):
        r1 = (correlation(delta(self.close, 1), delta(delay(self.close, 1), 1), 250)
              * delta(self.close, 1)) / self.close
        r2 = ts_sum((pow((delta(self.close, 1) / delay(self.close, 1)), 2)), 250)
        alpha = IndNeutralize(r1, self.ind) / r2
        # return alpha.fillna(value=0)
        pass

    def alpha49(self):
        x = (((delay(self.close, 20) - delay(self.close, 10)) / 10) -
             ((delay(self.close, 10) - self.close) / 10))
        alpha = (-1 * delta(self.close, 1))
        alpha[x < -0.1] = 1
        return alpha.fillna(value=0)

    def alpha50(self):
        alpha = -1 * ts_max(rank(correlation(rank(self.volume), rank(self.vwap), 5)), 5)
        return alpha.fillna(value=0)

    def alpha51(self):
        inner = (((delay(self.close, 20) - delay(self.close, 10)) / 10) -
                 ((delay(self.close, 10) - self.close) / 10))
        alpha = (-1 * delta(self.close, 1))
        alpha[inner < -0.05] = 1
        return alpha.fillna(value=0)

    def alpha52(self):
        x = rank(((ts_sum(self.returns, 240) - ts_sum(self.returns, 20)) / 220))
        alpha = -1 * delta(ts_min(self.low, 5), 5) * x * ts_rank(self.volume, 5)
        return alpha.fillna(value=0)

    def alpha53(self):
        alpha = -1 * delta((((self.close - self.low) - (self.high - self.close)) /
                            (self.close - self.low).replace(0, 0.0001)), 9)
        return alpha.fillna(value=0)

    def alpha54(self):
        x = (self.low - self.high).replace(0, -0.0001)
        alpha = -1 * (self.low - self.close) * (self.open ** 5) / (x * (self.close ** 5))
        return alpha

    def alpha55(self):
        x = (self.close - ts_min(self.low, 12)) / \
            (ts_max(self.high, 12) - ts_min(self.low, 12)).replace(0, 0.0001)
        alpha = -1 * correlation(rank(x), rank(self.volume),
                                 6).replace([-np.inf, np.inf], 0).fillna(value=0)
        return alpha

    # TODO cap?
    def alpha56(self, cap):
        alpha = 0 - \
                (1 * (rank((ts_sum(self.returns, 10) / ts_sum(ts_sum(self.returns, 2), 3))) * rank(
                    (self.returns * cap))))
        # return alpha.fillna(value=0)
        pass

    def alpha57(self):
        alpha = 0 - 1 * ((self.close - self.vwap) /
                         decay_linear(rank(ts_argmax(self.close, 30)), 2))
        return alpha.fillna(value=0)

    # TODO ind?
    def alpha58(self, ind):
        x = IndNeutralize(self.vwap, ind)
        alpha = -1 * ts_rank(decay_linear(correlation(x, self.volume, 4), 8), 6)
        # return alpha.fillna(value=0)
        pass

    # TODO ind?
    def alpha59(self):
        x = IndNeutralize(((self.vwap * 0.728317) + (self.vwap * (1 - 0.728317))), self.ind)
        alpha = -1 * ts_rank(decay_linear(correlation(x, self.volume, 4), 16), 8)
        # return alpha.fillna(value=0)
        pass

    def alpha60(self):
        x = ((self.close - self.low) - (self.high - self.close)) * \
            self.volume / (self.high - self.low).replace(0, 0.0001)
        alpha = - ((2 * scale(rank(x))) - scale(rank(ts_argmax(self.close, 10))))
        return alpha.fillna(value=0)

    def alpha61(self):
        adv180 = sma(self.volume, 180)
        alpha = rank((self.vwap - ts_min(self.vwap, 16))
                     ) < rank(correlation(self.vwap, adv180, 18))
        return alpha

    def alpha62(self):
        adv20 = sma(self.volume, 20)
        x1 = rank(correlation(self.vwap, ts_sum(adv20, 22), 10))
        x2 = rank(((rank(self.open) + rank(self.open)) < (rank(((self.high + self.low) / 2)) + rank(self.high))))
        alpha = x1 < x2
        return alpha * -1

    def alpha63(self):
        adv180 = sma(self.volume, 180).fillna(value=0)
        r1 = rank(decay_linear(delta(IndNeutralize(self.close, self.ind), 2), 8))
        r2 = rank(decay_linear(correlation(
            ((self.vwap * 0.318108) + (self.open * (1 - 0.318108))), ts_sum(adv180, 37), 14), 12))
        alpha = -1 * (r1 - r2)
        return alpha.fillna(value=0)

    def alpha64(self):
        adv120 = sma(self.volume, 120)
        x1 = rank(correlation(ts_sum(
            ((self.open * 0.178404) + (self.low * (1 - 0.178404))), 13), ts_sum(adv120, 13), 17))
        x2 = rank(delta(((((self.high + self.low) / 2) * 0.178404) +
                         (self.vwap * (1 - 0.178404))), 4))  # TODO
        alpha = x1 < x2
        return alpha * -1

    def alpha65(self):
        adv60 = sma(self.volume, 60)
        x1 = rank(correlation(
            ((self.open * 0.00817205) + (self.vwap * (1 - 0.00817205))), ts_sum(adv60, 9), 6))
        x2 = rank((self.open - ts_min(self.open, 14)))
        alpha = x1 < x2
        return alpha * -1

    def alpha66(self):
        x1 = rank(decay_linear(delta(self.vwap, 4), 7))
        x2 = (((self.low * 0.96633) + (self.low * (1 - 0.96633))) - self.vwap) / \
             (self.open - ((self.high + self.low) / 2))
        alpha = (x1 + ts_rank(decay_linear(x2, 11), 7)) * -1
        return alpha.fillna(value=0)

    def alpha67(self):
        adv20 = sma(self.volume, 20)
        r = rank(correlation(IndNeutralize(self.vwap, self.ind), IndNeutralize(adv20, self.ind), 6))
        alpha = pow(rank(self.high - ts_min(self.high, 2)), r) * -1
        return alpha.fillna(value=0)

    def alpha68(self):
        adv15 = sma(self.volume, 15)
        x1 = ts_rank(correlation(rank(self.high), rank(adv15), 9), 14)
        x2 = rank(delta(((self.close * 0.518371) + (self.low * (1 - 0.518371))), 1))  # TODO
        alpha = x1 < x2
        return alpha * -1

    def alpha69(self):
        adv20 = sma(self.volume, 20)
        r1 = rank(ts_max(delta(IndNeutralize(self.vwap, self.ind), 3), 5))
        r2 = ts_rank(correlation(
            ((self.close * 0.490655) + (self.vwap * (1 - 0.490655))), adv20, 5), 9)
        alpha = pow(r1, r2) * -1
        return alpha.fillna(value=0)

    def alpha70(self):
        adv50 = sma(self.volume, 50).fillna(value=0)
        r = ts_rank(correlation(IndNeutralize(self.close, self.ind), adv50, 18), 18)
        alpha = pow(rank(delta(self.vwap, 1)), r) * -1
        return alpha.fillna(value=0)

    def alpha71(self):
        adv180 = sma(self.volume, 180)
        x1 = ts_rank(decay_linear(correlation(
            ts_rank(self.close, 3), ts_rank(adv180, 12), 18), 4), 16)
        x2 = ts_rank(decay_linear(
            (rank(((self.low + self.open) - (self.vwap + self.vwap))).pow(2)), 16), 4)
        alpha = x1
        alpha[x1 < x2] = x2
        return alpha.fillna(value=0)

    def alpha72(self):
        adv40 = sma(self.volume, 40)
        x1 = rank(decay_linear(correlation(((self.high + self.low) / 2), adv40, 9), 10))
        x2 = rank(decay_linear(correlation(
            ts_rank(self.vwap, 4), ts_rank(self.volume, 19), 7), 3))
        alpha = (x1 / x2.replace(0, 0.0001)).fillna(value=0)
        return alpha

    def alpha73(self):
        x1 = rank(decay_linear(delta(self.vwap, 5), 3))
        x2 = delta(((self.open * 0.147155) + (self.low * (1 - 0.147155))), 2) / (
                    (self.open * 0.147155) + (self.low * (1 - 0.147155)))
        x3 = ts_rank(decay_linear((x2 * -1), 3), 17)
        alpha = x1
        alpha[x1 < x3] = x3
        return -1 * alpha.fillna(value=0)

    def alpha74(self):
        adv30 = sma(self.volume, 30)
        x1 = rank(correlation(self.close, ts_sum(adv30, 37), 15))
        x2 = rank(correlation(
            rank(((self.high * 0.0261661) + (self.vwap * (1 - 0.0261661)))), rank(self.volume), 11))
        alpha = x1 < x2
        return alpha * -1

    def alpha75(self):
        adv50 = sma(self.volume, 50)
        alpha = rank(correlation(self.vwap, self.volume, 4)) < rank(
            correlation(rank(self.low), rank(adv50), 12))
        return alpha

    def alpha76(self):
        adv81 = sma(self.volume, 81).fillna(value=0)
        r1 = rank(decay_linear(delta(self.vwap, 1), 12))
        r2 = ts_rank(decay_linear(
            ts_rank(correlation(IndNeutralize(self.low, self.ind), adv81, 8), 20), 17), 19)
        alpha = r1
        alpha[r1 < r2] = r2
        return alpha.fillna(value=0)

    def alpha77(self):
        adv40 = sma(self.volume, 40)
        x1 = rank(decay_linear(((((self.high + self.low) / 2) + self.high) - (self.vwap + self.high)), 20))
        x2 = rank(decay_linear(correlation(((self.high + self.low) / 2), adv40, 3), 6))
        alpha = x1
        alpha[x1 > x2] = x2
        return alpha.fillna(value=0)

    def alpha78(self):
        adv40 = sma(self.volume, 40)
        x1 = rank(correlation(
            ts_sum(((self.low * 0.352233) + (self.vwap * (1 - 0.352233))), 20), ts_sum(adv40, 20), 7))
        x2 = rank(correlation(rank(self.vwap), rank(self.volume), 6))
        alpha = x1.pow(x2)
        return alpha.fillna(value=0)

    def alpha79(self):
        adv150 = sma(self.volume, 150).fillna(value=0)
        r1 = rank(
            delta(IndNeutralize(((self.close * 0.60733) + (self.open * (1 - 0.60733))), self.ind), 1))
        r2 = rank(correlation(ts_rank(self.vwap, 4), ts_rank(adv150, 9), 15))
        alpha = (r1 < r2)  # TODO
        return alpha.fillna(value=0)

    def alpha80(self):
        adv10 = sma(self.volume, 10)
        r1 = rank(np.sign(
            delta(IndNeutralize(((self.open * 0.868128) + (self.high * (1 - 0.868128))), self.ind), 4)))
        r2 = ts_rank(correlation(self.high, adv10, 5), 6)
        alpha = pow(r1, r2) * -1
        return alpha.fillna(value=0)

    def alpha81(self):
        adv10 = sma(self.volume, 10)
        x1 = rank(np.log(
            product(rank((rank(correlation(self.vwap, ts_sum(adv10, 50), 8)).pow(4))), 15)))
        x2 = rank(correlation(rank(self.vwap), rank(self.volume), 5))
        alpha = x1 < x2
        return alpha * -1

    def alpha82(self):
        r1 = rank(decay_linear(delta(self.open, 1), 15))
        r2 = ts_rank(decay_linear(correlation(IndNeutralize(self.volume, self.ind),
                                              ((self.open * 0.634196) + (self.open * (1 - 0.634196))), 17), 7), 13)
        alpha = r1
        alpha[r1 > r2] = r2
        return -1 * alpha.fillna(value=0)

    def alpha83(self):
        x = rank(delay(((self.high - self.low) / (ts_sum(self.close, 5) / 5)), 2)) * \
            rank(rank(self.volume))
        alpha = x / (((self.high - self.low) / (ts_sum(self.close, 5) / 5)) / (self.vwap - self.close))
        return alpha.fillna(value=0)

    def alpha84(self):
        alpha = pow(ts_rank((self.vwap - ts_max(self.vwap, 15)), 21), delta(self.close, 5))
        return alpha.fillna(value=0)

    def alpha85(self):
        adv30 = sma(self.volume, 30)
        x1 = rank(correlation(
            ((self.high * 0.876703) + (self.close * (1 - 0.876703))), adv30, 10))
        alpha = x1.pow(
            rank(correlation(ts_rank(((self.high + self.low) / 2), 4), ts_rank(self.volume, 10), 7)))
        return alpha.fillna(value=0)

    def alpha86(self):
        adv20 = sma(self.volume, 20)
        x1 = ts_rank(correlation(self.close, sma(adv20, 15), 6), 20)
        x2 = rank(((self.open + self.close) - (self.vwap + self.open)))
        alpha = x1 < x2
        return alpha * -1

    def alpha87(self):
        adv81 = sma(self.volume, 81).fillna(value=0)
        r1 = rank(decay_linear(
            delta(((self.close * 0.369701) + (self.vwap * (1 - 0.369701))), 2), 3))
        r2 = ts_rank(decay_linear(
            abs(correlation(IndNeutralize(adv81, self.ind), self.close, 13)), 5), 14)
        alpha = r1
        alpha[r1 < r2] = r2
        return -1 * alpha.fillna(value=0)

    def alpha88(self):
        adv60 = sma(self.volume, 60)
        x1 = rank(decay_linear(
            ((rank(self.open) + rank(self.low)) - (rank(self.high) + rank(self.close))), 8))
        x2 = ts_rank(decay_linear(correlation(
            ts_rank(self.close, 8), ts_rank(adv60, 21), 8), 7), 3)
        alpha = x1
        alpha[x1 > x2] = x2
        return alpha.fillna(value=0)

    def alpha89(self):
        adv10 = sma(self.volume, 10)
        r1 = ts_rank(decay_linear(correlation(
            ((self.low * 0.967285) + (self.low * (1 - 0.967285))), adv10, 7), 6), 4)
        r2 = ts_rank(decay_linear(delta(IndNeutralize(self.vwap, self.ind), 3), 10), 15)
        alpha = r1 - r2
        return alpha.fillna(value=0)

    def alpha90(self):
        adv40 = sma(self.volume, 40).fillna(value=0)
        r1 = rank((self.close - ts_max(self.close, 5)))
        r2 = ts_rank(correlation(IndNeutralize(adv40, self.ind), self.low, 5), 3)
        alpha = pow(r1, r2) * -1
        return alpha.fillna(value=0)

    def alpha91(self):
        adv30 = sma(self.volume, 30)
        r1 = ts_rank(decay_linear(decay_linear(correlation(
            IndNeutralize(self.close, self.ind), self.volume, 10), 16), 4), 5)
        r2 = rank(decay_linear(correlation(self.vwap, adv30, 4), 3))
        alpha = (r1 - r2) * -1
        return alpha.fillna(value=0)

    def alpha92(self):
        adv30 = sma(self.volume, 30)
        x1 = ts_rank(decay_linear(
            ((((self.high + self.low) / 2) + self.close) < (self.low + self.open)), 15), 19)
        x2 = ts_rank(decay_linear(correlation(rank(self.low), rank(adv30), 8), 7), 7)
        alpha = x1
        alpha[x1 > x2] = x2
        return alpha.fillna(value=0)

    def alpha93(self):
        adv81 = sma(self.volume, 81).fillna(value=0)
        r1 = ts_rank(decay_linear(correlation(
            IndNeutralize(self.vwap, self.ind), adv81, 17), 20), 8)
        r2 = rank(decay_linear(
            delta(((self.close * 0.524434) + (self.vwap * (1 - 0.524434))), 3), 16))
        alpha = r1 / r2
        return alpha.fillna(value=0)

    def alpha94(self):
        adv60 = sma(self.volume, 60)
        x = rank((self.vwap - ts_min(self.vwap, 12)))
        alpha = x.pow(
            ts_rank(correlation(ts_rank(self.vwap, 20), ts_rank(adv60, 4), 18), 3)) * -1
        return alpha.fillna(value=0)

    def alpha95(self):
        adv40 = sma(self.volume, 40)
        x = ts_rank(
            (rank(correlation(ts_sum(((self.high + self.low) / 2), 19), ts_sum(adv40, 19), 13)).pow(5)), 12)  # TODO
        alpha = rank((self.open - ts_min(self.open, 12))) < x
        return alpha.fillna(value=0)

    def alpha96(self):
        adv60 = sma(self.volume, 60)
        x1 = ts_rank(decay_linear(correlation(rank(self.vwap), rank(self.volume), 4), 4), 8)
        x2 = ts_rank(decay_linear(ts_argmax(correlation(
            ts_rank(self.close, 7), ts_rank(adv60, 4), 4), 13), 14), 13)
        alpha = x1
        alpha[x1 < x2] = x2
        return -1 * alpha.fillna(value=0)  # TODO

    def alpha97(self):
        adv60 = sma(self.volume, 60).fillna(value=0)
        r1 = rank(decay_linear(delta(IndNeutralize(
            ((self.low * 0.721001) + (self.vwap * (1 - 0.721001))), self.ind), 3), 20))
        r2 = ts_rank(decay_linear(
            ts_rank(correlation(ts_rank(self.low, 8), ts_rank(adv60, 17), 5), 19), 16), 7)
        alpha = (r1 - r2) * -1
        return alpha.fillna(value=0)

    def alpha98(self):
        adv5 = sma(self.volume, 5)
        adv15 = sma(self.volume, 15)
        x1 = rank(decay_linear(correlation(self.vwap, ts_sum(adv5, 26), 5), 7))  # TODO
        alpha = x1 - rank(decay_linear(ts_rank(ts_argmin(correlation(rank(self.open), rank(adv15), 21), 9), 7), 8))
        return alpha.fillna(value=0)

    def alpha99(self):
        adv60 = sma(self.volume, 60)
        x1 = rank(correlation(ts_sum(((self.high + self.low) / 2), 20), ts_sum(adv60, 20), 9))
        x2 = rank(correlation(self.low, self.volume, 6))
        alpha = x1 < x2
        return alpha * -1

    def alpha100(self):
        adv20 = sma(self.volume, 20)
        r1 = IndNeutralize(
            rank(((((self.close - self.low) - (self.high - self.close)) / (self.high - self.low)) * self.volume)),
            self.ind)
        r2 = 1.5 * scale(IndNeutralize(r1, self.ind))
        r3 = scale(IndNeutralize(
            (correlation(self.close, rank(adv20), 5) - rank(ts_argmin(self.close, 30))), self.ind))
        alpha = -1 * (r2 - r3) * (self.volume / adv20)
        return alpha.fillna(value=0)

    def alpha101(self):
        alpha = (self.close - self.open) / ((self.high - self.low) + 0.001)
        return alpha
