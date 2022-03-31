from alpha101_utils import *


def alpha41(high, low, vwap):
    alpha = pow((high * low), 0.5) - vwap
    return alpha

# TODO rank
def alpha42(vwap, close):
    alpha = rank((vwap - close)) / rank((vwap + close))
    return alpha

# fillna--前38天为NA
def alpha43(volume, close):
    adv20 = sma(volume, 20)
    alpha = ts_rank(volume / adv20, 20) * ts_rank((-1 * delta(close, 7)), 8)
    return alpha.fillna(value=0)

# TODO replace,fillna,correlation
def alpha44(high, volume):
    alpha = -1 * correlation(high, rank(volume),
                             5).replace([-np.inf, np.inf], 0).fillna(value=0)
    return alpha

# TODO replace,fillna,correlation,rank
def alpha45(close, volume):
    x = correlation(close, volume, 2).replace(
        [-np.inf, np.inf], 0).fillna(value=0)
    alpha = -1 * (rank(sma(delay(close, 5), 20)) * x *
                  rank(correlation(ts_sum(close, 5), ts_sum(close, 20), 2)))
    return alpha.fillna(value=0)

# TODO fillna--第一天为NA,pdf的公式迷
def alpha46(close):
    x = ((delay(close, 20) - delay(close, 10)) / 10) - ((delay(close, 10) - close) / 10)
    alpha = (-1 * (close - delay(close, 1)))
    alpha[x < 0] = 1
    alpha[x > 0.25] = -1
    return alpha.fillna(value=0)

# TODO rank,fillna
def alpha47(volume, close, high, vwap):
    adv20 = sma(volume, 20)
    alpha = ((rank((1 / close)) * volume) / adv20) * ((high *
                                                       rank((high - close))) / sma(high, 5)) - rank((vwap - delay(vwap, 5)))
    return alpha.fillna(value=0)

# TODO fillna,ind--指数类的子行业
def alpha48(close, ind):
    r1 = (correlation(delta(close, 1), delta(delay(close, 1), 1), 250)
          * delta(close, 1)) / close
    r2 = ts_sum((pow((delta(close, 1) / delay(close, 1)), 2)), 250)
    alpha = IndNeutralize(r1, ind) / r2
    return alpha.fillna(value=0)


# TODO fillna--第一天为NA,pdf的公式迷
def alpha49(close):
    x = (((delay(close, 20) - delay(close, 10)) / 10) -
         ((delay(close, 10) - close) / 10))
    alpha = (-1 * delta(close, 1))
    alpha[x < -0.1] = 1
    return alpha.fillna(value=0)

# TODO rank,correlation,fillna
def alpha50(volume, vwap):
    alpha = -1 * ts_max(rank(correlation(rank(volume), rank(vwap), 5)), 5)
    return alpha.fillna(value=0)

