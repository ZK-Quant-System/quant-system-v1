from alpha101_utils import *


# alpha21 PASS
def alpha21(volume, close):
    """
    Alpha#21: ((((sum(close, 8) / 8) + stddev(close, 8)) < (sum(close, 2) / 2)) ? (-1 * 1) : (((sum(close,
2) / 2) < ((sum(close, 8) / 8) - stddev(close, 8))) ? 1 : (((1 < (volume / adv20)) || ((volume /
adv20) == 1)) ? 1 : (-1 * 1))))
    """
    x1 = sma(close, 8) + stddev(close, 8) < sma(close, 2)
    x2 = sma(close, 8) - stddev(close, 8) > sma(close, 2)
    x3 = sma(volume, 20) / volume < 1
    alpha = pd.DataFrame(np.ones_like(
        close), index=close.index, columns=close.columns)
    alpha[x1 | x3] = -1 * alpha
    return alpha


# alpha999 REBUILD (应该用open而不是close)
def alpha999(volume, open):
    """
    Alpha#21: ((((sum(close, 8) / 8) + stddev(close, 8)) < (sum(close, 2) / 2)) ? (-1 * 1) : (((sum(close,
2) / 2) < ((sum(close, 8) / 8) - stddev(close, 8))) ? 1 : (((1 < (volume / adv20)) || ((volume /
adv20) == 1)) ? 1 : (-1 * 1))))
    修改：open 替代 close
    """
    x1 = sma(open, 8) + stddev(open, 8) < sma(open, 2)
    x2 = sma(open, 8) - stddev(open, 8) > sma(open, 2)
    x3 = sma(volume, 20) / volume < 1
    alpha = pd.DataFrame(np.ones_like(
        open), index=open.index, columns=open.columns)
    alpha[x1 | x3] = -1 * alpha
    return alpha
