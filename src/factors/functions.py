# src/factors/functions.py
"""
因子计算函数库
每个函数接收必要的参数，返回因子值序列（可向量化计算）
输入数据通常为 DataFrame，索引为日期，列为股票代码
"""

import pandas as pd
import numpy as np
from typing import Dict
import warnings


def moving_average(data: pd.DataFrame, window: int, field: str = 'close') -> pd.Series:
    """计算移动平均线"""
    return data[field].rolling(window).mean()


def rsi(data: pd.DataFrame, window: int = 14) -> pd.Series:
    """
    计算相对强弱指标 RSI

    Args:
        data: 包含 'close' 列的 DataFrame
        window: RSI 周期，默认 14

    Returns:
        RSI 值序列 [0, 100]
    """
    delta = data['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window).mean()

    # 防止除零：当 loss 为 0 时，RSI = 100
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        rs = gain / loss.replace(0, np.nan)
        rsi_value = 100 - (100 / (1 + rs))

    # 当 gain 和 loss 都为 0 时（价格无变化），RSI 设为 50
    rsi_value = rsi_value.fillna(50)

    return rsi_value


def volume_ratio(data: pd.DataFrame, window: int = 5) -> pd.Series:
    """
    成交量比率 = 当日成交量 / 过去N日均量

    Args:
        data: 包含 'volume' 列的 DataFrame
        window: 均量周期，默认 5

    Returns:
        成交量比率
    """
    vol = data['volume']
    avg_vol = vol.rolling(window).mean()
    # 防止除零
    result = vol / avg_vol.replace(0, np.nan)
    return result


def atr(data: pd.DataFrame, window: int = 14) -> pd.Series:
    """
    平均真实波幅 ATR

    Args:
        data: 包含 high, low, close 的 DataFrame
        window: ATR 周期，默认 14

    Returns:
        ATR 值
    """
    high = data['high']
    low = data['low']
    close = data['close']
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_value = tr.rolling(window).mean()
    return atr_value


def ratio(data: Dict[str, pd.DataFrame], numerator: str, denominator: str) -> pd.Series:
    """
    两个因子的比值（用于复合因子）

    Args:
        data: 字典，键为因子名，值为对应因子值的 DataFrame
        numerator: 分子因子名
        denominator: 分母因子名

    Returns:
        比值序列
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = data[numerator] / data[denominator].replace(0, np.nan)
    return result


# ========== MACD 指标 ==========

def macd_dif(data: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
    """
    MACD DIF线（快线 - 慢线）

    Args:
        data: 包含 'close' 列的 DataFrame
        fast: 快线周期，默认 12
        slow: 慢线周期，默认 26
        signal: 信号线周期（此函数未使用，保留用于参数一致性）

    Returns:
        DIF 值
    """
    close = data['close']
    exp1 = close.ewm(span=fast, adjust=False).mean()
    exp2 = close.ewm(span=slow, adjust=False).mean()
    return exp1 - exp2


def macd_dea(data: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
    """
    MACD DEA线（DIF 的信号线）

    Args:
        data: 包含 'close' 列的 DataFrame
        fast: 快线周期
        slow: 慢线周期
        signal: DEA 周期，默认 9

    Returns:
        DEA 值
    """
    close = data['close']
    exp1 = close.ewm(span=fast, adjust=False).mean()
    exp2 = close.ewm(span=slow, adjust=False).mean()
    dif = exp1 - exp2
    return dif.ewm(span=signal, adjust=False).mean()


def macd_bar(data: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
    """
    MACD柱状图 (2*(DIF-DEA))

    Args:
        data: 包含 'close' 列的 DataFrame
        fast: 快线周期
        slow: 慢线周期
        signal: DEA 周期

    Returns:
        MACD 柱状图值
    """
    close = data['close']
    exp1 = close.ewm(span=fast, adjust=False).mean()
    exp2 = close.ewm(span=slow, adjust=False).mean()
    dif = exp1 - exp2
    dea = dif.ewm(span=signal, adjust=False).mean()
    return 2 * (dif - dea)


# ========== KDJ 指标 ==========

def kdj_k(data: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3) -> pd.Series:
    """
    KDJ指标K值

    Args:
        data: 包含 high, low, close 的 DataFrame
        n: RSV 周期，默认 9
        m1: K 值平滑周期
        m2: D 值平滑周期（此函数未使用）

    Returns:
        K 值
    """
    low = data['low'].rolling(n).min()
    high = data['high'].rolling(n).max()

    # 防止除零：当 high == low 时，RSV = 50
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        rsv = (data['close'] - low) / (high - low).replace(0, np.nan) * 100
        rsv = rsv.fillna(50)

    return rsv.ewm(alpha=1/m1, adjust=False).mean()


def kdj_d(data: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3) -> pd.Series:
    """
    KDJ指标D值

    Args:
        data: 包含 high, low, close 的 DataFrame
        n: RSV 周期
        m1: K 值平滑周期
        m2: D 值平滑周期

    Returns:
        D 值
    """
    low = data['low'].rolling(n).min()
    high = data['high'].rolling(n).max()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        rsv = (data['close'] - low) / (high - low).replace(0, np.nan) * 100
        rsv = rsv.fillna(50)

    k = rsv.ewm(alpha=1/m1, adjust=False).mean()
    return k.ewm(alpha=1/m2, adjust=False).mean()


def kdj_j(data: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3) -> pd.Series:
    """
    KDJ指标J值 (3K-2D)

    Args:
        data: 包含 high, low, close 的 DataFrame
        n: RSV 周期
        m1: K 值平滑周期
        m2: D 值平滑周期

    Returns:
        J 值
    """
    low = data['low'].rolling(n).min()
    high = data['high'].rolling(n).max()

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        rsv = (data['close'] - low) / (high - low).replace(0, np.nan) * 100
        rsv = rsv.fillna(50)

    k = rsv.ewm(alpha=1/m1, adjust=False).mean()
    d = k.ewm(alpha=1/m2, adjust=False).mean()
    return 3 * k - 2 * d


# ========== 布林带 ==========

def bollinger_width(data: pd.DataFrame, window: int = 20, num_std: int = 2) -> pd.Series:
    """
    布林带宽度 = (上轨-下轨)/中轨

    Args:
        data: 包含 'close' 列的 DataFrame
        window: 移动平均周期
        num_std: 标准差倍数

    Returns:
        布林带宽度
    """
    close = data['close']
    ma = close.rolling(window).mean()
    std = close.rolling(window).std()
    upper = ma + num_std * std
    lower = ma - num_std * std

    # 防止除零
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        width = (upper - lower) / ma.replace(0, np.nan)

    return width


def bollinger_position(data: pd.DataFrame, window: int = 20, num_std: int = 2) -> pd.Series:
    """
    布林带位置 = (价格 - 下轨) / (上轨 - 下轨)
    值在 0-1 之间表示在带内，大于 1 或小于 0 表示突破

    Args:
        data: 包含 close 的 DataFrame
        window: 移动平均周期
        num_std: 标准差倍数

    Returns:
        布林带位置
    """
    close = data['close']
    ma = close.rolling(window).mean()
    std = close.rolling(window).std()
    upper = ma + num_std * std
    lower = ma - num_std * std

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        position = (close - lower) / (upper - lower).replace(0, np.nan)

    return position


# ========== CCI 指标 ==========

def cci(data: pd.DataFrame, window: int = 14) -> pd.Series:
    """
    顺势指标 CCI

    Args:
        data: 包含 high, low, close 的 DataFrame
        window: CCI 周期

    Returns:
        CCI 值
    """
    tp = (data['high'] + data['low'] + data['close']) / 3
    ma_tp = tp.rolling(window).mean()
    md = tp.rolling(window).apply(lambda x: abs(x - x.mean()).mean(), raw=False)

    # 防止除零
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cci_value = (tp - ma_tp) / (0.015 * md.replace(0, np.nan))

    return cci_value


# ========== 动量指标 ==========

def momentum(data: pd.DataFrame, window: int = 10) -> pd.Series:
    """
    动量指标 = 当前价格 / N天前价格 - 1

    Args:
        data: 包含 'close' 列的 DataFrame
        window: 回看周期

    Returns:
        动量值
    """
    close = data['close']
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = close / close.shift(window).replace(0, np.nan) - 1
    return result


def roc(data: pd.DataFrame, window: int = 10) -> pd.Series:
    """
    变化率 ROC = (当前价格 - N天前价格) / N天前价格 * 100

    Args:
        data: 包含 'close' 列的 DataFrame
        window: 回看周期

    Returns:
        ROC 值
    """
    close = data['close']
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        result = (close - close.shift(window)) / close.shift(window).replace(0, np.nan) * 100
    return result


# ========== 波动率指标 ==========

def volatility(data: pd.DataFrame, window: int = 20) -> pd.Series:
    """
    历史波动率（年化）

    Args:
        data: 包含 'close' 列的 DataFrame
        window: 计算周期

    Returns:
        年化波动率
    """
    close = data['close']
    returns = close.pct_change()
    vol = returns.rolling(window).std() * np.sqrt(252)
    return vol


# 预留资金流向函数
# def net_inflow_ratio(data, field='main_net_inflow', denominator='amount'):
#     return data[field] / data[denominator]


# ========== 因子注册表 ==========

FACTOR_REGISTRY = {
    'ma': moving_average,
    'rsi': rsi,
    'volume_ratio': volume_ratio,
    'atr': atr,
    'macd_dif': macd_dif,
    'macd_dea': macd_dea,
    'macd_bar': macd_bar,
    'kdj_k': kdj_k,
    'kdj_d': kdj_d,
    'kdj_j': kdj_j,
    'bollinger_width': bollinger_width,
    'bollinger_position': bollinger_position,
    'cci': cci,
    'momentum': momentum,
    'roc': roc,
    'volatility': volatility,
}


def get_factor_function(name: str):
    """
    获取因子计算函数

    Args:
        name: 因子名称

    Returns:
        因子计算函数，不存在则返回 None
    """
    return FACTOR_REGISTRY.get(name)


def list_factors() -> list:
    """列出所有可用的因子"""
    return list(FACTOR_REGISTRY.keys())
