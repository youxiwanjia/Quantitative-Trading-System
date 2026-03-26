# src/factors/processor.py
"""
因子标准化处理
"""

import pandas as pd


class FactorProcessor:
    @staticmethod
    def winsorize(series: pd.Series, limits=(0.01, 0.99)) -> pd.Series:
        """去极值：将超出分位数的值替换为边界值"""
        lower = series.quantile(limits[0])
        upper = series.quantile(limits[1])
        return series.clip(lower, upper)

    @staticmethod
    def standardize(series: pd.Series, method='rank') -> pd.Series:
        """标准化处理"""
        if method == 'rank':
            # 百分位排名 [0,1]
            return series.rank(pct=True)
        elif method == 'zscore':
            # Z-score 标准化
            return (series - series.mean()) / series.std()
        elif method == 'none':
            return series
        else:
            raise ValueError(f"Unknown method: {method}")

    @staticmethod
    def neutralize(series: pd.Series, group_series: pd.Series) -> pd.Series:
        """行业中性化：对分组后的因子值进行调整（例如减去组内均值）"""
        # group_series 是与 series 索引对齐的行业分类
        return series - series.groupby(group_series).transform('mean')