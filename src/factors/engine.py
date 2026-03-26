# src/factors/engine.py
"""
因子计算引擎（修正版）
负责根据因子定义动态计算因子值
"""

import pandas as pd
from typing import List, Dict, Any, Optional
from .definitions import FactorDefinitionLoader
from . import functions
from src.data.loader import DataLoader


class FactorEngine:
    """因子计算引擎"""

    def __init__(self, factor_config_path: str, data_loader: DataLoader):
        self.def_loader = FactorDefinitionLoader(factor_config_path)
        self.data_loader = data_loader
        # 将函数库中的函数映射到名称
        self.function_map = {
            'moving_average': functions.moving_average,
            'rsi': functions.rsi,
            'volume_ratio': functions.volume_ratio,
            'atr': functions.atr,
            'ratio': functions.ratio,
        }

    def compute_factor(self, factor_name: str, stocks: List[str],
                       start_date: str, end_date: str) -> pd.Series:
        """
        计算单个因子值，返回 MultiIndex Series (date, code) -> value
        """
        info = self.def_loader.get_factor_info(factor_name)
        if info is None:
            raise ValueError(f"Factor {factor_name} not defined")

        # 收集依赖数据
        data_deps = {}
        for dep in info['inputs']:
            if dep == 'daily':
                df = self.data_loader.load_daily(stocks, start_date, end_date)
                data_deps['daily'] = df
            elif dep == 'derived':
                pass
            else:
                raise NotImplementedError(f"Data type {dep} not implemented")

        func = self.function_map[info['function']]
        params = info.get('params', {})

        return self._call_func(func, data_deps, params, stocks, start_date, end_date)

    def _call_func(self, func, data_deps, params, stocks, start_date, end_date):
        """调用因子计算函数，返回 MultiIndex Series"""
        # 复合因子（ratio）
        if func.__name__ == 'ratio':
            numerator = params['numerator']
            denominator = params['denominator']
            num_series = self.compute_factor(numerator, stocks, start_date, end_date)
            den_series = self.compute_factor(denominator, stocks, start_date, end_date)
            # 对齐索引后计算比值
            common_idx = num_series.index.intersection(den_series.index)
            result = num_series.loc[common_idx] / den_series.loc[common_idx]
            return result

        # 普通因子依赖 daily 数据
        if 'daily' not in data_deps:
            raise ValueError(f"Factor {func.__name__} requires daily data")

        daily_df = data_deps['daily']  # MultiIndex (date, code)

        # 按股票循环计算
        factor_series_list = []
        for code in stocks:
            try:
                stock_data = daily_df.xs(code, level='code')
            except KeyError:
                continue

            if stock_data.empty:
                continue

            try:
                # 函数应返回 Series，索引为日期
                series = func(stock_data, **params)
                if series is not None and not series.empty:
                    # 添加股票代码层级
                    series.index = pd.MultiIndex.from_product([series.index, [code]],
                                                               names=['date', 'code'])
                    factor_series_list.append(series)
            except Exception as e:
                import logging
                logging.getLogger(__name__).error(f"Error computing factor for {code}: {e}")
                continue

        if not factor_series_list:
            # 返回空的 MultiIndex Series
            empty_idx = pd.MultiIndex(levels=[[], []], codes=[[], []], names=['date', 'code'])
            return pd.Series(index=empty_idx, dtype=float)

        # 合并所有股票的因子值
        result = pd.concat(factor_series_list)
        result.sort_index(inplace=True)
        return result

    def compute_factors(self, factor_names: List[str], stocks: List[str],
                        start_date: str, end_date: str) -> pd.DataFrame:
        """
        批量计算多个因子，返回 DataFrame (date, code) × factors
        """
        factor_dfs = []
        for fname in factor_names:
            series = self.compute_factor(fname, stocks, start_date, end_date)
            if not series.empty:
                df = series.to_frame(name=fname)
                factor_dfs.append(df)

        if not factor_dfs:
            return pd.DataFrame()

        # 按列合并，自动对齐索引
        result = pd.concat(factor_dfs, axis=1)
        result.sort_index(level=['date', 'code'], inplace=True)
        return result