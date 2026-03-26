# src/strategies/multi_factor.py
import pandas as pd
from typing import List
from .base import SelectionStrategy
from src.factors.processor import FactorProcessor

print("===== multi_factor 模块已加载 =====")

class MultiFactorStrategy(SelectionStrategy):
    """多因子加权打分选股策略"""

    def __init__(self, params):
        super().__init__(params)
        self.factors = params['factors']
        self.weights = params['weights']
        self.top_n = params.get('top_n', 30)
        self.filter_st = params.get('filter_st', True)
        self.min_price = params.get('min_price', 1)
        self.standardize_method = params.get('standardize', 'rank')
        print("MultiFactorStrategy 初始化完成")

    def generate_signals(self, date: str, data_loader, factor_engine):
        print("=== generate_signals 开始 ===")
        print(f"选股日期: {date}")

        universe = data_loader.get_stock_universe(date)
        print(f"原始股票池数量: {len(universe)}")

        if self.filter_st:
            universe = universe[~universe['is_st']]
            print(f"剔除ST后股票池数量: {len(universe)}")

        stock_codes = universe['code'].tolist()
        print(f"最终股票代码数量: {len(stock_codes)}")

        if not stock_codes:
            print("股票池为空，返回空列表")
            return []

        start_calc = pd.to_datetime(date) - pd.Timedelta(days=180)
        start_calc = start_calc.strftime('%Y-%m-%d')
        print(f"因子计算区间: {start_calc} 到 {date}")

        factor_df = factor_engine.compute_factors(self.factors, stock_codes, start_calc, date)
        print("factor_df 计算完成")
        print(f"factor_df 类型: {type(factor_df)}")
        print(f"factor_df 形状: {factor_df.shape}")

        if factor_df.empty:
            print("factor_df 为空，返回空列表")
            return []

        if date not in factor_df.index.get_level_values('date'):
            print(f"日期 {date} 不在索引中")
            return []

        # 提取当日因子值
        latest = factor_df.xs(date, level='date')
        print(f"latest 原始形状: {latest.shape}")

        # 去重：对于同一股票，取第一个值
        if not latest.index.is_unique:
            dup_count = latest.index.duplicated().sum()
            print(f"发现 {dup_count} 个重复索引，进行去重")
            latest = latest.groupby(level='code').first()
            print(f"去重后形状: {latest.shape}")

        # 缺失值处理
        before_drop = len(latest)
        latest = latest.dropna()
        after_drop = len(latest)
        print(f"缺失值处理: 之前 {before_drop}, 之后 {after_drop}")

        if latest.empty:
            print("latest 为空，返回空列表")
            return []

        # 标准化
        normalized = latest.apply(
            lambda x: FactorProcessor.standardize(x, method=self.standardize_method)
        )
        print("标准化完成")

        # 加权求和
        score = pd.Series(0.0, index=normalized.index)
        for f, w in zip(self.factors, self.weights):
            if f in normalized.columns:
                score += normalized[f] * w
        print("加权求和完成")

        top_codes = score.nlargest(self.top_n).index.tolist()
        print(f"选股结果数量: {len(top_codes)}")
        if top_codes:
            print(f"前5只: {top_codes[:5]}")

        return top_codes