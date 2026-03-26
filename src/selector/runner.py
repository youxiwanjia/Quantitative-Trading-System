# src/selector/runner.py
from src.data.loader import DataLoader
from src.factors.engine import FactorEngine
from src.strategies.loader import StrategyLoader
from typing import List

class StockSelector:
    """选股执行器，整合数据、因子和策略，生成每日选股结果"""

    def __init__(self, strategy_config_path: str, factor_config_path: str):
        self.data_loader = DataLoader()
        self.factor_engine = FactorEngine(factor_config_path, self.data_loader)
        self.strategy = StrategyLoader.load(strategy_config_path)

    def run(self, date: str):
        """执行选股"""
        signals = self.strategy.generate_signals(date, self.data_loader, self.factor_engine)
        print("成功打印")
        return signals