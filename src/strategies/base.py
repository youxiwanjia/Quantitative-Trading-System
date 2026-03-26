# src/strategies/base.py
from abc import ABC, abstractmethod


class SelectionStrategy(ABC):
    """选股策略基类"""

    def __init__(self, params: dict):
        self.params = params

    @abstractmethod
    def generate_signals(self, date: str, data_loader, factor_engine):
        """
        生成选股信号
        :param date: 调仓日期 (YYYY-MM-DD)
        :param data_loader: 数据加载器
        :param factor_engine: 因子引擎（用于实时计算或读取因子）
        :return: 目标股票列表（或带权重的字典）
        """
        pass