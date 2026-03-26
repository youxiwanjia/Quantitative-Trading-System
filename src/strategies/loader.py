# src/strategies/loader.py
import yaml
from typing import Dict, Any   # 添加此行
from .multi_factor import MultiFactorStrategy

class StrategyLoader:
    # ... 原有代码
    """根据配置文件加载策略实例"""

    @staticmethod
    def load(config_path: str):
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        strategy_type = config.get('type')
        params = config.get('params', {})

        if strategy_type == 'MultiFactorStrategy':
            return MultiFactorStrategy(params)
        else:
            raise ValueError(f"Unknown strategy type: {strategy_type}")