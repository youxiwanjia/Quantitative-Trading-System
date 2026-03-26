# src/factors/definitions.py
"""
因子定义加载模块
负责解析 factors.yaml 配置文件，提供因子元信息查询
"""

import yaml
from typing import Dict, Any, List   # 添加此行

class FactorDefinitionLoader:
    # ... 原有代码
    """加载并解析因子定义文件"""

    def __init__(self, config_path: str):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        self.factors = self.config.get('factors', {})

    def get_factor_info(self, factor_name: str) -> Dict[str, Any]:
        """获取单个因子的定义信息"""
        return self.factors.get(factor_name)

    def list_factors(self) -> List[str]:
        """返回所有因子名称列表"""
        return list(self.factors.keys())

    def get_dependencies(self, factor_name: str) -> List[str]:
        """获取因子依赖的原始数据类型或因子名"""
        info = self.get_factor_info(factor_name)
        return info.get('inputs', []) if info else []