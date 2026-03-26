#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
分钟线数据采集器（向后兼容模块）

此模块保留用于向后兼容，实际实现已迁移到 fetcher_minute_base.py
推荐使用 MinuteBaseFetcher 或直接导入 get_baostock 连接管理器
"""

import warnings

# 导入新的实现
from .fetcher_minute_base import MinuteBaseFetcher
from .connection_manager import get_baostock

# 向后兼容：创建别名
MinuteFetcher = MinuteBaseFetcher

__all__ = ['MinuteFetcher', 'MinuteBaseFetcher', 'get_baostock']

# 弃用警告
warnings.warn(
    "fetcher_minute.py 中的 MinuteFetcher 已弃用，请使用 fetcher_minute_base.MinuteBaseFetcher",
    DeprecationWarning,
    stacklevel=2
)
