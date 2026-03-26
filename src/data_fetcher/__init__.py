#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据采集模块

提供统一的数据采集接口，支持多种数据源：
- Baostock (免费，主要数据源)
- Tushare (需要 Token)
"""

from .connection_manager import (
    get_baostock,
    get_tushare,
    baostock_context,
    tushare_context,
    with_baostock,
    with_tushare,
    DataSource,
    BaostockManager,
    TushareManager,
)

from .utils import (
    setup_logger,
    ensure_dir,
    is_mainboard,
    save_parquet,
    load_stock_list,
)

__all__ = [
    # 连接管理
    'get_baostock',
    'get_tushare',
    'baostock_context',
    'tushare_context',
    'with_baostock',
    'with_tushare',
    'DataSource',
    'BaostockManager',
    'TushareManager',
    # 工具函数
    'setup_logger',
    'ensure_dir',
    'is_mainboard',
    'save_parquet',
    'load_stock_list',
]
