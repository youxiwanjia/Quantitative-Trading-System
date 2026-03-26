# -*- coding: utf-8 -*-
"""系统配置文件"""

import os

# 项目根目录（上一级目录的上一级）
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

# 数据存储根目录（处理后数据）
DATA_ROOT = os.path.join(PROJECT_ROOT, 'data', 'processed').replace('\\', '/')

# 原始数据备份目录
RAW_ROOT = os.path.join(PROJECT_ROOT, 'data', 'raw').replace('\\', '/')

# 股票列表文件路径
STOCK_LIST_PATH = os.path.join(DATA_ROOT, 'metadata', 'stock_list.csv').replace('\\', '/')

# 主板股票代码前缀（用于过滤）
MAINBOARD_PREFIXES = ['600', '601', '603', '605', '000', '001', '002']

# 数据源配置（预留，目前主要使用AKShare）
DATA_SOURCES = {
    "daily": "akshare",
    "minute": "akshare",
}

# 通达信配置（暂未使用，保留）
TDX_CONFIG = {
    "market": "std",
    "multithread": True,
    "heartbeat": True,
}

# Parquet压缩格式
COMPRESSION = "snappy"  # 可选: snappy, gzip, zstd

# 日志配置
LOG_CONFIG = {
    "level": "DEBUG",   # 原来是 "INFO"
    "file": os.path.join(PROJECT_ROOT, 'logs', 'data_fetcher.log').replace('\\', '/'),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
}

# 默认数据开始日期（用于采集）
START_DATE = "2005-01-01"