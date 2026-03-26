#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
工具函数模块
"""

import os
import logging
import pandas as pd
from datetime import datetime
from src.config import settings


def setup_logging():
    """配置日志系统（全局）"""
    log_config = settings.LOG_CONFIG
    log_file = log_config.get('file')
    log_level = log_config.get('level', 'INFO')
    log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 确保日志目录存在
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 配置根日志器
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


def setup_logger(name):
    """获取一个模块级别的 logger，如果根日志尚未配置，则配置"""
    # 检查是否已有根日志配置，如果没有则调用 setup_logging
    if not logging.getLogger().hasHandlers():
        setup_logging()
    return logging.getLogger(name)


def ensure_dir(path):
    """确保目录存在，如果不存在则创建"""
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def is_mainboard(code):
    """判断股票代码是否属于主板"""
    if not isinstance(code, str):
        code = str(code)
    # 取前三位
    prefix = code[:3]
    return prefix in settings.MAINBOARD_PREFIXES


def save_parquet(df, path, partition_cols=None):
    """
    保存DataFrame为Parquet格式
    """
    ensure_dir(os.path.dirname(path))
    if partition_cols:
        # 按分区列保存
        df.to_parquet(path, partition_cols=partition_cols,
                      compression=settings.COMPRESSION, index=False)
    else:
        df.to_parquet(path, compression=settings.COMPRESSION, index=False)
    return path


def load_stock_list():
    """加载股票列表（如果存在）"""
    if os.path.exists(settings.STOCK_LIST_PATH):
        return pd.read_csv(settings.STOCK_LIST_PATH, dtype={'code': str})
    return pd.DataFrame(columns=['code', 'name', 'list_date', 'delist_date', 'is_st'])


def update_stock_list():
    """
    更新股票列表（从AKShare或备用源获取）
    此函数已被 generate_stock_list_batch.py 替代，保留为兼容性
    """
    # 如果使用腾讯批量生成，此处可忽略或调用批量脚本
    # 简单占位
    logging.warning("update_stock_list 已弃用，请使用 generate_stock_list_batch.py")
    pass


def get_trade_dates(start_date, end_date):
    """
    获取交易日列表（预留函数，可后续从交易日历文件读取）
    """
    # 简化：返回指定日期范围内的所有工作日
    dates = pd.date_range(start=start_date, end=end_date, freq='B')
    return [d.strftime('%Y-%m-%d') for d in dates]