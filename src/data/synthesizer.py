#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
K线数据合成器
负责从基础周期数据合成高级别K线
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import datetime, timedelta

from src.data.loader import DataLoader
from src.data.trading_calendar import get_calendar
from src.data_fetcher.utils import setup_logger, ensure_dir, save_parquet
from src.config import settings

logger = setup_logger('kline_synthesizer')


class KLineSynthesizer:
    """K线合成器"""

    def __init__(self):
        self.data_loader = DataLoader()
        self.calendar = get_calendar()

    def _resample_minute(self, df: pd.DataFrame, rule: str) -> pd.DataFrame:
        """
        将1分钟数据重采样为指定分钟级别
        rule: '5T', '15T', '60T', '120T' 等
        """
        if df.empty:
            return df

        # 确保datetime是索引
        if 'datetime' in df.columns:
            df = df.set_index('datetime')
        elif df.index.name != 'datetime':
            raise ValueError("DataFrame must have datetime column or index")

        # OHLC重采样
        resampled = df.resample(rule).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'amount': 'sum'
        }).dropna()

        # 计算涨跌幅（基于close）
        resampled['pct_chg'] = resampled['close'].pct_change() * 100

        resampled.reset_index(inplace=True)
        return resampled

    def _resample_daily(self, df: pd.DataFrame, rule: str) -> pd.DataFrame:
        """
        将日线数据重采样为周线、月线、年线
        rule: 'W', 'M', 'Y'
        """
        if df.empty:
            return df

        # 确保date是索引
        if 'date' in df.columns:
            df = df.set_index('date')
        elif df.index.name != 'date':
            raise ValueError("DataFrame must have date column or index")

        # 周线/月线/年线重采样
        resampled = df.resample(rule).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'amount': 'sum'
        }).dropna()

        # 计算涨跌幅
        resampled['pct_chg'] = resampled['close'].pct_change() * 100

        resampled.reset_index(inplace=True)
        resampled.rename(columns={'index': 'date'}, inplace=True)
        return resampled

    def synthesize_5min(self, code: str, date: str) -> pd.DataFrame:
        """从1分钟合成5分钟K线"""
        df_1min = self.data_loader.load_minute(code, date, '1min')
        if df_1min.empty:
            return pd.DataFrame()
        return self._resample_minute(df_1min, '5T')

    def synthesize_15min(self, code: str, date: str) -> pd.DataFrame:
        """从5分钟或1分钟合成15分钟K线"""
        # 尝试从5分钟合成（如果有5分钟基础数据）
        df_5min = self.data_loader.load_minute(code, date, '5min')
        if not df_5min.empty:
            return self._resample_minute(df_5min, '15T')
        # 否则从1分钟合成
        df_1min = self.data_loader.load_minute(code, date, '1min')
        return self._resample_minute(df_1min, '15T')

    def synthesize_60min(self, code: str, date: str) -> pd.DataFrame:
        """从30分钟合成60分钟K线"""
        df_30min = self.data_loader.load_minute(code, date, '30min')
        if df_30min.empty:
            return pd.DataFrame()
        return self._resample_minute(df_30min, '60T')

    def synthesize_120min(self, code: str, date: str) -> pd.DataFrame:
        """从60分钟或30分钟合成120分钟K线"""
        df_60min = self.data_loader.load_minute(code, date, '60min')
        if not df_60min.empty:
            return self._resample_minute(df_60min, '120T')
        df_30min = self.data_loader.load_minute(code, date, '30min')
        return self._resample_minute(df_30min, '120T')

    def synthesize_weekly(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从日线合成周线"""
        df_daily = self.data_loader.load_daily([code], start_date, end_date)
        if df_daily.empty:
            return pd.DataFrame()
        # 重置索引以便重采样
        df_daily = df_daily.reset_index()
        return self._resample_daily(df_daily, 'W')

    def synthesize_monthly(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从日线合成月线"""
        df_daily = self.data_loader.load_daily([code], start_date, end_date)
        if df_daily.empty:
            return pd.DataFrame()
        df_daily = df_daily.reset_index()
        return self._resample_daily(df_daily, 'M')

    def synthesize_yearly(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从日线合成年线"""
        df_daily = self.data_loader.load_daily([code], start_date, end_date)
        if df_daily.empty:
            return pd.DataFrame()
        df_daily = df_daily.reset_index()
        return self._resample_daily(df_daily, 'Y')

    def get_kline(self, code: str, period: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        统一接口获取K线数据，自动合成所需周期
        period: '1min', '5min', '15min', '30min', '60min', '120min', 'daily', 'weekly', 'monthly', 'yearly'
        """
        if period == '1min':
            return self.data_loader.load_minute(code, start_date, '1min', date_range=True)
        elif period == '5min':
            return self.synthesize_5min(code, start_date)
        elif period == '15min':
            return self.synthesize_15min(code, start_date)
        elif period == '30min':
            return self.data_loader.load_minute(code, start_date, '30min', date_range=True)
        elif period == '60min':
            return self.synthesize_60min(code, start_date)
        elif period == '120min':
            return self.synthesize_120min(code, start_date)
        elif period == 'daily':
            return self.data_loader.load_daily([code], start_date, end_date)
        elif period == 'weekly':
            return self.synthesize_weekly(code, start_date, end_date)
        elif period == 'monthly':
            return self.synthesize_monthly(code, start_date, end_date)
        elif period == 'yearly':
            return self.synthesize_yearly(code, start_date, end_date)
        else:
            raise ValueError(f"Unsupported period: {period}")