"""
数据加载器
负责从本地 Parquet 文件读取数据，为因子计算和选股提供统一接口。
"""

import os
import pandas as pd
from datetime import datetime
from typing import List, Union, Optional

# 导入项目配置
from src.config import settings


class DataLoader:
    """数据加载器"""

    def __init__(self, data_root: Optional[str] = None):
        """
        Args:
            data_root: 数据根目录，默认使用 settings.DATA_ROOT
        """
        self.data_root = data_root or settings.DATA_ROOT
        self._stock_list = None  # 延迟加载

    def _load_stock_list(self) -> pd.DataFrame:
        """加载股票列表元数据，返回的 code 列已包含市场前缀（如 'sh.600036'）"""
        if self._stock_list is None:
            path = os.path.join(self.data_root, 'metadata', 'stock_list.csv')
            if not os.path.exists(path):
                raise FileNotFoundError(f"股票列表文件不存在: {path}")
            self._stock_list = pd.read_csv(path, dtype={'code': str})
        return self._stock_list

    def get_stock_universe(self, date: Union[str, datetime]) -> pd.DataFrame:
        """
        获取指定日期可交易的股票列表（主板、非ST、已上市未退市）
        返回的 DataFrame 中 code 列为带市场前缀的格式（如 'sh.600036'）
        """
        if isinstance(date, str):
            date = pd.to_datetime(date).date()
        elif isinstance(date, datetime):
            date = date.date()

        df = self._load_stock_list()

        # 提取纯数字代码用于主板判断
        df['code_num'] = df['code'].str.split('.').str[1]
        df['is_mainboard'] = df['code_num'].str[:3].isin(settings.MAINBOARD_PREFIXES)

        # 如果 is_st 列不存在，假设全为 False
        if 'is_st' not in df.columns:
            df['is_st'] = False

        # 处理上市/退市日期
        if 'list_date' in df.columns:
            df['list_date'] = pd.to_datetime(df['list_date'], errors='coerce')
        else:
            df['list_date'] = pd.NaT

        if 'delist_date' in df.columns:
            df['delist_date'] = pd.to_datetime(df['delist_date'], errors='coerce')
        else:
            df['delist_date'] = pd.NaT

        # 如果没有有效的上市/退市日期，直接返回主板非ST股票（不进行日期过滤）
        if df['list_date'].isna().all() or df['delist_date'].isna().all():
            return df[~df['is_st'] & df['is_mainboard']].copy()

        # 日期过滤：list_date <= 目标日期，且 (delist_date >= 目标日期 或 delist_date 缺失)
        date_ts = pd.Timestamp(date)
        mask_list = df['list_date'] <= date_ts
        mask_delist = (df['delist_date'] >= date_ts) | (df['delist_date'].isna())
        mask = mask_list & mask_delist & (~df['is_st']) & df['is_mainboard']
        return df[mask].copy()

    def load_daily(self,
                   stocks: List[str],
                   start_date: Union[str, datetime],
                   end_date: Union[str, datetime],
                   fields: Optional[List[str]] = None) -> pd.DataFrame:
        """
        加载日线数据
        stocks: 股票代码列表，格式如 ['sh.600036', 'sz.000001']
        start_date: 开始日期
        end_date: 结束日期
        fields: 需要返回的字段，默认返回所有字段

        Returns:
            MultiIndex DataFrame: 索引为 (date, stock)，列为指定字段
        """
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date).date()
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date).date()

        years = set(range(start_date.year, end_date.year + 1))
        data_frames = []

        for code in stocks:
            for year in years:
                # 文件路径：market/daily/year={year}/stock={code}.parquet
                file_path = os.path.join(
                    self.data_root, 'market', 'daily',
                    f'year={year}', f'stock={code}.parquet'
                )
                if not os.path.exists(file_path):
                    continue

                df = pd.read_parquet(file_path)
                if 'date' not in df.columns:
                    continue
                df['date'] = pd.to_datetime(df['date'])
                # 过滤日期范围
                mask = (df['date'] >= pd.Timestamp(start_date)) & (df['date'] <= pd.Timestamp(end_date))
                df = df.loc[mask]
                if df.empty:
                    continue
                df['code'] = code
                data_frames.append(df)

        if not data_frames:
            return pd.DataFrame()

        combined = pd.concat(data_frames, ignore_index=True)
        # 设置为 MultiIndex (date, code)
        combined.set_index(['date', 'code'], inplace=True)
        combined.sort_index(level='date', inplace=True)

        if fields:
            available = [f for f in fields if f in combined.columns]
            combined = combined[available]

        return combined

    def load_minute(self, code: str, freq: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        加载指定股票的分钟线数据
        code: 带市场前缀的股票代码，如 'sh.600036'
        freq: '5', '15', '30', '60' 等
        start_date, end_date: 'YYYY-MM-DD'
        """
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        years = range(start.year, end.year + 1)
        data_frames = []

        for year in years:
            for month in range(1, 13):
                file_path = os.path.join(
                    self.data_root, 'market', f'{freq}min',
                    f'year={year}', f'month={month:02d}', f'stock={code}.parquet'
                )
                if not os.path.exists(file_path):
                    continue
                df = pd.read_parquet(file_path)
                df['datetime'] = pd.to_datetime(df['datetime'])
                mask = (df['datetime'] >= start) & (df['datetime'] <= end)
                df = df.loc[mask]
                if not df.empty:
                    data_frames.append(df)

        if not data_frames:
            return pd.DataFrame()
        return pd.concat(data_frames, ignore_index=True)

    def load_factor(self,
                    factor_name: str,
                    date: Union[str, datetime],
                    stocks: Optional[List[str]] = None) -> pd.Series:
        """
        加载指定日期的因子值
        factor_name: 因子名称
        date: 日期 'YYYY-MM-DD'
        stocks: 股票代码列表（带前缀），若为 None 则返回所有股票的因子值

        Returns:
            Series: index 为股票代码（带前缀），值为因子值
        """
        if isinstance(date, str):
            date_str = date
        else:
            date_str = date.strftime('%Y-%m-%d')

        file_path = os.path.join(
            self.data_root, 'factors', factor_name,
            f'date={date_str}.parquet'
        )

        if not os.path.exists(file_path):
            return pd.Series(dtype=float)

        df = pd.read_parquet(file_path)
        if 'code' not in df.columns or 'value' not in df.columns:
            raise ValueError(f"因子文件 {file_path} 格式不正确，需要 'code' 和 'value' 列")
        df.set_index('code', inplace=True)

        if stocks is not None:
            df = df[df.index.isin(stocks)]

        return df['value']

    # 预留接口
    def load_money_flow(self, stocks, start_date, end_date):
        raise NotImplementedError

    def load_lhb(self, date):
        raise NotImplementedError