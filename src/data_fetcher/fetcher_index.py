#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
指数日线数据采集器（Baostock 版）
用于获取沪深300、上证指数、深证成指等指数历史数据
"""

import pandas as pd
import time
from datetime import datetime
from typing import Optional, List, Dict

from .utils import setup_logger, ensure_dir, save_parquet
from .connection_manager import get_baostock, with_baostock
from src.config import settings

logger = setup_logger('index_fetcher')

# 常用指数代码映射（我们的代码 -> Baostock 格式）
INDEX_MAP: Dict[str, str] = {
    '000300.SH': 'sh.000300',   # 沪深300
    '000001.SH': 'sh.000001',   # 上证指数
    '399001.SZ': 'sz.399001',   # 深证成指
    '399006.SZ': 'sz.399006',   # 创业板指
    '000688.SH': 'sh.000688',   # 科创50
    '000016.SH': 'sh.000016',   # 上证50
    '000905.SH': 'sh.000905',   # 中证500
    '000852.SH': 'sh.000852',   # 中证1000
}

# 默认获取的指数列表
DEFAULT_INDICES = ['000300.SH', '000001.SH', '399001.SZ']


class IndexFetcher:
    """指数数据采集器"""

    def __init__(self):
        self._bs_manager = get_baostock()
        logger.info("IndexFetcher (Baostock) initialized")

    def _get_bs(self):
        """获取 Baostock 连接"""
        return self._bs_manager.get_connection()

    @with_baostock
    def fetch_index_by_code(self, index_code: str, start_date: Optional[str] = None,
                            end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        获取单只指数日线数据

        Args:
            index_code: 我们的代码，如 '000300.SH'
            start_date: 开始日期，格式 'YYYY-MM-DD'
            end_date: 结束日期，格式 'YYYY-MM-DD'

        Returns:
            DataFrame，索引为日期；失败返回 None
        """
        try:
            bs = self._get_bs()
            bs_code = INDEX_MAP.get(index_code)

            if bs_code is None:
                logger.error(f"不支持的指数代码: {index_code}，支持的代码: {list(INDEX_MAP.keys())}")
                return None

            start = start_date if start_date else settings.START_DATE
            end = end_date if end_date else datetime.now().strftime("%Y-%m-%d")

            logger.info(f"Fetching index data for {index_code} from {start} to {end}")

            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,code,open,high,low,close,volume,amount",
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag="3"  # 不复权（指数不需要复权）
            )

            if rs.error_code != '0':
                logger.error(f"Baostock error for {index_code}: {rs.error_msg}")
                return None

            data_list = []
            while (rs.error_code == '0') and rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                logger.warning(f"No data for {index_code}")
                return None

            df = pd.DataFrame(data_list, columns=rs.fields)
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
            df['code'] = index_code  # 使用我们的代码格式
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df.sort_index(inplace=True)

            logger.info(f"Fetched {len(df)} records for {index_code}")
            return df

        except Exception as e:
            logger.error(f"Error fetching {index_code}: {str(e)}")
            return None

    def fetch_all_indices(self, index_list: Optional[List[str]] = None,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        批量获取多个指数数据

        Args:
            index_list: 指数代码列表，None 则使用默认列表
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            合并后的 DataFrame
        """
        if index_list is None:
            index_list = DEFAULT_INDICES

        all_data = []
        for idx in index_list:
            df = self.fetch_index_by_code(idx, start_date, end_date)
            if df is not None:
                all_data.append(df)
            time.sleep(0.3)

        if all_data:
            return pd.concat(all_data)
        return None

    def save_index_data(self, df: pd.DataFrame, year: Optional[int] = None) -> None:
        """
        保存指数数据到 Parquet，格式同股票

        Args:
            df: 指数数据
            year: 指定年份保存，None 则按数据中的年份分别保存
        """
        if df is None or df.empty:
            return

        # 如果 df 的索引是日期，将其重置为列
        if isinstance(df.index, pd.DatetimeIndex):
            df_save = df.reset_index()
            df_save.rename(columns={'index': 'date'}, inplace=True)
        else:
            df_save = df.copy()

        df_save['date'] = pd.to_datetime(df_save['date'])
        df_save['year'] = df_save['date'].dt.year

        def save_year_data(df_year: pd.DataFrame, yr: int):
            """保存单年数据"""
            base_path = f"{settings.DATA_ROOT}/market/daily/year={yr}"
            for code, group in df_year.groupby('code'):
                group_save = group.drop(['year', 'code'], axis=1, errors='ignore')
                file_path = f"{base_path}/stock={code}.parquet"
                ensure_dir(base_path)
                group_save.to_parquet(file_path, compression=settings.COMPRESSION, index=False)
                logger.info(f"Saved {code} index data for {yr}")

        if year:
            df_year = df_save[df_save['year'] == year]
            if not df_year.empty:
                save_year_data(df_year, year)
        else:
            for yr, group in df_save.groupby('year'):
                save_year_data(group, yr)

    def fetch_and_save(self, index_code: str, start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> bool:
        """
        获取并保存指数数据（便捷方法）

        Args:
            index_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            是否成功
        """
        df = self.fetch_index_by_code(index_code, start_date, end_date)
        if df is not None and not df.empty:
            self.save_index_data(df)
            return True
        return False

    @staticmethod
    def get_supported_indices() -> List[str]:
        """获取支持的指数代码列表"""
        return list(INDEX_MAP.keys())

    @staticmethod
    def add_index_mapping(our_code: str, baostock_code: str) -> None:
        """
        添加新的指数映射

        Args:
            our_code: 我们的代码，如 '000300.SH'
            baostock_code: Baostock 格式代码，如 'sh.000300'
        """
        INDEX_MAP[our_code] = baostock_code
        logger.info(f"Added index mapping: {our_code} -> {baostock_code}")
