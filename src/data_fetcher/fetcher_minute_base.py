#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
分钟线数据采集器（Baostock 版）
支持5、15、30、60分钟线
采用一次性查询日期范围，避免单日循环
"""

import pandas as pd
import time
import os
import json
from datetime import datetime, timedelta
from typing import Optional, List, Tuple
import math

from .utils import setup_logger, ensure_dir
from .connection_manager import get_baostock, with_baostock
from src.config import settings

logger = setup_logger('minute_fetcher')


def save_parquet(df: pd.DataFrame, file_path: str):
    """保存数据到Parquet文件"""
    ensure_dir(os.path.dirname(file_path))
    try:
        df.to_parquet(file_path, index=False, compression='snappy')
    except Exception as e:
        logger.error(f"Failed to save parquet file {file_path}: {e}")
        raise


class MinuteBaseFetcher:
    """分钟线数据采集器"""

    def __init__(self, max_retries: int = 3, base_delay: float = 5):
        self._bs_manager = get_baostock()
        self.max_retries = max_retries
        self.base_delay = base_delay
        logger.info(f"MinuteBaseFetcher (Baostock) initialized with max_retries={max_retries}, base_delay={base_delay}")

    def _save_checkpoint(self, code: str, start_date: str, end_date: str,
                       freq: int, success: bool = True):
        """保存下载检查点"""
        checkpoint_dir = f"{settings.DATA_ROOT}/market/{freq}min/_checkpoints"
        ensure_dir(checkpoint_dir)

        checkpoint_file = f"{checkpoint_dir}/{code}_{start_date}_{end_date}.json"
        checkpoint_data = {
            "code": code,
            "start_date": start_date,
            "end_date": end_date,
            "freq": freq,
            "last_update": datetime.now().isoformat(),
            "success": success,
            "retry_count": 0
        }

        try:
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _load_checkpoint(self, code: str, start_date: str, end_date: str, freq: int) -> dict:
        """加载下载检查点"""
        checkpoint_file = f"{settings.DATA_ROOT}/market/{freq}min/_checkpoints/{code}_{start_date}_{end_date}.json"
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
        return None

    def _get_existing_dates(self, code: str, freq: int) -> Tuple[set, set]:
        """获取已下载的日期范围"""
        existing_start_dates = set()
        existing_end_dates = set()

        freq_dir = f"{settings.DATA_ROOT}/market/{freq}min"
        if not os.path.exists(freq_dir):
            return existing_start_dates, existing_end_dates

        # 遍历年月目录
        for year_dir in os.listdir(freq_dir):
            if year_dir.startswith('year='):
                year = int(year_dir.split('=')[1])
                month_path = os.path.join(freq_dir, year_dir)

                for month_dir in os.listdir(month_path):
                    if month_dir.startswith('month='):
                        month = int(month_dir.split('=')[1])
                        stock_dir = os.path.join(month_path, month_dir)

                        # 检查股票文件是否存在
                        stock_file = os.path.join(stock_dir, f"stock={code}.parquet")
                        if os.path.exists(stock_file):
                            # 读取文件中的日期范围
                            try:
                                df = pd.read_parquet(stock_file)
                                if not df.empty and 'datetime' in df.columns:
                                    df['datetime'] = pd.to_datetime(df['datetime'])
                                    existing_start_dates.add(df['datetime'].min().strftime('%Y-%m-%d'))
                                    existing_end_dates.add(df['datetime'].max().strftime('%Y-%m-%d'))
                            except Exception as e:
                                logger.warning(f"Error reading existing file {stock_file}: {e}")

        return existing_start_dates, existing_end_dates

    def _get_bs(self):
        """获取 Baostock 连接"""
        return self._bs_manager.get_connection()

    def _retry_with_backoff(self, func, *args, **kwargs):
        """带指数退避的重试机制"""
        last_exception = None
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    delay = self.base_delay * (2 ** attempt)
                    logger.warning(f"Attempt {attempt + 1} failed for {args[0] if args else 'unknown'}: {str(e)}. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"All {self.max_retries} attempts failed for {args[0] if args else 'unknown'}")

        raise last_exception if last_exception else Exception("Unknown error in retry mechanism")

    @with_baostock
    def fetch_minute_range(self, code: str, start_date: str, end_date: str,
                          freq: int = 30) -> Optional[pd.DataFrame]:
        """
        获取某股票在指定日期范围内的分钟线数据（一次性查询）

        Args:
            code: 股票代码，格式如 'sh.600036' 或 'sz.000001'（需包含市场前缀）
            start_date: 开始日期，格式 'YYYY-MM-DD'
            end_date: 结束日期，格式 'YYYY-MM-DD'
            freq: 分钟频率，支持 5、15、30、60

        Returns:
            DataFrame，包含 datetime, open, high, low, close, volume, amount, code 列
        """
        try:
            bs = self._get_bs()

            # 检查是否有已存在的数据，如果是追加模式，则只下载缺失的部分
            existing_start_dates, existing_end_dates = self._get_existing_dates(code, freq)

            actual_start_date = start_date
            actual_end_date = end_date

            if existing_start_dates and existing_end_dates:
                # 如果有重叠的数据，调整日期范围
                max_existing_start = max(existing_start_dates)
                min_existing_end = min(existing_end_dates)

                # 如果下载范围与已有数据有重叠，调整开始日期
                if max_existing_start >= start_date:
                    # 如果数据完整覆盖或超出范围，直接返回
                    if min_existing_end >= end_date:
                        logger.info(f"Data for {code} ({start_date} to {end_date}) already exists, skipping")
                        return pd.DataFrame()
                    else:
                        actual_start_date = (datetime.strptime(min_existing_end, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                        logger.info(f"Resuming from {actual_start_date} for {code}")

            # 如果实际开始日期大于结束日期，说明没有新数据需要下载
            if actual_start_date > end_date:
                logger.info(f"No new data to download for {code}")
                return pd.DataFrame()

            # 官方示例的完整字段
            fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"

            logger.info(f"Querying {code} from {actual_start_date} to {actual_end_date}, freq={freq}")

            rs = bs.query_history_k_data_plus(
                code,
                fields,
                start_date=actual_start_date,
                end_date=actual_end_date,
                frequency=str(freq),
                adjustflag="2"  # 前复权
            )

            if rs is None:
                logger.error(f"Baostock query returned None for {code}")
                return None

            if rs.error_code != '0':
                logger.error(f"Baostock error for {code}: {rs.error_msg}")
                return None

            data_list = []
            while (rs.error_code == '0') and rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                logger.warning(f"No {freq}min data for {code} in range {start_date} to {end_date}")
                return None

            df = pd.DataFrame(data_list, columns=rs.fields)

            # 提取需要的列
            cols_needed = ['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount']
            missing = [c for c in cols_needed if c not in df.columns]
            if missing:
                logger.error(f"Missing columns: {missing}")
                return None
            df = df[cols_needed].copy()

            # 解析时间
            time_series = df['time'].astype(str)
            if len(time_series.iloc[0]) >= 14:
                df['time_str'] = time_series.str[8:14]   # 提取 HHMMSS
            else:
                df['time_str'] = time_series.str[-6:]
            df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time_str'], format='%Y-%m-%d %H%M%S')

            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

            df = df[['datetime', 'open', 'high', 'low', 'close', 'volume', 'amount']].copy()
            df['code'] = code
            df.sort_values('datetime', inplace=True)

            logger.info(f"Fetched {len(df)} {freq}min records for {code} from {start_date} to {end_date}")
            return df

        except Exception as e:
            logger.error(f"Error fetching {freq}min data for {code}: {str(e)}")
            raise

    def fetch_month_range(self, code: str, year: int, month: int,
                         freq: int = 30) -> Optional[pd.DataFrame]:
        """
        获取某股票某月所有交易日的分钟线数据（一次性查询）

        Args:
            code: 股票代码
            year: 年份
            month: 月份
            freq: 分钟频率

        Returns:
            DataFrame
        """
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end = datetime(year + 1, 1, 1) - timedelta(days=1)
            end_date = end.strftime('%Y-%m-%d')
        else:
            end = datetime(year, month + 1, 1) - timedelta(days=1)
            end_date = end.strftime('%Y-%m-%d')
        return self.fetch_minute_range(code, start_date, end_date, freq)

    def fetch_multiple_stocks(self, codes: List[str], start_date: str, end_date: str,
                              freq: int = 30, delay: float = 0.5) -> pd.DataFrame:
        """
        批量获取多只股票的分钟线数据，支持断点续传和自动重试

        Args:
            codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            freq: 分钟频率
            delay: 请求间隔（秒）

        Returns:
            合并后的 DataFrame
        """
        all_data = []
        total = len(codes)

        # 首先检查哪些股票已经下载完成
        remaining_codes = []
        completed_codes = []

        for code in codes:
            checkpoint = self._load_checkpoint(code, start_date, end_date, freq)
            if checkpoint and checkpoint.get('success', False):
                # 检查是否真的有数据
                existing_dates = self._get_existing_dates(code, freq)
                if existing_dates and existing_dates[0]:  # 有数据
                    completed_codes.append(code)
                    logger.info(f"Skipping already completed: {code}")
                else:
                    remaining_codes.append(code)
            else:
                remaining_codes.append(code)

        logger.info(f"Completed: {len(completed_codes)}, Remaining: {len(remaining_codes)}")

        # 下载剩余的股票
        for idx, code in enumerate(remaining_codes, 1):
            logger.info(f"Progress: {idx}/{len(remaining_codes)} - {code}")

            # 使用重试机制
            try:
                # 创建一个带参数的包装函数
                def _fetch_with_params():
                    return self.fetch_minute_range(code, start_date, end_date, freq)

                df = self._retry_with_backoff(_fetch_with_params)

                if df is not None and not df.empty:
                    self.save_minute_data(df, freq)
                    all_data.append(df)
                    self._save_checkpoint(code, start_date, end_date, freq, True)
            except Exception as e:
                logger.error(f"Failed to download {code}: {str(e)}")
                self._save_checkpoint(code, start_date, end_date, freq, False)

            time.sleep(delay)

        # 如果有完成的数据，尝试读取现有文件来合并
        if completed_codes:
            existing_data = []
            for code in completed_codes:
                existing_dates = self._get_existing_dates(code, freq)
                if existing_dates and existing_dates[0]:
                    try:
                        # 读取一年的数据作为示例（实际可能需要更复杂的逻辑）
                        test_year = datetime.strptime(start_date, '%Y-%m-%d').year
                        base_path = f"{settings.DATA_ROOT}/market/{freq}min/year={test_year}/month=01"
                        stock_file = f"{base_path}/stock={code}.parquet"
                        if os.path.exists(stock_file):
                            df = pd.read_parquet(stock_file)
                            if not df.empty:
                                existing_data.append(df)
                    except Exception as e:
                        logger.warning(f"Error reading existing data for {code}: {e}")

            if existing_data:
                all_data.extend(existing_data)

        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()

    def save_minute_data(self, df: pd.DataFrame, freq: int,
                        year: Optional[int] = None, month: Optional[int] = None) -> None:
        """
        保存分钟线数据到 Parquet，按年月分区，支持追加模式

        Args:
            df: 分钟线数据
            freq: 分钟频率
            year: 指定年份（可选）
            month: 指定月份（可选）
        """
        if df is None or df.empty:
            return

        # 确保 datetime 列存在且为 datetime 类型
        if 'datetime' not in df.columns:
            logger.error("DataFrame 缺少 'datetime' 列")
            return

        df['datetime'] = pd.to_datetime(df['datetime'])

        # 如果指定了年月，直接保存
        if year is not None and month is not None:
            self._save_data_with_append(df, freq, year, month)
        else:
            # 按年月分组保存
            df['year'] = df['datetime'].dt.year
            df['month'] = df['datetime'].dt.month

            for (yr, mon), group in df.groupby(['year', 'month']):
                self._save_data_with_append(group, freq, yr, mon)

    def _save_data_with_append(self, df: pd.DataFrame, freq: int, year: int, month: int):
        """保存数据到文件，支持追加模式"""
        base_path = f"{settings.DATA_ROOT}/market/{freq}min/year={year}/month={month:02d}"

        for code, group in df.groupby('code'):
            file_path = f"{base_path}/stock={code}.parquet"

            # 检查文件是否已存在
            if os.path.exists(file_path):
                # 读取现有数据
                try:
                    existing_df = pd.read_parquet(file_path)

                    # 合并数据，去重
                    # 创建时间戳列用于去重
                    group['timestamp'] = group['datetime'].astype('int64')
                    existing_df['timestamp'] = existing_df['datetime'].astype('int64')

                    # 合并并去重
                    combined_df = pd.concat([existing_df, group], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=['timestamp', 'code'], keep='last')

                    # 移除临时列
                    combined_df = combined_df.drop(['timestamp'], axis=1, errors='ignore')

                    # 保存合并后的数据
                    save_parquet(combined_df, file_path)
                    logger.info(f"Appended {len(group)} new records to {code} {freq}min data for {year}-{month:02d}")

                except Exception as e:
                    logger.warning(f"Error appending to existing file {file_path}, creating new one: {e}")
                    save_parquet(group, file_path)
            else:
                # 创建新文件
                ensure_dir(os.path.dirname(file_path))
                save_parquet(group, file_path)
                logger.info(f"Created new file {file_path} with {len(group)} records")

    def fetch_and_save(self, code: str, start_date: str, end_date: str,
                       freq: int = 30) -> bool:
        """
        获取并保存分钟线数据（便捷方法），带断点续传和自动重试

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            freq: 分钟频率

        Returns:
            是否成功
        """
        # 加载检查点
        checkpoint = self._load_checkpoint(code, start_date, end_date, freq)
        retry_count = checkpoint['retry_count'] if checkpoint else 0

        try:
            # 使用重试机制
            def _fetch():
                return self.fetch_minute_range(code, start_date, end_date, freq)

            df = self._retry_with_backoff(_fetch, code, start_date, end_date, freq)

            if df is not None and not df.empty:
                self.save_minute_data(df, freq)
                # 保存成功检查点
                self._save_checkpoint(code, start_date, end_date, freq, True)
                return True
            else:
                # 没有新数据也算是成功
                self._save_checkpoint(code, start_date, end_date, freq, True)
                return True

        except Exception as e:
            # 更新失败检查点，增加重试次数
            if checkpoint:
                retry_count = checkpoint.get('retry_count', 0) + 1
                checkpoint['retry_count'] = retry_count
                checkpoint['last_update'] = datetime.now().isoformat()
                checkpoint_file = f"{settings.DATA_ROOT}/market/{freq}min/_checkpoints/{code}_{start_date}_{end_date}.json"
                ensure_dir(os.path.dirname(checkpoint_file))
                with open(checkpoint_file, 'w', encoding='utf-8') as f:
                    json.dump(checkpoint, f, ensure_ascii=False, indent=2)

            logger.error(f"Failed to fetch data for {code} after {retry_count + 1} attempts: {str(e)}")
            return False
