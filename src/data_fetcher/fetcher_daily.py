#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日线数据采集器（Baostock 版）- 增强版
使用 Baostock 获取历史日线数据（免费、稳定、含成交量）

增强功能：
- 断点续传（记录已下载的日期范围）
- 重复下载检测（自动跳过已下载数据）
- 下载进度显示
- 失败自动重试（最多3次）
- 错误日志记录
"""

import pandas as pd
import time
from datetime import datetime
from typing import Optional, List
import logging
import sys
from pathlib import Path

from .utils import setup_logger, ensure_dir, save_parquet
from .connection_manager import get_baostock, with_baostock
from .download_progress import get_daily_download_progress, ProgressPrinter
from src.config import settings

logger = setup_logger('daily_fetcher')

# 全局错误计数器
ERROR_COUNT = 0
MAX_ERRORS = 5  # 连续错误5次后暂停


class DailyFetcher:
    """日线数据采集器（Baostock）"""

    def __init__(self, show_progress: bool = False, max_retries: int = 3):
        """
        Args:
            show_progress: 是否显示进度条
            max_retries: 单次下载失败最大重试次数
        """
        self._bs_manager = get_baostock()
        self.show_progress = show_progress
        self.max_retries = max_retries
        self.progress_printer: Optional[ProgressPrinter] = None

        # 错误处理
        self.error_count = 0
        self.skip_count = 0
        self.success_count = 0

        logger.info("DailyFetcher (Baostock Enhanced) initialized")

    def _get_bs(self):
        """获取 Baostock 连接"""
        return self._bs_manager.get_connection()

    def _should_skip(self, code: str, start_date: str, end_date: str) -> bool:
        """检查是否应该跳过（日期范围已下载）"""
        progress = get_daily_download_progress()
        return progress.is_already_downloaded(code, start_date, end_date)

    def _reset_error_count(self):
        """重置错误计数器（下载成功时调用）"""
        self.error_count = 0
        logger.debug("错误计数器已重置")

    def _increment_error(self):
        """增加错误计数"""
        self.error_count += 1
        logger.warning(f"下载失败 ({self.error_count}/{MAX_ERRORS})")

        if self.error_count >= MAX_ERRORS:
            logger.error(f"连续失败 {MAX_ERRORS} 次，暂停下载...")
            self._show_summary()
            sys.exit(1)

    @with_baostock
    def fetch_daily_by_code(self, code: str, start_date: Optional[str] = None,
                            end_date: Optional[str] = None, retry: int = 0) -> Optional[pd.DataFrame]:
        """
        获取单只股票的日线数据（前复权）

        Args:
            code: 股票代码，格式如 'sh.600036' 或 'sz.000001'（需包含市场前缀）
            start_date: 开始日期，格式 'YYYY-MM-DD'，默认 '2005-01-01'
            end_date: 结束日期，格式 'YYYY-MM-DD'，默认今天
            retry: 重试次数（内部使用）

        Returns:
            DataFrame，索引为日期，列为 OHLCV 数据；失败返回 None
        """
        try:
            bs = self._get_bs()

            start = start_date if start_date else settings.START_DATE
            end = end_date if end_date else datetime.now().strftime("%Y-%m-%d")

            # 检查是否应该跳过
            if self._should_skip(code, start, end):
                self.skip_count += 1
                logger.info(f"[跳过] {code}: {start} 至 {end}")
                return None

            logger.info(f"下载 {code}: {start} 至 {end}")

            # 查询数据
            rs = bs.query_history_k_data_plus(
                code,
                "date,code,open,high,low,close,volume,amount",
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag="2"  # 前复权
            )

            if rs.error_code != '0':
                logger.error(f"Baostock error for {code}: {rs.error_msg}")

                # 重试逻辑
                if retry < self.max_retries:
                    logger.info(f"将重试 ({retry + 1}/{self.max_retries})...")
                    time.sleep(5)  # 等待5秒后重试
                    return self.fetch_daily_by_code(code, start_date, end_date, retry + 1)
                else:
                    self._increment_error()
                    return None

            data_list = []
            while (rs.error_code == '0') and rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                logger.warning(f"无数据: {code}")
                return None

            df = pd.DataFrame(data_list, columns=rs.fields)
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df.sort_index(inplace=True)

            # 保存进度
            progress = get_daily_download_progress()
            progress.add_downloaded_dates_batch(code, df.index.strftime('%Y-%m-%d').tolist())

            # 更新计数器
            self.success_count += 1
            self._reset_error_count()

            if self.show_progress and self.progress_printer:
                self.progress_printer.update(len(df))

            logger.info(f"✓ {code}: 下载 {len(df)} 条记录")
            return df

        except Exception as e:
            logger.error(f"Error fetching {code}: {str(e)}")

            # 重试逻辑
            if retry < self.max_retries:
                logger.info(f"将重试 ({retry + 1}/{self.max_retries})...")
                time.sleep(5)
                return self.fetch_daily_by_code(code, start_date, end_date, retry + 1)
            else:
                self._increment_error()
                return None

    def fetch_all_mainboard(self, stock_list: List[str], start_date: Optional[str] = None,
                           end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        批量下载主板股票日线数据（带断点续传和进度显示）

        Args:
            stock_list: 股票代码列表，格式如 ['sh.600036', 'sz.000001']
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            合并后的 DataFrame
        """
        total = len(stock_list)

        if self.show_progress:
            self.progress_printer = ProgressPrinter(total, f"日线数据下载")
            print(f"\n开始下载 {total} 只股票的日线数据...")
            print(f"起始日期: {start_date or settings.START_DATE}")
            print(f"结束日期: {end_date or datetime.now().strftime('%Y-%m-%d')}")
            print(f"模式: 断点续传\n")

        # 获取需要下载的股票列表
        progress = get_daily_download_progress()
        to_download = progress.get_stocks_to_download(stock_list, start_date or settings.START_DATE, end_date or datetime.now().strftime('%Y-%m-%d'))

        if len(to_download) < total:
            print(f"\n检测到 {total - len(to_download)} 只股票已下载，跳过重复下载")

        all_data = []
        error_stocks = []

        for idx, code in enumerate(to_download, 1):
            logger.info(f"\n进度: [{idx}/{len(to_download)}] - {code}")

            df = self.fetch_daily_by_code(code, start_date, end_date)
            if df is not None and not df.empty:
                all_data.append(df)
            else:
                error_stocks.append(code)

            # 每处理10只股票保存一次进度
            if idx % 10 == 0:
                progress._save_progress()

            # 避免请求过快
            time.sleep(0.3)

        if self.progress_printer:
            self.progress_printer.finish()

        self._show_summary()

        # 保存错误日志
        if error_stocks:
            self._save_error_log(error_stocks, start_date, end_date)

        if all_data:
            return pd.concat(all_data)
        return None

    def _show_summary(self):
        """显示下载摘要"""
        print(f"\n{'='*60}")
        print(f"下载摘要:")
        print(f"  成功: {self.success_count} 只")
        print(f"  跳过: {self.skip_count} 只")
        print(f"  失败: {self.error_count} 只")
        print(f"{'='*60}\n")

    def _save_error_log(self, error_stocks: List[str], start_date: str, end_date: str):
        """保存错误日志"""
        error_log_path = os.path.join(
            os.path.dirname(settings.DATA_ROOT),
            'logs',
            f'download_errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        )

        ensure_dir(os.path.dirname(error_log_path))

        with open(error_log_path, 'w', encoding='utf-8') as f:
            f.write(f"下载错误日志\n")
            f.write(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"日期范围: {start_date} 至 {end_date}\n")
            f.write(f"错误股票数: {len(error_stocks)}\n")
            f.write(f"\n失败股票列表:\n")

            for code in error_stocks:
                f.write(f"  - {code}\n")

        logger.warning(f"错误日志已保存到: {error_log_path}")

    def save_daily_data(self, df: pd.DataFrame, year: Optional[int] = None) -> None:
        """
        保存日线数据到 Parquet，按年分区

        Args:
            df: 日线数据，索引为日期
            year: 指定年份保存，None 则按数据中的年份分别保存
        """
        if df is None or df.empty:
            return

        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        # 重置索引，将日期转为列
        df_save = df.reset_index()
        df_save.rename(columns={'index': 'date'}, inplace=True)

        def save_year_data(df_year: pd.DataFrame, yr: int):
            """保存单年数据"""
            base_path = f"{settings.DATA_ROOT}/market/daily/year={yr}"
            for code, group in df_year.groupby('code'):
                # 保存时移除 code 列（code 作为文件名一部分）
                group_save = group.drop('code', axis=1, errors='ignore')
                file_path = f"{base_path}/stock={code}.parquet"
                ensure_dir(base_path)
                group_save.to_parquet(file_path, compression=settings.COMPRESSION, index=False)
                logger.info(f"✓ {code} daily data for {yr} 保存到 {file_path}")

        if year:
            df_year = df_save[df_save['date'].dt.year == year]
            if not df_year.empty:
                save_year_data(df_year, year)
        else:
            for yr, group in df_save.groupby(df_save['date'].dt.year):
                save_year_data(group, yr)

    def fetch_and_save(self, code: str, start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> bool:
        """
        获取并保存日线数据（便捷方法）

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            是否成功
        """
        df = self.fetch_daily_by_code(code, start_date, end_date)
        if df is not None and not df.empty:
            self.save_daily_data(df)
            return True
        return False

    def reset_progress(self, code: Optional[str] = None) -> None:
        """
        重置下载进度（用于重新下载）

        Args:
            code: 股票代码，None 表示清除所有
        """
        progress = get_daily_download_progress()
        progress.reset_daily_cache(code)
        logger.info(f"已重置 {'所有' if code is None else code} 下载进度")

    def get_progress_summary(self) -> dict:
        """获取下载进度摘要"""
        progress = get_daily_download_progress()
        summary = progress.get_progress_summary()

        # 添加计数器信息
        summary.update({
            'success_count': self.success_count,
            'skip_count': self.skip_count,
            'error_count': self.error_count
        })

        return summary
