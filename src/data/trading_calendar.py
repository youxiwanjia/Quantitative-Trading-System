#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
交易日历模块
负责管理沪深A股交易日历，自动从Baostock更新。
"""

import os
import pandas as pd
import baostock as bs
from datetime import datetime, date
from typing import List, Union, Optional

from src.config import settings
from src.data_fetcher.utils import setup_logger

logger = setup_logger('trading_calendar')


class TradingCalendar:
    """交易日历类，自动更新"""

    def __init__(self, market: str = '沪深A股'):
        self.market = market
        # 缓存文件路径：data/processed/metadata/trading_days.csv
        self.cache_path = os.path.join(settings.DATA_ROOT, 'metadata', 'trading_days.csv')
        self._df = None  # 延迟加载

    def _ensure_cache_dir(self):
        """确保缓存目录存在"""
        os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)

    def _load_cache(self) -> pd.DataFrame:
        """加载本地缓存，如果没有则创建空DataFrame"""
        if self._df is not None:
            return self._df
        if os.path.exists(self.cache_path):
            self._df = pd.read_csv(self.cache_path, dtype={'date': str})
            self._df['date'] = pd.to_datetime(self._df['date']).dt.date
        else:
            self._df = pd.DataFrame(columns=['date'])
        return self._df

    def _save_cache(self):
        """保存缓存到文件"""
        if self._df is not None and not self._df.empty:
            # 去重并按日期排序
            self._df = self._df.drop_duplicates().sort_values('date').reset_index(drop=True)
            self._df.to_csv(self.cache_path, index=False, encoding='utf-8')
            logger.info(f"交易日历已保存至 {self.cache_path}，共 {len(self._df)} 天")

    def _fetch_year(self, year: int) -> List[date]:
        """
        从Baostock获取指定年份的交易日列表
        """
        try:
            # 登录（如果未登录）
            lg = bs.login()
            if lg.error_code != '0':
                logger.error(f"Baostock登录失败: {lg.error_msg}")
                return []

            # 查询交易日
            start = f"{year}-01-01"
            end = f"{year}-12-31"
            rs = bs.query_trade_dates(start_date=start, end_date=end)
            if rs.error_code != '0':
                logger.error(f"查询交易日失败: {rs.error_msg}")
                return []

            dates = []
            while (rs.error_code == '0') and rs.next():
                row = rs.get_row_data()
                # row格式: ['日期', '是否交易日(1:是,0:否)']
                if row[1] == '1':
                    dates.append(datetime.strptime(row[0], '%Y-%m-%d').date())
            return dates
        except Exception as e:
            logger.error(f"获取{year}年交易日失败: {e}")
            return []
        finally:
            bs.logout()

    def _update_if_needed(self):
        """检查并更新交易日历到最新"""
        self._ensure_cache_dir()
        df = self._load_cache()
        if df.empty:
            # 首次运行，从2000年开始获取
            start_year = 2000
            current_year = datetime.now().year
            all_dates = []
            for y in range(start_year, current_year + 1):
                year_dates = self._fetch_year(y)
                all_dates.extend(year_dates)
                logger.info(f"已获取{y}年 {len(year_dates)} 个交易日")
            self._df = pd.DataFrame({'date': all_dates})
            self._save_cache()
            return

        # 检查是否需要更新（看是否已有今天的日期）
        today = date.today()
        last_date = df['date'].max()
        if last_date >= today:
            # 已包含今天，无需更新
            return

        # 需要从 last_date 的次年到现在
        last_year = last_date.year
        current_year = today.year
        for y in range(last_year, current_year + 1):
            # 避免重复获取 last_year 全部，可以只获取 last_year 中大于 last_date 的部分
            # 但为了简单，直接获取整年，然后去重
            year_dates = self._fetch_year(y)
            if year_dates:
                new_dates = [d for d in year_dates if d > last_date]
                if new_dates:
                    # 追加
                    new_df = pd.DataFrame({'date': new_dates})
                    self._df = pd.concat([self._df, new_df], ignore_index=True)
                    self._df = self._df.drop_duplicates().sort_values('date').reset_index(drop=True)
                    self._save_cache()
                    last_date = self._df['date'].max()
                else:
                    # 没有新日期，可能是当年还没交易日
                    pass

    def is_trading_day(self, date_input: Union[str, datetime, date]) -> bool:
        """判断给定日期是否为交易日"""
        self._update_if_needed()
        if isinstance(date_input, str):
            d = datetime.strptime(date_input, '%Y-%m-%d').date()
        elif isinstance(date_input, datetime):
            d = date_input.date()
        else:
            d = date_input
        return d in set(self._df['date'])

    def get_trading_days(self, start: Union[str, datetime, date],
                         end: Union[str, datetime, date]) -> List[date]:
        """获取指定区间内的交易日列表（含起止）"""
        self._update_if_needed()
        if isinstance(start, str):
            start = datetime.strptime(start, '%Y-%m-%d').date()
        elif isinstance(start, datetime):
            start = start.date()
        if isinstance(end, str):
            end = datetime.strptime(end, '%Y-%m-%d').date()
        elif isinstance(end, datetime):
            end = end.date()
        mask = (self._df['date'] >= start) & (self._df['date'] <= end)
        return self._df.loc[mask, 'date'].tolist()

    def get_all_trading_days(self) -> List[date]:
        """获取所有交易日（谨慎使用，可能数据量较大）"""
        self._update_if_needed()
        return self._df['date'].tolist()


# 单例实例（可选，方便全局使用）
_calendar_instance = None


def get_calendar() -> TradingCalendar:
    """获取交易日历单例"""
    global _calendar_instance
    if _calendar_instance is None:
        _calendar_instance = TradingCalendar()
    return _calendar_instance


if __name__ == '__main__':
    # 简单测试
    cal = TradingCalendar()
    print("今日是否交易日:", cal.is_trading_day('2026-03-19'))
    print("最近5个交易日:", cal.get_trading_days('2026-03-01', '2026-03-19')[-5:])