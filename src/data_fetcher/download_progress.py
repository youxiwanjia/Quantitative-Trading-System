#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
下载进度管理和断点续传

功能：
- 记录已下载的股票数据（日期范围）
- 检测重复下载
- 支持增量更新
- 防止数据覆盖
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import Set, Dict, Optional, Tuple
import logging

from src.config import settings

logger = logging.getLogger(__name__)


class DownloadProgress:
    """下载进度管理器"""

    def __init__(self, data_root: Optional[str] = None):
        """
        Args:
            data_root: 数据根目录，默认使用 settings.DATA_ROOT
        """
        self.data_root = data_root or settings.DATA_ROOT
        self.progress_file = os.path.join(
            os.path.dirname(self.data_root),
            'download_progress.json'
        )
        self.progress_data: Dict[str, Dict[str, str]] = {}  # {code: {'start': 'YYYY-MM-DD', 'end': 'YYYY-MM-DD'}}

        self._load_progress()

    def _load_progress(self) -> None:
        """从文件加载下载进度"""
        if not os.path.exists(self.progress_file):
            logger.info(f"下载进度文件不存在，创建新文件: {self.progress_file}")
            self.progress_data = {}
            return

        try:
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                self.progress_data = json.load(f)
            logger.info(f"加载了 {len(self.progress_data)} 只股票的下载进度")
        except Exception as e:
            logger.error(f"加载下载进度失败: {e}，将重新创建进度文件")
            self.progress_data = {}

    def _save_progress(self) -> None:
        """保存下载进度到文件"""
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, indent=2, ensure_ascii=False)
            logger.debug(f"下载进度已保存到 {self.progress_file}")
        except Exception as e:
            logger.error(f"保存下载进度失败: {e}")

    def get_downloaded_ranges(self, code: str) -> Optional[Dict[str, str]]:
        """
        获取某只股票的已下载日期范围

        Args:
            code: 股票代码（格式如 'sh.600036' 或 'sz.000001'）

        Returns:
            {'start': 'YYYY-MM-DD', 'end': 'YYYY-MM-DD'} 或 None（未下载）
        """
        return self.progress_data.get(code)

    def is_already_downloaded(self, code: str, start_date: str, end_date: str) -> bool:
        """
        检查某日期范围是否已下载

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            True 如果已下载，False 否则
        """
        existing = self.get_downloaded_ranges(code)
        if existing is None:
            return False

        existing_start = existing.get('start')
        existing_end = existing.get('end')

        # 检查日期范围是否有重叠
        if existing_start and existing_end:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
            existing_start_dt = datetime.strptime(existing_start, '%Y-%m-%d').date()
            existing_end_dt = datetime.strptime(existing_end, '%Y-%m-%d').date()

            # 有重叠
            if not (end_dt < existing_start_dt or start_dt > existing_end_dt):
                logger.info(f"股票 {code} 的日期范围 {start_date} 至 {end_date} 已下载过")
                return True

        return False

    def mark_as_downloaded(self, code: str, start_date: str, end_date: str) -> None:
        """
        标记为已下载

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        """
        self.progress_data[code] = {
            'start': start_date,
            'end': end_date,
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        self._save_progress()
        logger.info(f"标记 {code} 为已下载: {start_date} 至 {end_date}")

    def update_progress(self, code: str, downloaded_start: str, downloaded_end: str) -> None:
        """
        更新进度（增量更新时使用）

        Args:
            code: 股票代码
            downloaded_start: 实际下载的开始日期
            downloaded_end: 实际下载的结束日期
        """
        existing = self.get_downloaded_ranges(code)

        if existing is None:
            # 首次下载，记录完整范围
            self.mark_as_downloaded(code, downloaded_start, downloaded_end)
        else:
            # 增量更新，更新日期范围
            start_dt = datetime.strptime(downloaded_start, '%Y-%m-%d').date()
            end_dt = datetime.strptime(downloaded_end, '%Y-%m-%d').date()
            existing_start_dt = datetime.strptime(existing['start'], '%Y-%m-%d').date()
            existing_end_dt = datetime.strptime(existing['end'], '%Y-%m-%d').date()

            # 更新为最新范围
            new_start = downloaded_start
            new_end = downloaded_end

            # 如果下载范围比之前大，需要扩展
            if start_dt < existing_start_dt:
                new_start = existing['start']
            if end_dt > existing_end_dt:
                new_end = downloaded_end

            self.progress_data[code] = {
                'start': new_start,
                'end': new_end,
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            self._save_progress()
            logger.info(f"更新 {code} 进度: {new_start} 至 {new_end}")

    def get_progress_summary(self) -> Dict:
        """获取下载进度摘要"""
        total_stocks = len(self.progress_data)
        return {
            'total_stocks': total_stocks,
            'progress_file': self.progress_file
        }

    def clear_progress(self, code: Optional[str] = None) -> None:
        """
        清除进度（用于重新下载）

        Args:
            code: 股票代码，None 表示清除所有
        """
        if code:
            if code in self.progress_data:
                del self.progress_data[code]
                logger.info(f"已清除 {code} 的下载进度")
        else:
            self.progress_data = {}
            logger.info("已清除所有下载进度")

        self._save_progress()

    def get_stocks_to_download(self, all_stocks: list[str], start_date: str, end_date: str) -> list[str]:
        """
        获取需要下载的股票列表（跳过已下载的）

        Args:
            all_stocks: 所有股票代码列表
            start_date: 目标开始日期
            end_date: 目标结束日期

        Returns:
            需要下载的股票列表
        """
        to_download = []
        skipped = []

        for code in all_stocks:
            if not self.is_already_downloaded(code, start_date, end_date):
                to_download.append(code)
            else:
                skipped.append(code)

        if skipped:
            logger.info(f"跳过已下载的股票: {len(skipped)} 只")

        return to_download

    def reset_all_progress(self) -> None:
        """重置所有下载进度（用于重新开始）"""
        if confirm_dialog("确定要重置所有下载进度吗？这将重新下载所有股票数据。"):
            self.clear_progress()
            logger.warning("所有下载进度已重置")


class DailyDownloadProgress(DownloadProgress):
    """日线下载进度管理器（特殊化）"""

    def __init__(self, data_root: Optional[str] = None):
        super().__init__(data_root)
        self.daily_cache_file = os.path.join(
            os.path.dirname(self.data_root),
            'metadata',
            'downloaded_dates.json'
        )
        self.daily_downloaded_dates: Set[str] = set()
        self._load_daily_cache()

    def _load_daily_cache(self) -> None:
        """加载已下载的日线日期缓存"""
        if not os.path.exists(self.daily_cache_file):
            logger.info(f"日线下载缓存文件不存在: {self.daily_cache_file}")
            self.daily_downloaded_dates = set()
            return

        try:
            with open(self.daily_cache_file, 'r', encoding='utf-8') as f:
                self.daily_downloaded_dates = set(json.load(f))
            logger.info(f"加载了 {len(self.daily_downloaded_dates)} 个已下载的日期")
        except Exception as e:
            logger.error(f"加载日线下载缓存失败: {e}")
            self.daily_downloaded_dates = set()

    def _save_daily_cache(self) -> None:
        """保存日线下载缓存"""
        try:
            with open(self.daily_cache_file, 'w', encoding='utf-8') as f:
                json.dump(list(self.daily_downloaded_dates), f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"保存日线下载缓存失败: {e}")

    def add_downloaded_date(self, code: str, date: str) -> None:
        """添加已下载的日期"""
        self.daily_downloaded_dates.add(f"{code}_{date}")
        self._save_daily_cache()

    def add_downloaded_dates_batch(self, code: str, dates: list[str]) -> None:
        """批量添加已下载的日期"""
        self.daily_downloaded_dates.update([f"{code}_{d}" for d in dates])
        self._save_daily_cache()

    def is_date_downloaded(self, code: str, date: str) -> bool:
        """检查某天的数据是否已下载"""
        return f"{code}_{date}" in self.daily_downloaded_dates

    def get_downloaded_dates(self, code: str) -> list[str]:
        """获取某股票已下载的日期列表"""
        return [d.split('_')[1] for d in self.daily_downloaded_dates if d.startswith(f"{code}_")]

    def reset_daily_cache(self, code: Optional[str] = None) -> None:
        """重置日线缓存"""
        if code:
            to_remove = [d for d in self.daily_downloaded_dates if d.startswith(f"{code}_")]
            if to_remove:
                for d in to_remove:
                    self.daily_downloaded_dates.remove(d)
                logger.info(f"已清除 {len(to_remove)} 个日期的缓存")
        else:
            self.daily_downloaded_dates = set()
            logger.info("已清除所有日线下载缓存")

        self._save_daily_cache()


def confirm_dialog(message: str) -> bool:
    """确认对话框（简化版，实际应用中可用 GUI）"""
    import sys
    try:
        # 尝试显示确认对话框
        import tkinter as tk
        from tkinter import messagebox

        root = tk.Tk()
        root.withdraw()  # 隐藏主窗口
        result = messagebox.askyesno("确认", message)
        root.destroy()
        return result
    except Exception:
        # 降级到命令行
        print(f"\n{message}")
        return input("确认下载吗？(y/n): ").strip().lower() == 'y'


# 全局单例实例
_daily_download_progress: Optional[DailyDownloadProgress] = None


def get_daily_download_progress(data_root: Optional[str] = None) -> DailyDownloadProgress:
    """
    获取日线下载进度单例

    Args:
        data_root: 数据根目录，默认使用配置中的设置

    Returns:
        DailyDownloadProgress 实例
    """
    global _daily_download_progress
    if _daily_download_progress is None:
        _daily_download_progress = DailyDownloadProgress(data_root)
    return _daily_download_progress


# 用于命令行工具的进度显示
class ProgressPrinter:
    """进度打印器"""

    def __init__(self, total: int, name: str = "下载"):
        self.total = total
        self.current = 0
        self.name = name
        self.start_time = datetime.now()

    def update(self, count: int = 1):
        """更新进度"""
        self.current += count
        percent = (self.current / self.total) * 100
        elapsed = datetime.now() - self.start_time
        speed = self.current / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0

        # 计算剩余时间
        remaining = (self.total - self.current) / speed if speed > 0 else 0
        remaining_str = f"{remaining:.1f}秒" if remaining < 3600 else f"{remaining/60:.1f}分钟"

        bar_length = 40
        filled = int(bar_length * self.current / self.total)
        bar = '█' * filled + '░' * (bar_length - filled)

        print(f"\r[{bar}] {percent:.1f}% ({self.current}/{self.total}) {self.name} | "
              f"速度: {speed:.1f}/s | 剩余: {remaining_str}", end='', flush=True)

    def finish(self):
        """完成"""
        elapsed = datetime.now() - self.start_time
        print(f"\n✓ {self.name} 完成！耗时: {elapsed}")

    def error(self, message: str):
        """出错"""
        print(f"\n✗ {self.name} 失败: {message}")
