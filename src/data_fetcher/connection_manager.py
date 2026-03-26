#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据源连接管理器

统一管理 Baostock 和 Tushare 的连接，提供：
- 单例模式避免重复登录
- 连接池管理
- 自动重连机制
- 连接状态监控
"""

import threading
import time
import logging
from typing import Optional, Callable, Any
from abc import ABC, abstractmethod
from functools import wraps

logger = logging.getLogger(__name__)


class ConnectionError(Exception):
    """连接异常"""
    pass


class BaseConnectionManager(ABC):
    """连接管理器基类"""

    def __init__(self):
        self._connected = False
        self._lock = threading.Lock()
        self._last_error: Optional[str] = None
        self._login_time: Optional[float] = None

    @property
    def is_connected(self) -> bool:
        """是否已连接"""
        return self._connected

    @property
    def last_error(self) -> Optional[str]:
        """最后的错误信息"""
        return self._last_error

    @abstractmethod
    def connect(self) -> bool:
        """建立连接"""
        pass

    @abstractmethod
    def disconnect(self) -> bool:
        """断开连接"""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """测试连接是否有效"""
        pass

    def ensure_connected(self) -> None:
        """确保已连接，未连接则自动连接"""
        if not self._connected:
            with self._lock:
                if not self._connected:
                    if not self.connect():
                        raise ConnectionError(f"连接失败: {self._last_error}")

    def reconnect(self) -> bool:
        """重新连接"""
        self.disconnect()
        return self.connect()


class BaostockManager(BaseConnectionManager):
    """
    Baostock 连接管理器（单例模式）

    使用方式：
        from src.data_fetcher.connection_manager import get_baostock

        bs = get_baostock()
        bs.query_history_k_data_plus(...)

        # 或使用上下文管理器
        with baostock_context() as bs:
            bs.query_history_k_data_plus(...)
    """

    _instance: Optional['BaostockManager'] = None
    _instance_lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return
        super().__init__()
        self._bs = None
        self._initialized = True
        logger.info("BaostockManager 实例已创建")

    def connect(self) -> bool:
        """登录 Baostock"""
        try:
            import baostock as bs
            self._bs = bs

            with self._lock:
                result = bs.login()
                if result.error_code == '0':
                    self._connected = True
                    self._login_time = time.time()
                    self._last_error = None
                    logger.info("Baostock 登录成功")
                    return True
                else:
                    self._connected = False
                    self._last_error = result.error_msg
                    logger.error(f"Baostock 登录失败: {result.error_msg}")
                    return False
        except ImportError:
            self._last_error = "baostock 未安装，请执行: pip install baostock"
            logger.error(self._last_error)
            return False
        except Exception as e:
            self._connected = False
            self._last_error = str(e)
            logger.error(f"Baostock 连接异常: {e}")
            return False

    def disconnect(self) -> bool:
        """登出 Baostock"""
        try:
            if self._bs and self._connected:
                self._bs.logout()
                logger.info("Baostock 已登出")
            self._connected = False
            self._login_time = None
            return True
        except Exception as e:
            logger.error(f"Baostock 登出异常: {e}")
            return False

    def test_connection(self) -> bool:
        """测试连接是否有效"""
        if not self._connected or not self._bs:
            return False
        try:
            # 通过查询一个简单请求测试连接
            rs = self._bs.query_trade_dates(
                start_date=time.strftime('%Y-%m-%d'),
                end_date=time.strftime('%Y-%m-%d')
            )
            return rs.error_code == '0'
        except Exception as e:
            logger.warning(f"Baostock 连接测试失败: {e}")
            return False

    def get_connection(self):
        """获取 baostock 模块引用"""
        self.ensure_connected()
        return self._bs

    def __enter__(self):
        self.ensure_connected()
        return self._bs

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 不自动断开，保持连接复用
        pass


class TushareManager(BaseConnectionManager):
    """
    Tushare 连接管理器（单例模式）

    使用方式：
        from src.data_fetcher.connection_manager import get_tushare

        ts = get_tushare(token='your_token')
        ts.daily(ts_code='000001.SZ', ...)

        # 或先设置环境变量 TUSHARE_TOKEN
        # export TUSHARE_TOKEN=your_token
    """

    _instance: Optional['TushareManager'] = None
    _instance_lock = threading.Lock()

    def __init__(self, token: Optional[str] = None):
        if hasattr(self, '_initialized') and self._initialized:
            return
        super().__init__()
        self._token = token
        self._pro = None
        self._initialized = True
        logger.info("TushareManager 实例已创建")

    def __new__(cls, token: Optional[str] = None):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def set_token(self, token: str) -> None:
        """设置 Token"""
        self._token = token
        self._connected = False  # 重置连接状态

    def connect(self) -> bool:
        """初始化 Tushare Pro API"""
        if not self._token:
            import os
            self._token = os.environ.get('TUSHARE_TOKEN')

        if not self._token:
            self._last_error = "未设置 Tushare Token，请通过 set_token() 或环境变量 TUSHARE_TOKEN 设置"
            logger.error(self._last_error)
            return False

        try:
            import tushare as ts
            ts.set_token(self._token)
            self._pro = ts.pro_api()
            self._connected = True
            self._login_time = time.time()
            self._last_error = None
            logger.info("Tushare 初始化成功")
            return True
        except ImportError:
            self._last_error = "tushare 未安装，请执行: pip install tushare"
            logger.error(self._last_error)
            return False
        except Exception as e:
            self._connected = False
            self._last_error = str(e)
            logger.error(f"Tushare 初始化异常: {e}")
            return False

    def disconnect(self) -> bool:
        """断开 Tushare 连接（实际上是清理状态）"""
        self._pro = None
        self._connected = False
        self._login_time = None
        logger.info("Tushare 连接已断开")
        return True

    def test_connection(self) -> bool:
        """测试连接是否有效"""
        if not self._connected or not self._pro:
            return False
        try:
            # 测试一个简单查询
            df = self._pro.trade_cal(
                exchange='SSE',
                start_date=time.strftime('%Y%m%d'),
                end_date=time.strftime('%Y%m%d')
            )
            return df is not None and not df.empty
        except Exception as e:
            logger.warning(f"Tushare 连接测试失败: {e}")
            return False

    def get_pro(self):
        """获取 Tushare Pro API 实例"""
        self.ensure_connected()
        return self._pro

    def get_connection(self):
        """获取 Tushare Pro API 实例（别名）"""
        return self.get_pro()

    def __enter__(self):
        self.ensure_connected()
        return self._pro

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


# ==================== 全局单例获取函数 ====================

_baostock_manager: Optional[BaostockManager] = None
_tushare_manager: Optional[TushareManager] = None


def get_baostock() -> BaostockManager:
    """
    获取 Baostock 连接管理器单例

    Returns:
        BaostockManager 实例

    Usage:
        bs_manager = get_baostock()
        bs = bs_manager.get_connection()
        rs = bs.query_history_k_data_plus(...)
    """
    global _baostock_manager
    if _baostock_manager is None:
        _baostock_manager = BaostockManager()
    return _baostock_manager


def get_tushare(token: Optional[str] = None) -> TushareManager:
    """
    获取 Tushare 连接管理器单例

    Args:
        token: Tushare Token，可选，也可通过环境变量 TUSHARE_TOKEN 设置

    Returns:
        TushareManager 实例

    Usage:
        ts_manager = get_tushare('your_token')
        pro = ts_manager.get_pro()
        df = pro.daily(ts_code='000001.SZ', start_date='20230101', end_date='20231231')
    """
    global _tushare_manager
    if _tushare_manager is None:
        _tushare_manager = TushareManager(token)
    elif token:
        _tushare_manager.set_token(token)
    return _tushare_manager


# ==================== 上下文管理器快捷方式 ====================

def baostock_context():
    """
    Baostock 上下文管理器

    Usage:
        with baostock_context() as bs:
            rs = bs.query_history_k_data_plus(...)
    """
    return get_baostock()


def tushare_context(token: Optional[str] = None):
    """
    Tushare 上下文管理器

    Usage:
        with tushare_context('your_token') as pro:
            df = pro.daily(ts_code='000001.SZ', ...)
    """
    return get_tushare(token)


# ==================== 装饰器：自动确保连接 ====================

def with_baostock(func: Callable) -> Callable:
    """
    装饰器：自动确保 Baostock 已连接

    Usage:
        @with_baostock
        def fetch_data(code, start, end):
            bs = get_baostock().get_connection()
            return bs.query_history_k_data_plus(...)
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        get_baostock().ensure_connected()
        return func(*args, **kwargs)
    return wrapper


def with_tushare(token: Optional[str] = None) -> Callable:
    """
    装饰器工厂：自动确保 Tushare 已连接

    Usage:
        @with_tushare('your_token')
        def fetch_data(code, start, end):
            pro = get_tushare().get_pro()
            return pro.daily(...)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            get_tushare(token).ensure_connected()
            return func(*args, **kwargs)
        return wrapper
    return decorator


# ==================== 统一数据源接口 ====================

class DataSource:
    """
    统一数据源接口

    提供统一的数据获取方法，底层可切换 Baostock 或 Tushare
    """

    BAOSTOCK = 'baostock'
    TUSHARE = 'tushare'

    def __init__(self, source: str = 'baostock', tushare_token: Optional[str] = None):
        """
        Args:
            source: 数据源，'baostock' 或 'tushare'
            tushare_token: Tushare Token（使用 tushare 时需要）
        """
        self.source = source.lower()
        self._tushare_token = tushare_token

    @property
    def manager(self):
        """获取当前数据源的连接管理器"""
        if self.source == self.BAOSTOCK:
            return get_baostock()
        elif self.source == self.TUSHARE:
            return get_tushare(self._tushare_token)
        else:
            raise ValueError(f"不支持的数据源: {self.source}")

    def switch_source(self, source: str) -> None:
        """切换数据源"""
        self.source = source.lower()
        logger.info(f"数据源已切换为: {self.source}")


# ==================== 测试代码 ====================

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    print("=" * 50)
    print("测试 Baostock 连接管理器")
    print("=" * 50)

    # 测试 Baostock
    bs_mgr = get_baostock()
    if bs_mgr.connect():
        print(f"连接状态: {bs_mgr.is_connected}")
        print(f"连接测试: {bs_mgr.test_connection()}")

        # 使用连接
        bs = bs_mgr.get_connection()
        rs = bs.query_all_stock()
        print(f"获取股票数量: {len(rs.get_data()) if rs.error_code == '0' else '失败'}")

        bs_mgr.disconnect()
    else:
        print(f"连接失败: {bs_mgr.last_error}")

    print("\n" + "=" * 50)
    print("测试 Tushare 连接管理器（需要有效的 Token）")
    print("=" * 50)

    # 测试 Tushare（需要 Token）
    import os
    token = os.environ.get('TUSHARE_TOKEN')
    if token:
        ts_mgr = get_tushare(token)
        if ts_mgr.connect():
            print(f"连接状态: {ts_mgr.is_connected}")
            print(f"连接测试: {ts_mgr.test_connection()}")
            ts_mgr.disconnect()
        else:
            print(f"连接失败: {ts_mgr.last_error}")
    else:
        print("未设置 TUSHARE_TOKEN 环境变量，跳过 Tushare 测试")

    print("\n测试完成")
