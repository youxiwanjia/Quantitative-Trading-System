import pandas as pd
from datetime import datetime
import yaml

from src.data.loader import DataLoader
from src.factors.engine import FactorEngine
from src.strategies.loader import StrategyLoader
from .simulator import Simulator
from .analyzer import PerformanceAnalyzer
from src.data.trading_calendar import get_calendar
from src.data_fetcher.fetcher_index import IndexFetcher
import logging
logger = logging.getLogger(__name__)


class BacktestEngine:
    def __init__(self, config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        self.data_loader = DataLoader()
        # 读取 factor_config 并传递给 FactorEngine
        factor_config_path = self.config.get('factor_config', 'configs/factors.yaml')
        self.factor_engine = FactorEngine(factor_config_path, self.data_loader)
        self.simulator = Simulator(self.config['backtest'])

        # 加载策略（支持多策略）
        self.strategies = []
        for strat_cfg in self.config['strategies']:
            strategy = StrategyLoader.load(strat_cfg['config'])
            self.strategies.append({
                'name': strat_cfg['name'],
                'strategy': strategy,
                'weight': strat_cfg.get('weight', 1.0)
            })

    def run(self):
        start = pd.to_datetime(self.config['backtest']['start_date'])
        end = pd.to_datetime(self.config['backtest']['end_date'])

        # 获取基准指数净值
        benchmark_nav = self._get_benchmark_nav(start, end)

        # 生成所有日期（包括周末）
        cal = get_calendar()
        trading_days = cal.get_trading_days(start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
        for date_str in trading_days:
            date = pd.to_datetime(date_str).date()
            # 其余代码不变（仍需获取股票池等）

            # 获取当日股票池
            universe = self.data_loader.get_stock_universe(date)
            if universe.empty:
                continue

            # 使用第一个策略生成选股信号
            strategy = self.strategies[0]['strategy']
            target_codes = strategy.generate_signals(date, self.data_loader, self.factor_engine)

            # 获取当日所有股票的价格
            stocks = universe['code'].tolist()
            daily_df = self.data_loader.load_daily(stocks, date, date, fields=['close'])
            if daily_df.empty:
                continue

            # 转换为价格字典：{code: price}
            # 方法一：使用 groupby（推荐）
            prices = daily_df['close'].groupby(level='code').first().to_dict()

            # 执行调仓
            self.simulator.execute_orders(date, target_codes, prices, self.config['trading'])

        # 获取净值序列
        nav = self.simulator.get_daily_nav()

        # 计算绩效
        perf = PerformanceAnalyzer.calculate(nav['nav'], benchmark_nav)
        pd.DataFrame(self.simulator.trades).to_csv('trades.csv', index=False)
        return nav, perf

    def _get_benchmark_nav(self, start, end):
        """获取基准指数净值，若本地无数据则自动下载"""
        code = self.config['benchmark']['code']
        try:
            # 尝试加载本地数据
            df = self.data_loader.load_daily([code], start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'), fields=['close'])
            if df.empty:
                # 本地无数据，自动下载
                logger.info(f"本地无基准数据 {code}，尝试自动下载...")
                fetcher = IndexFetcher()
                df_index = fetcher.fetch_index_by_code(code, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
                if df_index is not None and not df_index.empty:
                    fetcher.save_index_data(df_index)
                    # 重新加载
                    df = self.data_loader.load_daily([code], start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'), fields=['close'])
                else:
                    logger.warning(f"下载基准 {code} 失败")
                    return None
            nav = df['close'].unstack('code')[code]
            nav = nav / nav.iloc[0]
            return nav
        except Exception as e:
            logger.error(f"获取基准净值失败: {e}")
            return None