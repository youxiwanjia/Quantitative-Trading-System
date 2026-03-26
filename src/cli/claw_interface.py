#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
claw机器助理调用接口

支持命令：
  update_stock_list         更新股票列表（从AKShare获取）
  fetch_daily               下载日线数据
  fetch-minute-base         下载分钟线数据（5/15/30/60分钟）
  fetch_index               下载指数数据
  select-stocks             执行多因子选股
  run-backtest              运行回测
"""

import sys
import os
import argparse
import logging
import json
import pandas as pd
from datetime import datetime

# 将项目根目录添加到 sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.insert(0, PROJECT_ROOT)

from src.config import settings
from src.data_fetcher import utils
from src.data_fetcher.fetcher_daily import DailyFetcher
from src.data_fetcher.fetcher_minute_base import MinuteBaseFetcher
from src.data_fetcher.fetcher_index import IndexFetcher

# 配置日志
utils.setup_logging()
logger = logging.getLogger(__name__)


# ==================== 命令处理函数 ====================

def cmd_update_stock_list(args):
    """更新股票列表（从AKShare获取实时行情，提取代码和名称）"""
    logger.info("开始更新股票列表...")
    try:
        import akshare as ak
        df = ak.stock_zh_a_spot_em()  # 获取沪深京A股实时行情
        df['code'] = df['代码'].astype(str).str.zfill(6)
        df['name'] = df['名称']
        stock_list = df[['code', 'name']].copy()
        stock_list['list_date'] = None
        stock_list['delist_date'] = None
        stock_list['is_st'] = False
        os.makedirs(os.path.dirname(settings.STOCK_LIST_PATH), exist_ok=True)
        stock_list.to_csv(settings.STOCK_LIST_PATH, index=False, encoding='utf-8')
        logger.info(f"股票列表已更新，共 {len(stock_list)} 只股票，保存至 {settings.STOCK_LIST_PATH}")
        print(json.dumps({"status": "success", "count": len(stock_list)}))
    except Exception as e:
        logger.error(f"更新股票列表失败: {e}")
        print(json.dumps({"status": "error", "message": str(e)}))
        return 1
    return 0


def cmd_fetch_daily(args):
    """下载日线数据"""
    logger.info(f"开始下载日线数据: {args.start} 至 {args.end}, force={args.force}")
    try:
        if args.codes:
            codes = [c.strip() for c in args.codes.split(',')]
        else:
            df_stocks = pd.read_csv(settings.STOCK_LIST_PATH)
            codes = df_stocks['code'].astype(str).tolist()

        fetcher = DailyFetcher()
        for code in codes:
            logger.info(f"处理股票 {code}")
            df = fetcher.fetch_daily_by_code(code, args.start, args.end)
            if df is not None and not df.empty:
                years = df.index.year.unique()
                for year in years:
                    df_year = df[df.index.year == year]
                    fetcher.save_daily_data(df_year, year=int(year))
            else:
                logger.warning(f"股票 {code} 无数据")
        logger.info("日线数据下载完成")
        print(json.dumps({"status": "success"}))
    except Exception as e:
        logger.error(f"下载日线失败: {e}")
        print(json.dumps({"status": "error", "message": str(e)}))
        return 1
    return 0


def cmd_fetch_minute_base(args):
    """下载分钟线数据（支持5/15/30/60分钟），带断点续传和自动重试"""
    logger.info(f"开始下载 {args.freq}分钟数据: {args.codes} 从 {args.start} 至 {args.end}")

    # 自定义重试次数和延迟
    max_retries = args.retries if hasattr(args, 'retries') else 3
    base_delay = args.delay if hasattr(args, 'delay') else 5

    try:
        from src.data_fetcher.fetcher_minute_base import MinuteBaseFetcher

        fetcher = MinuteBaseFetcher(max_retries=max_retries, base_delay=base_delay)

        # 解析股票代码列表
        codes = args.codes.split(',') if args.codes else None
        if codes is None:
            df_stocks = pd.read_csv(settings.STOCK_LIST_PATH)
            codes = df_stocks['code'].astype(str).tolist()

        freq = int(args.freq)
        start_date = args.start
        end_date = args.end

        # 批量下载，使用断点续传功能
        df = fetcher.fetch_multiple_stocks(codes, start_date, end_date, freq, delay=0.5)

        if not df.empty:
            # 最终保存一次，确保数据合并
            fetcher.save_minute_data(df, freq, None, None)
            logger.info(f"成功下载 {len(df)} 条 {args.freq}分钟记录")
        else:
            logger.info("没有新数据需要下载")

        logger.info(f"{args.freq}分钟数据下载完成")
        print(json.dumps({"status": "success"}))
    except Exception as e:
        logger.error(f"下载分钟线失败: {e}")
        print(json.dumps({"status": "error", "message": str(e)}))
        return 1
    return 0


def cmd_fetch_index(args):
    """下载指数数据"""
    logger.info(f"开始下载指数数据: {args.indices} 从 {args.start} 至 {args.end}")
    try:
        indices = args.indices.split(',') if args.indices else ['000300.SH', '000001.SH', '399001.SZ']
        fetcher = IndexFetcher()
        for code in indices:
            df = fetcher.fetch_index_by_code(code.strip(), args.start, args.end)
            if df is not None and not df.empty:
                fetcher.save_index_data(df)
        print(json.dumps({"status": "success"}))
    except Exception as e:
        logger.error(f"下载指数失败: {e}")
        print(json.dumps({"status": "error", "message": str(e)}))
        return 1
    return 0


def cmd_select_stocks(args):
    """执行多因子选股"""
    logger.info(f"开始选股: 日期 {args.date}, 策略 {args.strategy_config}")
    try:
        from src.selector.runner import StockSelector
        selector = StockSelector(args.strategy_config, args.factor_config)
        result = selector.run(args.date)

        output = {
            "date": args.date,
            "stocks": result,
            "count": len(result)
        }
        if args.output:
            pd.DataFrame(result, columns=['code']).to_csv(args.output, index=False, encoding='utf-8')
            logger.info(f"选股结果已保存至 {args.output}")
        print(json.dumps(output, ensure_ascii=False))
    except Exception as e:
        logger.error(f"选股失败: {e}")
        print(json.dumps({"status": "error", "message": str(e)}))
        return 1
    return 0


def cmd_run_backtest(args):
    """运行回测"""
    logger.info(f"开始回测: 配置文件 {args.config}")
    try:
        from src.backtest.engine import BacktestEngine
        engine = BacktestEngine(args.config)
        nav, perf = engine.run()

        # 保存净值
        nav.to_csv(args.output_nav)
        logger.info(f"净值已保存至 {args.output_nav}")

        # 保存绩效指标（过滤掉非序列化的内容）
        perf_serializable = {}
        for k, v in perf.items():
            if isinstance(v, (float, int, str, bool, type(None))):
                perf_serializable[k] = v
            elif isinstance(v, pd.Series):
                # 跳过序列数据，不保存
                pass
            else:
                perf_serializable[k] = str(v)

        with open(args.output_perf, 'w', encoding='utf-8') as f:
            json.dump(perf_serializable, f, indent=2, ensure_ascii=False)
        logger.info(f"绩效指标已保存至 {args.output_perf}")

        # 打印关键指标
        print("\n=== 回测结果 ===")
        for k in ['total_return', 'annual_return', 'sharpe_ratio', 'max_drawdown', 'win_rate']:
            if k in perf:
                print(f"{k}: {perf[k]:.4f}")

        print(json.dumps({"status": "success", "nav_file": args.output_nav, "perf_file": args.output_perf}))
    except Exception as e:
        logger.error(f"回测失败: {e}")
        print(json.dumps({"status": "error", "message": str(e)}))
        return 1
    return 0


# ==================== 主函数 ====================

def main():
    parser = argparse.ArgumentParser(description='量化系统 claw 调用接口')
    subparsers = parser.add_subparsers(dest='command', help='可用命令')

    # 1. 更新股票列表
    subparsers.add_parser('update_stock_list', help='更新股票列表')

    # 2. 抓取日线
    parser_daily = subparsers.add_parser('fetch_daily', help='下载日线数据')
    parser_daily.add_argument('--start', default=settings.START_DATE, help='开始日期 YYYY-MM-DD')
    parser_daily.add_argument('--end', default=datetime.today().strftime('%Y-%m-%d'), help='结束日期 YYYY-MM-DD')
    parser_daily.add_argument('--codes', help='股票代码列表，逗号分隔，不指定则使用全部')
    parser_daily.add_argument('--force', action='store_true', help='强制重新下载')

    # 3. 抓取分钟线
    parser_min = subparsers.add_parser('fetch-minute-base', help='下载分钟线数据（5/15/30/60分钟），支持断点续传')
    parser_min.add_argument('--freq', choices=['5', '15', '30', '60'], required=True, help='分钟频率')
    parser_min.add_argument('--codes', help='股票代码列表，逗号分隔，不指定则使用全部')
    parser_min.add_argument('--start', required=True, help='开始日期 YYYY-MM-DD')
    parser_min.add_argument('--end', required=True, help='结束日期 YYYY-MM-DD')
    parser_min.add_argument('--retries', type=int, default=3, help='最大重试次数（默认3次）')
    parser_min.add_argument('--delay', type=float, default=5, help='基础重试延迟秒数（默认5秒）')

    # 4. 抓取指数
    parser_index = subparsers.add_parser('fetch_index', help='下载指数数据')
    parser_index.add_argument('--indices', help='指数代码列表，逗号分隔，默认常用指数')
    parser_index.add_argument('--start', default='2005-01-01', help='开始日期 YYYY-MM-DD')
    parser_index.add_argument('--end', default=datetime.today().strftime('%Y-%m-%d'), help='结束日期 YYYY-MM-DD')

    # 5. 执行选股
    parser_select = subparsers.add_parser('select-stocks', help='执行多因子选股')
    parser_select.add_argument('--date', default=datetime.today().strftime('%Y-%m-%d'), help='选股日期')
    parser_select.add_argument('--strategy-config', default='configs/strategy.yaml', help='策略配置文件路径')
    parser_select.add_argument('--factor-config', default='configs/factors.yaml', help='因子定义文件路径')
    parser_select.add_argument('--output', help='输出CSV文件路径')

    # 6. 运行回测
    parser_backtest = subparsers.add_parser('run-backtest', help='运行回测')
    parser_backtest.add_argument('--config', default='configs/backtest.yaml', help='回测配置文件路径')
    parser_backtest.add_argument('--output-nav', default='backtest_nav.csv', help='输出净值文件')
    parser_backtest.add_argument('--output-perf', default='backtest_perf.json', help='输出绩效文件')

    args = parser.parse_args()

    if args.command == 'update_stock_list':
        return cmd_update_stock_list(args)
    elif args.command == 'fetch_daily':
        return cmd_fetch_daily(args)
    elif args.command == 'fetch-minute-base':
        return cmd_fetch_minute_base(args)
    elif args.command == 'fetch_index':
        return cmd_fetch_index(args)
    elif args.command == 'select-stocks':
        return cmd_select_stocks(args)
    elif args.command == 'run-backtest':
        return cmd_run_backtest(args)
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())