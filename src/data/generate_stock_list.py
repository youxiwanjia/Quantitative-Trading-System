#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票列表生成器（Baostock）
使用 query_all_stock 获取最新交易日的主板股票列表
保存为带市场前缀的格式（如 sh.600036）
"""

import baostock as bs
import pandas as pd
import os
import sys
from datetime import datetime, date

# 添加项目根目录到路径（如果作为独立脚本运行）
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.config import settings
from src.data.trading_calendar import get_calendar


def generate_stock_list():
    # 登录
    lg = bs.login()
    print(f"登录结果: error_code={lg.error_code}, error_msg={lg.error_msg}")

    # 获取最近一个交易日的日期
    cal = get_calendar()
    today = date.today()
    # 获取从今年年初到今天的所有交易日，取最后一个作为最近交易日
    start = date(today.year - 1, 1, 1)  # 取近一年确保有数据
    trading_days = cal.get_trading_days(start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
    if not trading_days:
        # 如果无交易日，降级使用固定日期
        latest_trade_date = "2024-01-02"
        print(f"交易日历无数据，使用固定日期: {latest_trade_date}")
    else:
        latest_trade_date = trading_days[-1].strftime("%Y-%m-%d")
        print(f"使用最新交易日: {latest_trade_date}")

    # 获取该交易日全部股票列表
    rs = bs.query_all_stock(latest_trade_date)
    if rs.error_code != '0':
        print(f"查询失败: {rs.error_msg}")
        bs.logout()
        return

    data_list = []
    while (rs.error_code == '0') and rs.next():
        data_list.append(rs.get_row_data())

    if not data_list:
        print("未获取到股票数据")
        bs.logout()
        return

    df = pd.DataFrame(data_list, columns=rs.fields)
    print(f"获取到 {len(df)} 只股票")

    # 主板代码前缀（带市场标识）
    mainboard_prefixes = ['sh.600', 'sh.601', 'sh.603', 'sh.605', 'sz.000', 'sz.001', 'sz.002']
    df['is_mainboard'] = df['code'].str.startswith(tuple(mainboard_prefixes))

    mainboard_df = df[df['is_mainboard']].copy()

    # 选择所需列
    result = mainboard_df[['code', 'code_name', 'tradeStatus']].copy()
    result.rename(columns={
        'code_name': 'name',
        'tradeStatus': 'trade_status'
    }, inplace=True)
    # 添加必要的字段（list_date/delist_date 暂时未知，后续可忽略日期过滤）
    result['list_date'] = None
    result['delist_date'] = None
    # 根据 tradeStatus 判断是否为 ST（简单判断）
    result['is_st'] = result['trade_status'].str.contains('ST', case=False, na=False)

    # 保存到 metadata
    os.makedirs(os.path.dirname(settings.STOCK_LIST_PATH), exist_ok=True)
    result[['code', 'name', 'list_date', 'delist_date', 'is_st']].to_csv(
        settings.STOCK_LIST_PATH, index=False, encoding='utf-8'
    )
    print(f"主板股票列表已保存至 {settings.STOCK_LIST_PATH}，共 {len(result)} 只")
    print("示例股票代码：")
    print(result[['code', 'name']].head(10))

    bs.logout()


if __name__ == '__main__':
    generate_stock_list()