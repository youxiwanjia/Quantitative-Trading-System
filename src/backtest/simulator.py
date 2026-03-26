import pandas as pd
import numpy as np

class Simulator:
    """模拟交易执行器，正确保留未卖出股票市值"""

    def __init__(self, config):
        self.initial_cash = config['initial_cash']
        self.cash = self.initial_cash
        self.positions = {}          # {code: {'quantity': int, 'cost': float}}
        self.trades = []              # 记录每笔交易
        self.daily_value = []         # 每日总资产

    def get_position_value(self, prices):
        """计算当前持仓市值（按给定价格）"""
        value = 0
        for code, pos in self.positions.items():
            if code in prices:
                value += pos['quantity'] * prices[code]
        return value

    def execute_orders(self, date, target_codes, current_prices, config):
        """
        执行调仓：
        1. 卖出不在目标列表的股票
        2. 对目标列表中的股票，按等权重目标市值调整持仓（只买不卖多余部分）
        3. 记录每日净值
        """
        commission_rate = config['commission']
        stamp_tax_rate = config['stamp_tax']
        slippage = config['slippage']
        min_comm = config['min_commission']

        # 计算当前总资产（现金 + 持仓市值）
        current_value = self.get_position_value(current_prices)
        total_asset = self.cash + current_value

        # 目标持仓：等权重分配
        n_target = len(target_codes)
        if n_target == 0:
            target_per_stock = 0
        else:
            target_per_stock = total_asset / n_target

        # 第一步：卖出不在目标列表中的股票
        for code in list(self.positions.keys()):
            if code not in target_codes:
                pos = self.positions[code]
                price = current_prices[code] * (1 - slippage)  # 卖出滑点
                quantity = pos['quantity']
                turnover = quantity * price
                commission = max(turnover * commission_rate, min_comm)
                stamp_tax = turnover * stamp_tax_rate
                net_cash = turnover - commission - stamp_tax
                self.cash += net_cash
                self.trades.append({
                    'date': date,
                    'code': code,
                    'action': 'sell',
                    'price': price,
                    'quantity': quantity,
                    'turnover': turnover,
                    'commission': commission,
                    'stamp_tax': stamp_tax
                })
                del self.positions[code]

        # 第二步：买入目标列表中的股票（按目标市值调整）
        for code in target_codes:
            # 计算当前持有市值
            current_qty = self.positions.get(code, {}).get('quantity', 0)
            current_price = current_prices[code]
            current_hold_value = current_qty * current_price

            # 需要达到的目标市值
            target_value = target_per_stock
            need_value = target_value - current_hold_value

            if need_value <= 0:
                # 当前持有已经达到或超过目标，不买入
                continue

            # 计算需要买入的数量
            buy_price = current_price * (1 + slippage)  # 买入滑点
            buy_quantity = int(need_value / buy_price)
            if buy_quantity <= 0:
                continue

            # 检查现金是否足够
            max_quantity = int(self.cash / buy_price)
            if buy_quantity > max_quantity:
                buy_quantity = max_quantity
            if buy_quantity == 0:
                continue

            turnover = buy_quantity * buy_price
            commission = max(turnover * commission_rate, min_comm)
            total_cost = turnover + commission

            if total_cost > self.cash:
                # 重新计算可买数量
                buy_quantity = int(self.cash / (buy_price * (1 + commission_rate)))
                if buy_quantity == 0:
                    continue
                turnover = buy_quantity * buy_price
                commission = max(turnover * commission_rate, min_comm)
                total_cost = turnover + commission

            # 执行买入
            self.cash -= total_cost
            if code in self.positions:
                # 累加持仓，更新加权平均成本
                old_qty = self.positions[code]['quantity']
                old_cost = self.positions[code]['cost']
                new_qty = old_qty + buy_quantity
                new_cost = (old_qty * old_cost + turnover) / new_qty
                self.positions[code] = {'quantity': new_qty, 'cost': new_cost}
            else:
                self.positions[code] = {'quantity': buy_quantity, 'cost': buy_price}

            self.trades.append({
                'date': date,
                'code': code,
                'action': 'buy',
                'price': buy_price,
                'quantity': buy_quantity,
                'turnover': turnover,
                'commission': commission,
                'stamp_tax': 0
            })

        # 记录当日总资产（现金 + 所有持仓市值）
        new_value = self.cash + self.get_position_value(current_prices)
        self.daily_value.append((date, new_value))
        return self.cash, self.positions

    def get_daily_nav(self):
        """返回 DataFrame: date, nav"""
        df = pd.DataFrame(self.daily_value, columns=['date', 'nav'])
        df.set_index('date', inplace=True)
        return df