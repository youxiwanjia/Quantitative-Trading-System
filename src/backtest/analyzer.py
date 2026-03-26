import pandas as pd
import numpy as np

class PerformanceAnalyzer:
    @staticmethod
    def calculate(nav: pd.Series, benchmark_nav: pd.Series = None, risk_free=0.02):
        """
        nav: 日度净值序列 index为日期
        benchmark_nav: 基准净值序列
        risk_free: 无风险年利率
        """
        # 计算日收益率
        returns = nav.pct_change().dropna()
        if benchmark_nav is not None:
            bench_returns = benchmark_nav.pct_change().dropna()
            # 对齐日期
            common = returns.index.intersection(bench_returns.index)
            returns = returns.loc[common]
            bench_returns = bench_returns.loc[common]
        else:
            bench_returns = None

        # 年化收益率
        total_days = len(returns)
        total_return = (nav.iloc[-1] / nav.iloc[0]) - 1
        annual_return = (1 + total_return) ** (252 / total_days) - 1

        # 年化波动率
        annual_vol = returns.std() * np.sqrt(252)

        # 夏普比率
        sharpe = (annual_return - risk_free) / annual_vol if annual_vol != 0 else np.nan

        # 最大回撤
        cumulative = (1 + returns).cumprod()
        running_max = cumulative.cummax()
        drawdown = (cumulative - running_max) / running_max
        max_drawdown = drawdown.min()
        max_dd_duration = (drawdown == drawdown.min()).argmax() if not drawdown.empty else 0

        # 胜率
        win_rate = (returns > 0).mean()

        # 换手率（平均每日换手）
        # 需要交易记录，此处简化，暂不计算

        result = {
            'total_return': total_return,
            'annual_return': annual_return,
            'annual_volatility': annual_vol,
            'sharpe_ratio': sharpe,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'start_date': nav.index[0],
            'end_date': nav.index[-1]
        }

        # 如果提供了基准，计算超额收益、信息比率等
        if bench_returns is not None:
            excess_returns = returns - bench_returns
            # 跟踪误差
            tracking_error = excess_returns.std() * np.sqrt(252)
            info_ratio = (excess_returns.mean() * 252) / (excess_returns.std() * np.sqrt(252)) if tracking_error != 0 else np.nan
            result['tracking_error'] = tracking_error
            result['info_ratio'] = info_ratio
            # 超额收益序列
            result['excess_returns'] = excess_returns

        return result

    @staticmethod
    def plot(nav, benchmark_nav=None, save_path=None):
        """绘制净值曲线（可选，需安装 matplotlib）"""
        try:
            import matplotlib.pyplot as plt
            plt.figure(figsize=(12,6))
            plt.plot(nav.index, nav.values, label='Strategy')
            if benchmark_nav is not None:
                plt.plot(benchmark_nav.index, benchmark_nav.values, label='Benchmark')
            plt.legend()
            if save_path:
                plt.savefig(save_path)
            plt.show()
        except ImportError:
            print("matplotlib not installed, skipping plot.")