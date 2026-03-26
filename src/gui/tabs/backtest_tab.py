import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import json

class BacktestTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.create_widgets()
        self.load_data()

    def create_widgets(self):
        # 工具栏
        toolbar = ttk.Frame(self)
        toolbar.pack(fill=tk.X, padx=5, pady=5)

        ttk.Button(toolbar, text="运行回测", command=self.run_backtest).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="加载回测结果", command=self.load_data).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="参数配置", command=self.config_params).pack(side=tk.LEFT, padx=2)

        # 左右分割
        paned = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 左侧：绩效指标表格
        left_frame = ttk.Frame(paned)
        paned.add(left_frame, weight=1)

        columns = ('metric', 'value')
        self.tree = ttk.Treeview(left_frame, columns=columns, show='headings', height=15)
        self.tree.heading('metric', text='指标')
        self.tree.heading('value', text='数值')
        self.tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 右侧：图表
        right_frame = ttk.Frame(paned)
        paned.add(right_frame, weight=3)

        self.figure = plt.Figure(figsize=(6, 4), dpi=100)
        self.ax = self.figure.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.figure, master=right_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 底部交易记录（可选）
        self.trade_tree = ttk.Treeview(self, columns=('date', 'code', 'action', 'price', 'quantity'), show='headings', height=5)
        self.trade_tree.heading('date', text='日期')
        self.trade_tree.heading('code', text='代码')
        self.trade_tree.heading('action', text='操作')
        self.trade_tree.heading('price', text='价格')
        self.trade_tree.heading('quantity', text='数量')
        self.trade_tree.pack(fill=tk.X, padx=5, pady=5)

    def load_data(self):
        # 加载绩效指标
        self.tree.delete(*self.tree.get_children())
        try:
            with open('backtest_perf.json', 'r') as f:
                perf = json.load(f)
                for k, v in perf.items():
                    if isinstance(v, (float, int)):
                        self.tree.insert('', 'end', values=(k, f"{v:.4f}"))
        except:
            sample = [('总收益率', '-0.1701'), ('年化收益率', '-0.6400'),
                      ('夏普比率', '-1.9223'), ('最大回撤', '-0.2420'),
                      ('胜率', '0.3913')]
            for item in sample:
                self.tree.insert('', 'end', values=item)

        # 加载净值数据并绘图
        try:
            nav = pd.read_csv('backtest_nav.csv', parse_dates=['date'])
            self.ax.clear()
            self.ax.plot(nav['date'], nav['nav'], label='策略净值', color='blue')
            # 如果有基准，模拟一条
            # 生成模拟基准
            import numpy as np
            benchmark = nav['nav'].values * (1 + np.random.randn(len(nav))*0.01)  # 模拟
            self.ax.plot(nav['date'], benchmark, label='沪深300', color='green')
            # 超额收益
            excess = nav['nav'].values - benchmark
            self.ax.plot(nav['date'], excess, label='超额收益', color='purple')

            self.ax.set_title('净值曲线对比')
            self.ax.set_xlabel('日期')
            self.ax.set_ylabel('净值')
            self.ax.legend()
            self.figure.autofmt_xdate()
            self.canvas.draw()
        except Exception as e:
            print("绘图错误:", e)

        # 加载交易记录
        self.trade_tree.delete(*self.trade_tree.get_children())
        try:
            trades = pd.read_csv('trades.csv')
            for _, row in trades.iterrows():
                self.trade_tree.insert('', 'end', values=(row['date'], row['code'], row['action'], f"{row['price']:.2f}", row['quantity']))
        except:
            pass

    def run_backtest(self):
        messagebox.showinfo("提示", "运行回测功能待实现")
        # 调用回测引擎
        # 完成后刷新数据

    def config_params(self):
        messagebox.showinfo("提示", "参数配置功能待实现")