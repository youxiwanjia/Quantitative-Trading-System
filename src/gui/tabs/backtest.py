import tkinter as tk
from tkinter import ttk
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

class BacktestTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.create_widgets()

    def create_widgets(self):
        # 控制栏
        control_frame = ttk.Frame(self)
        control_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Button(control_frame, text="运行回测", command=self.run_backtest).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="加载最新结果", command=self.load_backtest_result).pack(side=tk.LEFT, padx=5)

        # 创建左右两部分：左侧表格，右侧图表
        paned = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 左侧绩效指标表格
        left_frame = ttk.Frame(paned)
        paned.add(left_frame, weight=1)

        columns = ('metric', 'value')
        self.tree = ttk.Treeview(left_frame, columns=columns, show='headings', height=10)
        self.tree.heading('metric', text='指标')
        self.tree.heading('value', text='数值')
        self.tree.pack(fill=tk.BOTH, expand=True)

        # 右侧图表
        right_frame = ttk.Frame(paned)
        paned.add(right_frame, weight=2)

        self.figure = plt.Figure(figsize=(6, 4), dpi=100)
        self.ax = self.figure.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.figure, master=right_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # 加载示例数据
        self.load_sample_data()

    def load_sample_data(self):
        # 尝试读取回测结果文件
        try:
            with open('backtest_perf.json', 'r') as f:
                import json
                perf = json.load(f)
                for k, v in perf.items():
                    if isinstance(v, float):
                        self.tree.insert('', 'end', values=(k, f"{v:.4f}"))
        except:
            sample = [('总收益率', '-0.1701'), ('年化收益率', '-0.6400'),
                      ('夏普比率', '-1.9223'), ('最大回撤', '-0.2420'),
                      ('胜率', '0.3913')]
            for item in sample:
                self.tree.insert('', 'end', values=item)

        # 尝试读取净值数据并绘图
        try:
            nav = pd.read_csv('backtest_nav.csv', parse_dates=['date'])
            self.ax.clear()
            self.ax.plot(nav['date'], nav['nav'])
            self.ax.set_title('净值曲线')
            self.ax.set_xlabel('日期')
            self.ax.set_ylabel('净值')
            self.figure.autofmt_xdate()
            self.canvas.draw()
        except:
            pass

    def run_backtest(self):
        # 这里应调用回测模块
        print("运行回测...")
        # 完成后刷新数据

    def load_backtest_result(self):
        # 重新加载最新的结果文件
        self.tree.delete(*self.tree.get_children())
        self.load_sample_data()