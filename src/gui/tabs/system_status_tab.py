import tkinter as tk
from tkinter import ttk
import datetime

class SystemStatusTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.create_widgets()
        self.update_status()

    def create_widgets(self):
        # 状态信息使用LabelFrame分组
        # 数据源状态
        source_frame = ttk.LabelFrame(self, text="数据源状态")
        source_frame.pack(fill=tk.X, padx=10, pady=5)

        self.source_label = ttk.Label(source_frame, text="当前使用: Baostock")
        self.source_label.pack(anchor='w', padx=10, pady=2)

        # 交易日历
        cal_frame = ttk.LabelFrame(self, text="交易日历")
        cal_frame.pack(fill=tk.X, padx=10, pady=5)

        self.cal_label = ttk.Label(cal_frame, text="最新交易日: 2026-03-20")
        self.cal_label.pack(anchor='w', padx=10, pady=2)

        # 数据存储路径
        path_frame = ttk.LabelFrame(self, text="数据存储路径")
        path_frame.pack(fill=tk.X, padx=10, pady=5)

        self.path_label = ttk.Label(path_frame, text="D:/Quantitative Trading System/data/processed")
        self.path_label.pack(anchor='w', padx=10, pady=2)

        # 数据统计
        stats_frame = ttk.LabelFrame(self, text="数据统计")
        stats_frame.pack(fill=tk.X, padx=10, pady=5)

        self.stats_text = tk.Text(stats_frame, height=8, wrap='word')
        self.stats_text.pack(fill=tk.BOTH, padx=10, pady=5)

        # 日志输出框
        log_frame = ttk.LabelFrame(self, text="日志输出")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        self.log_text = tk.Text(log_frame, height=10, wrap='word')
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # 添加一些模拟日志
        self.add_log("系统初始化完成")
        self.add_log("数据加载成功")

    def update_status(self):
        # 更新数据统计
        stats = """
   - 股票总数: 3374
   - 日线数据: 2026-01-01 ~ 2026-03-20 (共45个交易日)
   - 30分钟线: 2026-03-01 ~ 2026-03-20 (部分)
   - 指数数据: 沪深300、上证指数、深证成等
        """
        self.stats_text.delete(1.0, tk.END)
        self.stats_text.insert(1.0, stats)

    def add_log(self, message):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)