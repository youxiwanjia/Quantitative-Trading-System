import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
from datetime import datetime

class StockListTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.create_widgets()
        self.refresh_data()

    def create_widgets(self):
        # 顶部工具栏
        toolbar = ttk.Frame(self)
        toolbar.pack(fill=tk.X, padx=5, pady=5)

        ttk.Button(toolbar, text="更新实时行情", command=self.update_realtime).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="抓取历史行情", command=self.fetch_history).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="更换数据源", command=self.change_source).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="更新股票列表", command=self.update_stock_list).pack(side=tk.LEFT, padx=2)

        # 更新时间标签
        self.update_time_label = ttk.Label(toolbar, text="更新于: --")
        self.update_time_label.pack(side=tk.RIGHT, padx=10)

        # 表格
        columns = ('code', 'name', 'price', 'change')
        self.tree = ttk.Treeview(self, columns=columns, show='headings', height=25)
        self.tree.heading('code', text='股票代码')
        self.tree.heading('name', text='股票名称')
        self.tree.heading('price', text='最新价格')
        self.tree.heading('change', text='当日涨跌幅')

        self.tree.column('code', width=100)
        self.tree.column('name', width=150)
        self.tree.column('price', width=100)
        self.tree.column('change', width=100)

        # 滚动条
        scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def refresh_data(self):
        # 模拟加载数据，实际应从数据库或数据接口获取
        # 这里先用一些示例数据
        self.tree.delete(*self.tree.get_children())
        sample_data = [
            ('000001', '平安银行', '12.34', '+2.5%'),
            ('600036', '招商银行', '45.67', '-0.8%'),
            ('600519', '贵州茅台', '1650.00', '+1.2%'),
            ('000858', '五粮液', '150.20', '+0.3%'),
            ('002415', '海康威视', '32.10', '-1.5%'),
        ]
        for item in sample_data:
            self.tree.insert('', 'end', values=item)
        self.update_time_label.config(text=f"更新于: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    def update_realtime(self):
        # 调用数据采集模块更新实时行情
        messagebox.showinfo("提示", "更新实时行情功能待实现")
        self.refresh_data()

    def fetch_history(self):
        messagebox.showinfo("提示", "抓取历史行情功能待实现")

    def change_source(self):
        messagebox.showinfo("提示", "更换数据源功能待实现")

    def update_stock_list(self):
        # 调用股票列表更新脚本
        messagebox.showinfo("提示", "更新股票列表功能待实现")
        self.refresh_data()