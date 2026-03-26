import tkinter as tk
from tkinter import ttk
import pandas as pd

class StockSelectionTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.create_widgets()

    def create_widgets(self):
        # 顶部控制栏
        control_frame = ttk.Frame(self)
        control_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(control_frame, text="选股日期:").pack(side=tk.LEFT, padx=5)
        self.date_entry = ttk.Entry(control_frame, width=10)
        self.date_entry.insert(0, "2026-03-18")
        self.date_entry.pack(side=tk.LEFT, padx=5)

        ttk.Button(control_frame, text="运行选股", command=self.run_selection).pack(side=tk.LEFT, padx=5)

        # 表格显示区域
        columns = ('code', 'name', 'score', 'factor1', 'factor2', 'factor3')
        self.tree = ttk.Treeview(self, columns=columns, show='headings', height=20)
        self.tree.heading('code', text='代码')
        self.tree.heading('name', text='名称')
        self.tree.heading('score', text='综合得分')
        self.tree.heading('factor1', text='因子1')
        self.tree.heading('factor2', text='因子2')
        self.tree.heading('factor3', text='因子3')

        # 滚动条
        scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # 初始加载示例数据（可以从CSV读取，但这里先占位）
        self.load_sample_data()

    def load_sample_data(self):
        # 尝试读取最新选股结果CSV，如果没有则显示示例
        try:
            df = pd.read_csv('stocks.csv')
            # 假设CSV只有code列，我们还需要名称等，这里简化
            for i, row in df.iterrows():
                self.tree.insert('', 'end', values=(row['code'], '', '', '', '', ''))
        except:
            # 示例数据
            sample = [('000001', '平安银行', '0.85', '1.2', '0.3', '0.5'),
                      ('600036', '招商银行', '0.78', '1.1', '0.4', '0.6')]
            for item in sample:
                self.tree.insert('', 'end', values=item)

    def run_selection(self):
        date = self.date_entry.get()
        # 这里应该调用选股模块，并刷新表格
        # 由于耗时，应放在线程中，但暂时简化
        print(f"运行选股，日期：{date}")
        # 刷新表格（这里仅示例）
        self.tree.delete(*self.tree.get_children())
        self.tree.insert('', 'end', values=('600519', '贵州茅台', '0.99', '2.1', '0.8', '1.2'))