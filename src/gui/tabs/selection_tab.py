import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
from ..dialogs.factor_config_dialog import FactorConfigDialog

class SelectionTab(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.create_widgets()

    def create_widgets(self):
        # 顶部按钮
        toolbar = ttk.Frame(self)
        toolbar.pack(fill=tk.X, padx=5, pady=5)

        ttk.Button(toolbar, text="条件选股", command=self.open_factor_dialog).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="加载上次结果", command=self.load_last_result).pack(side=tk.LEFT, padx=2)

        # 结果表格
        columns = ('rank', 'code', 'name', 'score', 'details')
        self.tree = ttk.Treeview(self, columns=columns, show='headings', height=25)
        self.tree.heading('rank', text='排名')
        self.tree.heading('code', text='股票代码')
        self.tree.heading('name', text='股票名称')
        self.tree.heading('score', text='综合得分')
        self.tree.heading('details', text='因子明细')

        self.tree.column('rank', width=50)
        self.tree.column('code', width=100)
        self.tree.column('name', width=150)
        self.tree.column('score', width=100)
        self.tree.column('details', width=300)

        # 滚动条
        scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # 初始加载示例数据
        self.load_sample_data()

    def load_sample_data(self):
        # 尝试读取上次选股结果文件
        try:
            df = pd.read_csv('stocks.csv')
            # 假设只有code列，需要补充分数等
            for i, row in df.iterrows():
                self.tree.insert('', 'end', values=(i+1, row['code'], '', '', ''))
        except:
            sample = [
                (1, '600519', '贵州茅台', '0.95', 'MA5:1.2, RSI:0.8'),
                (2, '000858', '五粮液', '0.92', 'MA5:1.1, RSI:0.9'),
                (3, '000001', '平安银行', '0.88', 'MA5:0.9, RSI:0.7'),
            ]
            for item in sample:
                self.tree.insert('', 'end', values=item)

    def open_factor_dialog(self):
        dialog = FactorConfigDialog(self)
        self.wait_window(dialog)
        if dialog.result:
            # 用户点击了确定，运行选股
            self.run_selection(dialog.result)

    def run_selection(self, factor_config):
        # 这里应调用实际的选股模块
        print("选股配置:", factor_config)
        messagebox.showinfo("选股", f"使用配置: {factor_config} 运行选股")
        # 模拟选股结果
        self.tree.delete(*self.tree.get_children())
        sample = [
            (1, '600519', '贵州茅台', '0.95', '...'),
            (2, '000858', '五粮液', '0.92', '...'),
            (3, '000001', '平安银行', '0.88', '...'),
        ]
        for item in sample:
            self.tree.insert('', 'end', values=item)

    def load_last_result(self):
        self.load_sample_data()