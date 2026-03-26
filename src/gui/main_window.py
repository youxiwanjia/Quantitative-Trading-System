import tkinter as tk
from tkinter import ttk
from .tabs.stock_list_tab import StockListTab
from .tabs.selection_tab import SelectionTab
from .tabs.backtest_tab import BacktestTab
from .tabs.system_status_tab import SystemStatusTab

class MainWindow:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("量化交易系统 - GUI")
        self.root.geometry("1200x700")

        # 创建标签页控件
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 添加四个标签页
        self.stock_list_tab = StockListTab(self.notebook)
        self.selection_tab = SelectionTab(self.notebook)
        self.backtest_tab = BacktestTab(self.notebook)
        self.system_status_tab = SystemStatusTab(self.notebook)

        self.notebook.add(self.stock_list_tab, text="主板股票列表")
        self.notebook.add(self.selection_tab, text="选股")
        self.notebook.add(self.backtest_tab, text="回测")
        self.notebook.add(self.system_status_tab, text="系统状态")

    def run(self):
        self.root.mainloop()