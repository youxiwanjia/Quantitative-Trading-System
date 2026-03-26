import tkinter as tk
from tkinter import ttk, messagebox

class FactorConfigDialog(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("选股条件配置")
        self.geometry("800x500")
        self.result = None

        # 可用因子列表（模拟数据）
        self.available_factors = [
            "MA均线", "RSI", "MACD", "KDJ", "布林带宽度", "CCI", "ATR"
        ]
        self.selected_factors = []  # 存储已选因子及其参数

        self.create_widgets()
        self.grab_set()  # 模态

    def create_widgets(self):
        # 主布局：左右两列
        main_frame = ttk.Frame(self)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 左侧：可用因子列表
        left_frame = ttk.LabelFrame(main_frame, text="可用因子列表")
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)

        self.factor_listbox = tk.Listbox(left_frame, selectmode=tk.SINGLE)
        self.factor_listbox.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        for f in self.available_factors:
            self.factor_listbox.insert(tk.END, f)
        self.factor_listbox.bind('<<ListboxSelect>>', self.on_factor_select)

        # 右侧：已选因子方案列表
        right_frame = ttk.LabelFrame(main_frame, text="已选因子方案")
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)

        columns = ('name', 'period_type', 'period', 'weight')
        self.tree = ttk.Treeview(right_frame, columns=columns, show='headings', height=10)
        self.tree.heading('name', text='因子')
        self.tree.heading('period_type', text='周期类型')
        self.tree.heading('period', text='周期参数')
        self.tree.heading('weight', text='权重')
        self.tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 底部参数配置区域
        param_frame = ttk.LabelFrame(self, text="因子参数设置")
        param_frame.pack(fill=tk.X, padx=10, pady=5)

        # 当前选中的因子
        self.current_factor = tk.StringVar()
        ttk.Label(param_frame, text="当前因子:").grid(row=0, column=0, padx=5, pady=5, sticky='w')
        ttk.Label(param_frame, textvariable=self.current_factor).grid(row=0, column=1, padx=5, pady=5, sticky='w')

        # 周期类型选择
        ttk.Label(param_frame, text="周期类型:").grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.period_type_var = tk.StringVar(value="日线")
        period_types = ['1min', '5min', '30min', '60min', '日线', '周线', '月线']
        period_combo = ttk.Combobox(param_frame, textvariable=self.period_type_var, values=period_types, width=10)
        period_combo.grid(row=1, column=1, padx=5, pady=5, sticky='w')

        # 周期参数输入
        ttk.Label(param_frame, text="周期参数:").grid(row=2, column=0, padx=5, pady=5, sticky='w')
        self.period_entry = ttk.Entry(param_frame, width=10)
        self.period_entry.grid(row=2, column=1, padx=5, pady=5, sticky='w')
        self.period_entry.insert(0, "5")  # 默认5

        # 权重输入
        ttk.Label(param_frame, text="权重:").grid(row=3, column=0, padx=5, pady=5, sticky='w')
        self.weight_entry = ttk.Entry(param_frame, width=10)
        self.weight_entry.grid(row=3, column=1, padx=5, pady=5, sticky='w')
        self.weight_entry.insert(0, "0.3")

        # 添加按钮
        ttk.Button(param_frame, text="添加因子到方案", command=self.add_factor).grid(row=4, column=0, columnspan=2, pady=10)

        # 底部按钮
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)
        ttk.Button(btn_frame, text="取消", command=self.cancel).pack(side=tk.RIGHT, padx=5)
        ttk.Button(btn_frame, text="确定并运行选股", command=self.ok).pack(side=tk.RIGHT, padx=5)

    def on_factor_select(self, event):
        selection = self.factor_listbox.curselection()
        if selection:
            factor = self.factor_listbox.get(selection[0])
            self.current_factor.set(factor)

    def add_factor(self):
        factor = self.current_factor.get()
        if not factor:
            messagebox.showwarning("警告", "请先选择一个因子")
            return
        period_type = self.period_type_var.get()
        period = self.period_entry.get()
        weight = self.weight_entry.get()

        # 简单验证
        if not period.isdigit():
            messagebox.showerror("错误", "周期参数必须为数字")
            return
        try:
            weight_float = float(weight)
        except ValueError:
            messagebox.showerror("错误", "权重必须为数字")
            return

        # 添加到已选列表
        self.tree.insert('', 'end', values=(factor, period_type, period, weight))
        self.selected_factors.append({
            'name': factor,
            'period_type': period_type,
            'period': int(period),
            'weight': weight_float
        })

    def cancel(self):
        self.destroy()

    def ok(self):
        if not self.selected_factors:
            messagebox.showwarning("警告", "至少添加一个因子")
            return
        # 返回配置结果
        self.result = self.selected_factors
        self.destroy()