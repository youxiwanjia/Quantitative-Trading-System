#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
图形界面启动脚本
"""
import sys
import os

# 将项目根目录添加到 sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.gui.main_window import MainWindow

if __name__ == "__main__":
    app = MainWindow()
    app.run()