！这是一个不可用的半成品！
## 项目简介

Quantitative Trading System 是一个模块化、可扩展的量化交易系统，专注于A股市场的多因子选股与策略回测。系统支持日线、分钟线数据采集，内置技术因子计算引擎，提供多因子加权选股策略，并集成了回测框架与图形界面。目标是为量化研究者和交易员提供一个开箱即用的研究平台，并支持自定义因子与机器学习模型扩展。

主要特性

- 数据采集：日线、分钟线（5/15/30/60）、指数行情、交易日历，支持 Baostock 和 AKShare 数据源。
- 因子计算：内置常用技术因子（MA、RSI、MACD、KDJ、布林带、CCI、ATR等），支持 YAML 配置。
- 选股策略：多因子加权打分，支持自定义因子组合与权重。
- 回测系统：支持每日调仓、交易成本（佣金、印花税、滑点）、基准对比，输出净值曲线与绩效指标。
- 命令行接口：提供统一 CLI，支持数据下载、选股、回测。
- 图形界面（GUI）：简单易用的可视化界面，支持选股条件配置与结果展示。

## 目录结构

text复制下载

```text
Quantitative Trading System/
├── configs/                     # 配置文件
│   ├── factors.yaml             # 因子定义
│   ├── strategy.yaml            # 策略参数
│   └── backtest.yaml            # 回测配置
├── data/                        # 数据存储（自动生成）
│   ├── processed/               # Parquet格式数据
│   │   ├── market/              # 行情数据（日线、分钟线）
│   │   ├── factors/             # 预计算因子
│   │   └── metadata/            # 元数据（股票列表、交易日历）
│   └── raw/                     # 原始数据备份
├── logs/                        # 运行日志
├── src/                         # 源代码
│   ├── config/                  # 配置加载
│   ├── data/                    # 数据加载器与交易日历
│   ├── data_fetcher/            # 数据采集模块
│   │   ├── fetcher_daily.py     # 日线采集
│   │   ├── fetcher_minute_base.py  # 分钟线采集
│   │   └── fetcher_index.py     # 指数采集
│   ├── factors/                 # 因子计算引擎
│   │   ├── engine.py            # 因子计算引擎
│   │   ├── functions.py         # 因子函数库
│   │   └── processor.py         # 因子标准化
│   ├── strategies/              # 选股策略
│   │   ├── base.py              # 策略基类
│   │   ├── multi_factor.py      # 多因子策略
│   │   └── loader.py            # 策略配置加载
│   ├── backtest/                # 回测引擎
│   │   ├── engine.py            # 回测主控
│   │   ├── simulator.py         # 模拟交易
│   │   └── analyzer.py          # 绩效分析
│   ├── selector/                # 选股执行器
│   │   └── runner.py
│   ├── cli/                     # 命令行接口
│   │   └── claw_interface.py
│   └── gui/                     # 图形界面（开发中）
├── libraries/                   # 第三方库（可选）
├── gui.py                       # GUI启动入口
├── requirements.txt             # 依赖列表
└── README.md                    # 本文件
```## 安装与依赖

### 环境要求

- Python 3.8+
- 推荐使用虚拟环境（如 conda 或 venv）

### 安装依赖

bash复制下载

```text
pip install -r requirements.txt
```requirements.txt 内容应包含：

text复制下载

```text
baostock
akshare
pandas
pyarrow
pyyaml
requests
tqdm
matplotlib
```## 快速开始

### 1. 生成主板股票列表

bash复制下载

```text
python src/data/generate_stock_list.py
```该脚本从 Baostock 获取最新交易日的主板股票，并保存到 data/processed/metadata/stock_list.csv（格式如 sh.600000）。

### 2. 下载日线数据

bash复制下载

```text
# 下载指定股票
python src/cli/claw_interface.py fetch_daily --codes sh.600036,sz.000001 --start 2020-01-01 --end 2026-03-21

# 下载全部主板股票（建议分批或夜间运行）
python src/cli/claw_interface.py fetch_daily --start 2005-01-01 --end 2026-03-21
```### 3. 下载分钟线数据（可选）

bash复制下载

```text
# 5分钟线（单只股票）
python src/cli/claw_interface.py fetch-minute-base --freq 5 --codes sh.600000 --start 2024-07-01 --end 2024-07-31

# 全市场5分钟线（数据量大，耗时较长）
python src/cli/claw_interface.py fetch-minute-base --freq 5 --start 2020-01-01 --end 2026-03-21
```### 4. 执行选股

bash复制下载

```text
# 使用默认配置，选股日期为当天
python src/cli/claw_interface.py select-stocks --output stocks.csv

# 指定日期
python src/cli/claw_interface.py select-stocks --date 2026-03-18 --output stocks.csv
```选股结果保存在 stocks.csv，包含股票代码和综合得分。

### 5. 运行回测

bash复制下载

```text
python src/cli/claw_interface.py run-backtest --config configs/backtest.yaml
```回测结果输出：

- backtest_nav.csv：每日净值
- backtest_perf.json：绩效指标（年化收益、夏普比率、最大回撤等）

### 6. 启动图形界面（实验性）

bash复制下载

```text
python gui.py
```## 配置说明

### 因子配置 (`configs/factors.yaml`)

定义因子名称、计算函数、参数及依赖数据。示例：

yaml复制下载

```text
factors:
  ma5:
    type: technical
    function: moving_average
    params: { window: 5, field: close }
    inputs: [daily]
  rsi_14:
    type: momentum
    function: rsi
    params: { window: 14 }
    inputs: [daily]
```### 策略配置 (`configs/strategy.yaml`)

指定使用的因子及其权重：

yaml复制下载

```text
type: "MultiFactorStrategy"
params:
  factors: ["ma5_ma20_ratio", "rsi_14", "volume_ratio_5"]
  weights: [0.4, 0.3, 0.3]
  top_n: 30
  filter_st: true
  standardize: "rank"
```### 回测配置 (`configs/backtest.yaml`)

设置回测时间、初始资金、交易成本、基准等：

yaml复制下载

```text
backtest:
  start_date: "2025-01-01"
  end_date: "2026-03-18"
  initial_cash: 1000000
trading:
  commission: 0.0004
  stamp_tax: 0.001
  slippage: 0.002
benchmark:
  code: "000300.SH"
factor_config: "configs/factors.yaml"
strategies:
  - name: "strategy_base"
    config: "configs/strategy.yaml"
    weight: 1.0
```## 高级功能

### 因子扩展

- 在 src/factors/functions.py 中添加新因子函数。
- 在 configs/factors.yaml 中注册因子。

### 分钟线数据使用

分钟线数据以 5min、15min、30min、60min 存储在 data/processed/market/ 下。可通过 DataLoader.load_minute() 方法加载。

### 机器学习扩展（规划中）

- 支持使用 Ta-lib 生成技术指标。
- 特征工程：IC/IR 分析、特征筛选。
- 模型训练：LightGBM 排序学习。
- 集成到回测系统。

## 注意事项

- 数据源稳定性：Baostock 和 AKShare 为免费数据源，存在网络波动和限流可能，建议增加重试机制。
- 磁盘空间：全市场5分钟线数据约 5-10 GB，请确保有足够空间。
- 股票代码格式：系统统一使用带市场前缀的代码（如 sh.600036），在生成股票列表时已自动处理。
- 回测精度：当前模拟器采用收盘价成交，未考虑涨跌停、停牌等，实际交易需进一步细化。

## 后续计划

- 修复已知 Bug（登录状态检查、除零保护等）
- 引入特征缓存与批量数据加载优化
- 集成 LightGBM 机器学习模型
- 完善图形界面，支持因子配置可视化
- 添加资金流向、龙虎榜等另类数据
- 实现模拟盘自动交易接口

## 许可证

本项目仅供学习和研究使用，数据源版权归原始提供者所有，请勿用于商业用途。
