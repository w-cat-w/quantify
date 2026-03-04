# 🌩️Polymarket Weather Quant Bot (气象预测量化交易机器人)

一个专为 Polymarket 二元气象市场（最高温预测）设计的自动化量化交易机器人。

本项目实现了与 Polymarket CLOB API 的自动交互，内置概率建模、凯利仓位管理和严格风控机制。  
它通过多源气象数据融合，寻找市场错误定价（Edge），在正期望前提下执行自动化策略。

## ✨核心特性

### 📡1. 多气象源融合 (Alpha Generation)
- 多模型并行：Open-Meteo (HRRR, GFS Seamless, Best Match)、NWS、METAR。
- 智能置信度加权：结合预测时距、模型可靠度、源间分歧度（Disagreement Index）动态融合。

### 🧮 2. 精确概率建模 (Probabilistic Modeling)
- 动态标准差（Dynamic Sigma）：随结算时间临近自动收窄。
- 对齐 Polymarket/Wunderground 整度结算规则：按 `L-0.5` 到 `H+0.5` 计算有效概率。

### ⚔️ 3. 双向优势驱动 (Edge-Driven Dual-Side Trading)
- YES / NO 双向扫描，分别计算公平概率与 Edge。
- 仅在优势满足阈值时交易（例如 `edge_threshold=0.15`）。
- NO 方向价格上限保护（高价 NO 自动拦截，避免盈亏比失衡）。

### 🛡️4. 风控与执行 (Risk Management & Execution)
- Fractional Kelly 动态仓位管理。
- 止盈、止损、移动止损（Trailing Stop）。
- Dust 合成对冲：小于最小可卖价值时，自动尝试对侧对冲风险。
- Spread 过滤：高滑点盘口拒绝下单。
- 日内亏损熔断（Standby）机制。

## ⚙️系统架构

机器人采用轮询架构（run_forever），主循环流程如下：

1. 市场发现（Market Discovery）：从 Gamma API 动态发现目标城市 T0/T1/T2 市场。
2. 预测获取（Forecast Fetching）：拉取并融合多源天气数据。
3. 概率与优势计算（Probability & Edge）：对每个 Outcome (YES/NO) 计算公平概率与 Edge。
4. 仓位与风控检查（Sizing & Safety）：凯利仓位、暴露上限、价差过滤、熔断状态联合决策。
5. 执行与退出（Execution & Exit）：买入、减仓、止盈止损、合成对冲。
6. 报告输出（Reporting）：写入 `diagnostics.json` 与历史快照。

## 🚀安装与运行

### 环境依赖
- Python 3.9+
- py-clob-client
- requests
- pytz

### 安装步骤

1. 克隆仓库

```bash
git clone https://github.com/w-cat-w/quantify.git
cd quantify
```

2. 安装依赖

```bash
pip install -r requirements.txt
```

3. 配置环境变量

复制 `.env.example` 为 `.env`，并填写：

```ini
POLYMARKET_PRIVATE_KEY=0xYourPrivateKeyHere
POLYMARKET_SIGNATURE_TYPE=0
POLYMARKET_FUNDER=
POLY_DRY_RUN=true
```

4. 启动机器人

```bash
python Quantify.py
```

## 📊核心参数（示例）

`Quantify.py` 底部可调参数示例：

```python
bot = PolymarketWeatherMaster(
    private_key=PRIVATE_KEY,
    signature_type=SIGNATURE_TYPE,
    funder=FUNDER,
    investment_usdc=10.0,
    edge_threshold=0.15,
    min_fair_prob=0.20,
    max_trade_usdc=3.0,
    total_exposure_limit=0.80,
    enable_daily_loss_standby=True,
    dry_run=False,
)
```

## 输出文件

- `reports/latest_actions.json`：最新一轮动作结果
- `reports/diagnostics.json`：高频诊断信息（含概率、Edge、价差等）
- `reports/history/*.json`：历史快照
- `reports/history_index.json`：历史索引

## ⚠️免责声明

本仓库代码仅供学术研究和技术交流使用，不构成任何投资或财务建议。
加密货币和预测市场具有极高的波动性和归零风险。在真实环境运行本机器人会导致您钱包中的真实 USDC 发生转移。
使用者应自行承担因软件 Bug、网络延迟、API 变更或策略失效等导致的全部财务损失。

