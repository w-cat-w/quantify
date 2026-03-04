# 🌩️ Polymarket Weather Quant Bot (气象预测量化交易机器人)

一个专为 Polymarket 二元气象市场（天气最高温预测）设计的高级自动化量化交易机器人。

本项目不仅实现了与 Polymarket CLOB API 的全自动交互，更内置了机构级的概率建模、基于凯利公式的资金管理，以及极其严苛的风控退出机制。它通过多源气象数据融合寻找市场的“错误定价（Edge）”，在确保正期望值（Positive EV）的前提下执行无人值守的自动化套利。

## ✨ 核心特性 (Core Features)

### 1. 📡 多气象源融合 (Alpha Generation)
- **多模型并行**：集成 Open-Meteo (HRRR, GFS Seamless, Best Match)、美国国家气象局 (NWS) 以及实时航空气象 (METAR) 数据。
- **智能置信度加权**：根据预测时间跨度（Horizon）、模型历史可靠度以及各数据源之间的分歧度（Disagreement Index），动态计算加权预测值和置信度。

### 2. 🧮 精确的概率建模 (Probabilistic Modeling)
- **动态标准差 (Dynamic Sigma)**：随着结算时间的临近，自动收窄温度分布预测的标准差 $\sigma$。
- **精准对齐结算规则**：完美适配 Polymarket 基于 Wunderground 的“整数结算”与“0.5 进位”规则。算法将物理预测值映射到 `[L-0.5, H+0.5)` 区间进行正态分布 CDF 积分，捕捉盘口隐藏的真实胜率。

### 3. ⚔️ 双向优势驱动 (Edge-Driven Dual-Side Trading)
- **YES / NO 全域扫描**：不再盲目押注单一方向。系统同时计算气温落入某区间（YES）和不落入某区间（NO）的公平概率。
- **严格的出手门槛**：仅在“模型概率 - 盘口价格 > 预设阈值（如 15% Edge）”时才触发交易指令。
- **过滤“伪高胜率”**：内置最高价限制（如拒绝买入价格高于 50¢ 的 NO Token），彻底杜绝盈亏比极度失衡的“捡硬币”陷阱。

### 4. 🛡️ 极端风控与资金管理 (Risk Management & Execution)
- **Fractional Kelly 仓位管理**：基于边际优势（Edge）和凯利公式动态计算单笔下注金额，并受限于全盘暴露上限。
- **移动止损 (Trailing Stop)**：当仓位获得显著浮盈（如 +30%）后，自动上移止损线。一旦回撤 10% 立即清仓，彻底锁定利润。
- **微小仓位合成对冲 (Synthetic Close)**：面对 Polymarket $1.00 的最低市价单硬限制，当亏损仓位价值跌破 $1.00 无法直接卖出时，机器人会自动买入对侧 Token（如持有 YES 则买入 NO）形成完美对冲，物理锁死残余风险。
- **流动性保护**：下单前进行买卖价差（Spread）检测，拒绝在高滑点盘口执行 FOK 订单。

---

## ⚙️ 系统架构 (Architecture)



机器人采用轮询架构（`run_forever`），主循环流程如下：
1. **Market Discovery**: 扫描 Gamma API，动态发现特定城市 T0/T1/T2 的天气区间市场。
2. **Forecast Fetching**: 并发拉取各大气象局 API，融合计算未来最高温 $\mu$ 与 置信度。
3. **Probability & Edge Engine**: 针对每个 Outcome (YES/NO) 计算 Fair Probability，并与 CLOB 实时盘口价格对比，得出 Edge。
4. **Position Sizing & Safety Check**: 凯利公式计算底仓，结合总暴露上限、价差过滤、日内亏损熔断（Standby 模式）等风控指标得出最终下单量。
5. **Execution & Exit**: 评估现有持仓，执行止盈/移动止损/合成平仓；向符合要求的新信号发射 FOK 市价单。
6. **Reporting**: 导出高频诊断日志（`diagnostics.json`）与历史交易快照。

---

## 🚀 安装与运行 (Installation & Usage)

### 环境依赖
- Python 3.9+
- [py-clob-client](https://github.com/Polymarket/py-clob-client) (Polymarket 官方 Python SDK)
- `requests`, `pytz`

### 安装步骤
1. 克隆本仓库：
   ```bash
   git clone [https://github.com/YOUR_USERNAME/polymarket-weather-quant.git](https://github.com/YOUR_USERNAME/polymarket-weather-quant.git)
   cd polymarket-weather-quant
安装依赖：

Bash
pip install -r requirements.txt
配置环境变量：
复制 .env.example 为 .env 并填入你的 Polymarket 钱包私钥。

Ini, TOML
POLYMARKET_PRIVATE_KEY=0xYourPrivateKeyHere
POLYMARKET_SIGNATURE_TYPE=0  # 0 for EOA, 1 or 2 for Proxy Wallets
# POLY_DRY_RUN=true          # 开启此项则只打印交易信号，不真实下单
启动机器人
Bash
python Quantify.py
📊 核心配置参数 (Configuration Tuning)
在 Quantify.py 底部可调整机器人的交易偏好：

Python
bot = PolymarketWeatherMaster(
    private_key=PRIVATE_KEY,
    investment_usdc=10.0,           # 基准资金
    edge_threshold=0.15,            # 最小优势阈值 (15%)
    min_fair_prob=0.20,             # 最低公平概率 (过滤极小概率事件)
    max_trade_usdc=3.0,             # 单笔最高预算
    total_exposure_limit=0.80,      # 全局资金暴露上限 (80%)
    enable_daily_loss_standby=True, # 开启日内亏损熔断保护
    dry_run=False                   # 是否为模拟沙盒模式
)
⚠️ 免责声明 (Disclaimer)
本仓库代码仅供学术研究和技术交流使用，不构成任何投资或财务建议。
加密货币和预测市场具有极高的波动性和归零风险。在真实环境运行本机器人会导致您钱包中的真实 USDC 发生转移。
使用者应自行承担因软件 Bug、网络延迟、API 变更或策略失效导致的全部财务损失。Do not trade with money you cannot afford to lose.

Created by [Your Name/Handle] - Pull Requests are welcome!
