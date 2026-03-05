# 🌦️ Polymarket Weather Quant Bot

一个面向 **Polymarket 天气市场** 的实盘量化机器人，核心目标是：

- 用多气象源融合给出温度分布概率
- 在 YES / NO 双向盘口中寻找定价偏差（Edge）
- 在严格风控下自动交易、自动减仓、自动写入诊断数据

---

## ✨ 当前版本能力

### 📡 多源天气融合
- Open-Meteo（含 HRRR 失败自动降级）
- NWS（美国）
- METAR（机场实测链路）
- 源可靠度动态权重 + 分歧指数（`disagreement_index`）
- 置信度评分（`confidence_score`）参与交易过滤

### 🧠 概率模型（已对齐结算规则）
- 按市场区间计算概率，不是只看点值
- 对整度结算区间采用有效积分范围（如区间 48-49 会按物理边界扩展）
- 根据距离结算时间动态调整 sigma
- 支持华氏 / 摄氏市场自动识别与计算

### ⚔️ YES / NO 双向交易
- 同一温度区间同时评估 YES 与 NO 的公平概率和 Edge
- 统一按 Edge 排序挑选机会
- NO 方向单独价格上限保护（防高价低盈亏比 NO）
- 避免同一 slot 同时持有 YES 与 NO 冲突仓位

### 🛡️ 实盘风控与执行
- Fractional Kelly + 资金利用率控制
- 最小交易额与交易所硬门槛（$1）兼容
- 强信号小额补偿：仅在高 edge + 高置信度时补到 $1
- 盘口价差过滤（spread filter）
- 止盈 / 硬止损 / 模型反转 / 临近结算保护
- 移动止损（Trailing Stop）
- Dust 仓位合成平仓（预算上限）
- 日内亏损熔断开关（Standby）

### 🧾 数据与可观测性
- JSON 报告：最新动作 + 历史快照 + diagnostics
- MySQL 双写（可开关）
- 扁平化分析表：`fact_bot_actions`、`dim_bot_diagnostics`
- 真实成交流水表：`trade_history`
- 前端历史回放页（静态）

---

## 🏗️ 项目结构

```text
.
├─ Quantify.py                      # 主策略脚本
├─ frontend/history_viewer.html     # 静态历史回放页面
├─ reports/                         # JSON 报告输出目录
├─ scripts/init_mysql.sql           # MySQL 初始化脚本
├─ scripts/migrate_json_to_mysql.py # 历史 JSON 迁移到 MySQL
├─ .env.example                     # 环境变量模板
└─ start_bot_watchdog.bat           # Windows 启动脚本（仓库内）
```

---

## 🚀 快速开始

### 1) 安装依赖

```bash
pip install -r requirements.txt
```

### 2) 配置环境变量

复制 `.env.example` 为 `.env`，至少配置：

```ini
POLYMARKET_PRIVATE_KEY=0xYOUR_PRIVATE_KEY
POLYMARKET_SIGNATURE_TYPE=0
POLYMARKET_FUNDER=

POLY_DRY_RUN=false
POLY_LOOP_INTERVAL_SECONDS=300
POLY_HEARTBEAT_SECONDS=3600

POLY_ENABLE_DAILY_LOSS_STANDBY=false

POLY_ENABLE_DB_DUAL_WRITE=false
POLY_DB_HOST=127.0.0.1
POLY_DB_PORT=3306
POLY_DB_USER=root
POLY_DB_PASSWORD=root
POLY_DB_NAME=quantify
POLY_DB_CONNECT_TIMEOUT_S=5
```

### 3) 运行机器人

```bash
python Quantify.py
```

---

## ⚙️ 默认实盘参数（代码内）

当前 `__main__` 实例化参数为：

- `investment_usdc=20.0`
- `edge_threshold=0.12`
- `min_confidence=0.60`
- `min_fair_prob=0.20`
- `max_trade_usdc=3.0`

说明：这是一套偏保守的实盘参数，强调过滤质量而非交易频率。📉

---

## 📊 输出与排障

### JSON 输出
- `reports/latest_actions.json`：最新一轮动作
- `reports/history/*.json`：历史批次
- `reports/history_index.json`：历史索引
- `reports/diagnostics.json`：全量诊断（含 YES/NO 对比）

### MySQL（启用双写时）
- `fact_bot_actions`：每条策略动作（含信号、价格、edge、confidence）
- `dim_bot_diagnostics`：模型诊断宽表（YES/NO 拆分）
- `trade_history`：真实买卖成交流水

### 常见“没下单”原因
- `dynamic_buy_usdc<1.0`
- `PRICE_TOO_LOW` / `PRICE_TOO_HIGH`
- `CONFIDENCE_TOO_LOW`
- `EDGE_ABS_TOO_LOW` / `EDGE_RATIO_TOO_LOW`
- `SKIP_WIDE_SPREAD`

---

## 🧪 前端历史回放

启动静态服务后访问：

- `http://127.0.0.1:8000/frontend/history_viewer.html`

可按日期 / 批次查看策略行为、YES/NO 对比、过滤原因和执行结果。🧭

---

## 🔐 安全提示

- 私钥只放 `.env`，不要提交到 Git
- `.env`、本地 CSV、运行日志不要上传仓库
- 建议先 `dry_run=true` 验证再实盘

---

## ⚠️ 免责声明

本项目仅用于技术研究与策略实验，不构成任何投资建议。预测市场和加密资产波动极大，实盘可能发生全部本金损失。请仅使用可承受损失的资金。🙏
