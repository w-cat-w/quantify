# Polymarket Weather Master

基于 `py-clob-client + requests` 的 Polymarket 气象量化交易机器人。  
当前版本已支持多城市、多天气源、YES/NO 双向交易、持仓成本跟踪、止盈止损、历史回放与诊断面板。

代码主入口：`Quantify.py`（核心类：`PolymarketWeatherMaster`）

## 功能概览

- 多城市扫描：默认覆盖 New York、Miami、Chicago、Atlanta、Toronto、London、Paris、Brussels、Vienna、Seoul、Tokyo、Dallas、Ankara、Buenos Aires、Wellington、Sydney、Seattle、Sao Paulo、Munich、Lucknow。
- 动态市场发现：基于 Gamma `/events` 搜索与评分，自动锁定 T0/T1/T2（纽约时区）目标天气市场。
- YES/NO 双向交易：同一区间同时评估 YES 与 NO 的 `fair_prob/price/edge`，按 Edge 统一排序决策。
- 多源天气融合：优先尝试 Open-Meteo 模型（含 HRRR/GFS 相关），并融合 NWS、METAR、fallback 源。
- 风控体系：最小概率/价格过滤、相对优势过滤、仓位上限、全局暴露上限、日内亏损熔断 Standby。
- 持仓管理：加权成本、浮盈亏、止盈/止损/模型反转平仓、临近结算防守减仓。
- 报告系统：每轮输出 `latest_actions.json`、历史快照、`diagnostics.json`，并提供前端回放页面。
- 单实例保护：文件锁防止多开同一脚本。

## 核心流程

1. 按纽约时区计算交易日期（T0/T1/T2）并构造日期标签（如 `March 4`）。
2. 先做市场探测（Discovery），找到可交易 condition 与 outcome token。
3. 获取城市未来 3 天最高温预测（多数据源融合）。
4. 对每个区间计算：
   - `fair_prob_yes` 与 `fair_prob_no = 1 - fair_prob_yes`
   - 市场价格、绝对 edge、相对 edge（edge_ratio）
5. 在风控条件满足时执行交易（BUY/HOLD/REDUCE），并记录成本、日志、历史与诊断。

## 默认交易与风控参数（当前代码）

- `bankroll_fraction=0.18`
- `edge_threshold=0.10`
- `min_fair_prob=0.15`
- `min_market_price=0.10`
- `max_market_price=0.70`
- `min_edge_ratio=1.25`
- `min_trade_usdc=1.0`（受交易所最小买入约束）
- `max_trade_usdc=2.0`
- `total_exposure_limit=0.80`
- `take_profit_ratio=1.12`
- `stop_loss_ratio=0.96`
- `daily_loss_limit_ratio=0.30`（触发 Standby）
- `min_hours_to_settlement_for_entry=12.0`
- `single_outcome_per_condition=True`

说明：这些参数都在 `Quantify.py` 构造函数中，可按实盘反馈继续调优。

## 环境准备

### 1) 安装依赖

```powershell
.\.venv\Scripts\pip.exe install py-clob-client requests pytz
```

### 2) 配置私钥与账户参数

程序启动时会自动读取项目根目录 `.env`（若存在），并以环境变量为准。

必填：

- `POLYMARKET_PRIVATE_KEY=0x...`（64 位十六进制私钥）

可选（代理钱包/邮箱钱包常用）：

- `POLYMARKET_SIGNATURE_TYPE=0|1|2`
- `POLYMARKET_FUNDER=0x...`

循环配置（可选）：

- `POLY_LOOP_INTERVAL_SECONDS=300`（默认 5 分钟）
- `POLY_HEARTBEAT_SECONDS=3600`
- `POLY_DRY_RUN=true|false`

PowerShell 临时设置示例：

```powershell
$env:POLYMARKET_PRIVATE_KEY="0x你的私钥"
$env:POLYMARKET_SIGNATURE_TYPE="1"
$env:POLYMARKET_FUNDER="0x你的funder地址"
```

## 运行方式

### 方式 A：直接运行主脚本

```powershell
.\.venv\Scripts\python.exe .\Quantify.py
```

### 方式 B：看门狗模式（自动重启）

```powershell
.\start_bot_watchdog.bat
```

## 输出文件与前端回放

每轮执行会写入：

- `reports/latest_actions.json`：最新一轮策略结果
- `reports/history/*.json`：历史快照
- `reports/history_index.json`：历史索引（倒序）
- `reports/diagnostics.json`：最新一轮诊断数据（含 YES/NO 双向对比）
- `reports/positions_cost.json`：持仓成本缓存
- `reports/daily_realized_pnl.json`：日内已实现盈亏

前端页面：

- `frontend/history_viewer.html`

启动静态服务并访问：

```powershell
.\.venv\Scripts\python.exe -m http.server 8000
```

- `http://127.0.0.1:8000/frontend/history_viewer.html`

页面支持：

- 历史批次筛选
- BUY/HOLD/REDUCE 回放
- Discovery 发现结果展示
- YES/NO 同区间价格、公允概率、Edge 对比与推荐方向

## 常见问题

1. 看起来“没下单”
- 先看 `reports/latest_actions.json` 的 `hold_reason`。
- 常见原因：`SETTLE_TOO_NEAR`、`PRICE_TOO_LOW`、`PROB_TOO_LOW`、`total_exposure_limit`、`daily_loss_standby_active`。

2. 为什么有两个 python 进程
- 可能是父子进程关系，不一定是双实例。
- 以单实例锁文件 `reports/quantify_bot.lock` 为准；若重复启动，会被拦截退出。

3. 日志有 `POST /auth/api-key 400` 但随后恢复
- 常见于凭证刷新流程，不一定是致命错误；关注后续是否有 `CLOB API credentials refreshed`。

## 安全说明

- 私钥不要提交到仓库。
- 建议仅用小额资金做参数验证。
- 本项目仅作技术研究与自动化示例，不构成投资建议。
