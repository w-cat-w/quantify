# Polymarket Weather Master

基于 py-clob-client 和 requests 的 Polymarket 气象量化交易机器人。
当前策略聚焦纽约拉瓜迪亚机场 (LGA) 未来三天最高气温市场 (T0/T1/T2)。
当前版本已扩展为多城市并行（默认常见城市），每个城市使用与 NYC 相同的策略逻辑。
默认城市包括：New York、Miami、Chicago、Atlanta、Toronto、London、Paris、Brussels、Vienna、Seoul、Tokyo、Dallas、Ankara、Buenos Aires、Wellington、Sydney。

## 项目能力

- 纽约时区日期管理：自动计算 T0/T1/T2，输出 Polymarket 标题日期格式（如 March 4）。
- 动态市场发现：自动发现市场并提取 conditionId、outcomes、clobTokenIds。
- 气象预测：优先 Open-Meteo HRRR，失败回退 GFS；按日计算最高温。
- 量化信号：计算 fair_prob、market_price、edge，并输出 BUY/HOLD/REDUCE。
- 持仓感知：读取 token 持仓，支持仓位上限和负 edge 减仓。
- 小资金适配：根据 USDC 余额动态调整下单金额，不再固定 10 美元。
- 容错机制：网络重试、429 限频退避、CLOB 401/403 自动刷新凭证。

## 核心文件

- Quantify.py

核心类：PolymarketWeatherMaster

## 策略概要

1. 发现未来三天 NYC 高温市场。
2. 获取 LGA 三天温度预测并计算每日最高温。
3. 对每个 outcome 计算模型概率 fair_prob。
4. 从 CLOB 获取买价 market_price，计算 edge = fair_prob - market_price。
5. 满足稳定性、edge、仓位和资金约束时买入；负 edge 时减仓。

## 风控与资金管理

固定风控：
- edge_threshold：买入最小优势阈值（默认 0.08）。
- max_position_shares_per_token：单 token 份额上限。
- max_total_position_shares_per_condition：单 condition 总份额上限。
- reduce_fraction_on_negative_edge：负 edge 减仓比例（默认 0.5）。

小资金自适应：
- capital_utilization_per_trade：每笔最多使用余额比例。
- capital_reserve_usdc：预留不交易资金。
- min_trade_usdc：最小下单金额，低于该值不下单。
- max_token_exposure_ratio：单 token 资金暴露比例上限。
- max_condition_exposure_ratio：单 condition 资金暴露比例上限。

## 依赖安装

在项目目录执行：

```powershell
.\.venv\Scripts\pip.exe install py-clob-client requests pytz
```

## 私钥配置（环境变量）

程序入口只从环境变量读取私钥：POLYMARKET_PRIVATE_KEY

当前终端生效：

```powershell
$env:POLYMARKET_PRIVATE_KEY="0x你的64位十六进制私钥"
```

持久化到用户环境变量：

```powershell
[Environment]::SetEnvironmentVariable("POLYMARKET_PRIVATE_KEY","0x你的64位十六进制私钥","User")
```

## 运行

```powershell
.\.venv\Scripts\python.exe .\Quantify.py
```

默认 dry_run=True（仅信号，不实盘下单）。
需要实盘时，将 Quantify.py 入口中的 dry_run 改为 False。

## 静态页面查看机会与操作

脚本每轮会自动输出：

- `reports/latest_actions.json`
- `reports/history_index.json`（按日期倒序索引）
- `reports/history/*.json`（每轮快照）
- `frontend/history_viewer.html`（历史查询页面，前后端分离）

直接打开 `frontend/history_viewer.html` 即可查看。  
如果浏览器本地文件限制较严，建议在项目目录启动一个静态服务：

```powershell
.\.venv\Scripts\python.exe -m http.server 8000
```

然后访问：

- `http://127.0.0.1:8000/frontend/history_viewer.html`

历史页面用法：

1. 先选日期（下拉框）。  
2. 再选该日期下的一次运行批次（按时间倒序）。  
3. 输入关键词筛选该批次里的机会和操作记录。
4. 若当轮无交易机会，页面也会显示 `[DISCOVERY]` 行，说明每个日期的市场发现结果与跳过原因。
5. 页面顶部会显示“运行摘要卡片”（BUY/HOLD/REDUCE、DISCOVERY FOUND/SKIP、总记录）。

## 常见问题

1. 报错 Could not discover market for March X
- 已改为跳过当日，不会中断整轮。
- 常见原因是当天市场未上线或 Gamma query 返回无关结果。

2. 日志出现 POST /auth/api-key 400，随后 GET /auth/derive-api-key 200
- 通常可恢复，不是致命错误。

3. PowerShell 设置变量报语法错误
- 设置变量和运行命令要分行，或用分号分隔：

```powershell
$env:POLYMARKET_PRIVATE_KEY="0x你的私钥"; .\.venv\Scripts\python.exe .\Quantify.py
```

## 安全提示

- 不要把私钥写入代码仓库。
- 私钥一旦泄露，立即更换并转移资产。
- 本项目仅供研究和自动化示例，不构成投资建议。
