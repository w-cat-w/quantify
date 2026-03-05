import json
import logging
import math
import os
import random
import re
import statistics
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pytz
import requests
try:
    import pymysql
except Exception:  # pragma: no cover
    pymysql = None
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams, MarketOrderArgs, OrderArgs
from py_clob_client.constants import POLYGON
from py_clob_client.exceptions import PolyApiException


# 全局日志对象：统一使用同一个 logger，方便接入监控系统
LOGGER = logging.getLogger("polymarket_weather_master")

try:
    import msvcrt
except Exception:  # pragma: no cover
    msvcrt = None


# 单实例锁句柄（进程生命周期内保持打开）
_SINGLE_INSTANCE_LOCK_FP = None


def load_env_file(path: str = ".env") -> None:
    """从本地 .env 读取环境变量（仅设置当前进程尚未设置的键）。"""
    p = Path(path)
    if not p.exists():
        return
    try:
        text = p.read_text(encoding="utf-8-sig")
    except Exception:
        return
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        key = k.strip()
        val = v.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


def acquire_single_instance_lock(lock_file: str = "reports/quantify_bot.lock") -> bool:
    """
    获取单实例文件锁（Windows）。
    返回 True 表示拿到锁，False 表示已有实例在运行。
    """
    global _SINGLE_INSTANCE_LOCK_FP
    if msvcrt is None:
        return True
    lock_path = Path(lock_file)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fp = open(lock_path, "a+", encoding="utf-8")
    try:
        fp.seek(0)
        # 锁定首字节，非阻塞；已被占用时抛 OSError
        msvcrt.locking(fp.fileno(), msvcrt.LK_NBLCK, 1)
        fp.seek(0)
        fp.truncate(0)
        fp.write(str(os.getpid()))
        fp.flush()
        _SINGLE_INSTANCE_LOCK_FP = fp
        return True
    except OSError:
        fp.close()
        return False


@dataclass
class OutcomeToken:
    """单个 outcome 的结构化信息。"""

    # Polymarket outcome 文本，例如 "48F to 50F"
    label: str
    # 该区间 YES 方向 token id
    yes_token_id: str
    # 该区间 NO 方向 token id
    no_token_id: str


@dataclass
class DailyMarket:
    """某一天(T0/T1/T2)对应的天气市场。"""

    # 日期键，格式 YYYY-MM-DD（纽约时区）
    date_key: str
    # 标题日期，格式如 "March 4"
    date_label: str
    # 市场问题文本
    question: str
    # Polymarket condition id
    condition_id: str
    # 市场下全部可交易 outcome
    outcomes: List[OutcomeToken]
    # 结算时间 ISO（用于动态 sigma），可能为空
    settle_time_iso: str = ""


class PolymarketWeatherMaster:
    """
    高级 Polymarket 气象交易机器人。

    功能模块：
    1) 动态日期管理（纽约时区 T0/T1/T2）
    2) 动态市场发现（自动匹配 condition id + token ids）
    3) Open-Meteo HRRR/GFS 预测获取
    4) 概率建模、edge 计算、交易信号
    5) 本地私钥签名下单 + 容错重试
    """

    # Gamma 聚合 API（用于发现市场）
    GAMMA_BASE = "https://gamma-api.polymarket.com"
    # CLOB 交易 API（用于拉价格和下单）
    CLOB_HOST = "https://clob.polymarket.com"
    # Data API（用于拉账户持仓）
    DATA_API_BASE = "https://data-api.polymarket.com"
    # Open-Meteo 主接口（优先带 models=hrrr）
    OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"
    # Open-Meteo GFS 回退接口
    OPEN_METEO_GFS = "https://api.open-meteo.com/v1/gfs"
    # NWS（美国国家气象局）网格 API
    NWS_POINTS_API = "https://api.weather.gov/points"
    # AviationWeather（航空气象）API
    AVIATION_WEATHER_API = "https://aviationweather.gov/api/data"
    # 纽约时区，用于与 Polymarket 天气市场日期规则对齐
    NYC_TZ = pytz.timezone("America/New_York")

    # 常见天气城市配置（市场发现关键词 + 预测坐标）
    # temp_unit:
    # - fahrenheit: 适配美国城市常见 F 区间市场
    # - celsius: 适配国际城市常见 C 区间市场
    COMMON_CITIES: List[Dict[str, Any]] = [
        {
            "name": "New York",
            "aliases": ["NYC", "New York City", "New York", "纽约", "紐約"],
            "lat": 40.7769,   # LGA
            "lon": -73.8740,
            "temp_unit": "fahrenheit",
            "timezone": "America/New_York",
            "base_sigma": 2.2,
        },
        {
            "name": "Miami",
            "aliases": ["Miami", "迈阿密", "邁阿密"],
            "lat": 25.7617,
            "lon": -80.1918,
            "temp_unit": "fahrenheit",
            "timezone": "America/New_York",
            "base_sigma": 1.2,
        },
        {
            "name": "Chicago",
            "aliases": ["Chicago", "芝加哥"],
            "lat": 41.8781,
            "lon": -87.6298,
            "temp_unit": "fahrenheit",
            "timezone": "America/Chicago",
            "base_sigma": 2.8,
        },
        {
            "name": "Atlanta",
            "aliases": ["Atlanta", "亚特兰大", "亞特蘭大"],
            "lat": 33.7490,
            "lon": -84.3880,
            "temp_unit": "fahrenheit",
            "timezone": "America/New_York",
            "base_sigma": 2.0,
        },
        {
            "name": "Toronto",
            "aliases": ["Toronto", "多伦多", "多倫多"],
            "lat": 43.6532,
            "lon": -79.3832,
            "temp_unit": "celsius",
            "timezone": "America/Toronto",
            "base_sigma": 2.4,
        },
        {
            "name": "London",
            "aliases": ["London", "伦敦", "倫敦"],
            "lat": 51.5072,
            "lon": -0.1276,
            "temp_unit": "celsius",
            "timezone": "Europe/London",
            "base_sigma": 2.8,
        },
        {
            "name": "Paris",
            "aliases": ["Paris", "巴黎"],
            "lat": 48.8566,
            "lon": 2.3522,
            "temp_unit": "celsius",
            "timezone": "Europe/Paris",
            "base_sigma": 2.4,
        },
        {
            "name": "Brussels",
            "aliases": ["Brussels", "布鲁塞尔", "布魯塞爾"],
            "lat": 50.8503,
            "lon": 4.3517,
            "temp_unit": "celsius",
            "timezone": "Europe/Brussels",
            "base_sigma": 2.3,
        },
        {
            "name": "Vienna",
            "aliases": ["Vienna", "维也纳", "維也納"],
            "lat": 48.2082,
            "lon": 16.3738,
            "temp_unit": "fahrenheit",
            "timezone": "Europe/Vienna",
            "base_sigma": 2.2,
        },
        {
            "name": "Seoul",
            "aliases": ["Seoul", "首尔", "首爾"],
            "lat": 37.5665,
            "lon": 126.9780,
            "temp_unit": "celsius",
            "timezone": "Asia/Seoul",
            "base_sigma": 2.4,
        },
        {
            "name": "Tokyo",
            "aliases": ["Tokyo", "东京", "東京"],
            "lat": 35.6762,
            "lon": 139.6503,
            "temp_unit": "celsius",
            "timezone": "Asia/Tokyo",
            "base_sigma": 2.2,
        },
        {
            "name": "Dallas",
            "aliases": ["Dallas", "达拉斯", "達拉斯"],
            "lat": 32.7767,
            "lon": -96.7970,
            "temp_unit": "fahrenheit",
            "timezone": "America/Chicago",
            "base_sigma": 2.5,
        },
        {
            "name": "Ankara",
            "aliases": ["Ankara", "安卡拉"],
            "lat": 39.9334,
            "lon": 32.8597,
            "temp_unit": "celsius",
            "timezone": "Europe/Istanbul",
            "base_sigma": 2.4,
        },
        {
            "name": "Buenos Aires",
            "aliases": ["Buenos Aires", "布宜诺斯艾利斯", "布宜諾斯艾利斯"],
            "lat": -34.6037,
            "lon": -58.3816,
            "temp_unit": "celsius",
            "timezone": "America/Argentina/Buenos_Aires",
            "base_sigma": 2.2,
        },
        {
            "name": "Wellington",
            "aliases": ["Wellington", "惠灵顿", "惠靈頓"],
            "lat": -41.2865,
            "lon": 174.7762,
            "temp_unit": "celsius",
            "timezone": "Pacific/Auckland",
            "base_sigma": 2.3,
        },
        {
            "name": "Sydney",
            "aliases": ["Sydney", "悉尼", "雪梨"],
            "lat": -33.8688,
            "lon": 151.2093,
            "temp_unit": "celsius",
            "timezone": "Australia/Sydney",
            "base_sigma": 2.1,
        },
        {
            "name": "Seattle",
            "aliases": ["Seattle", "西雅图", "西雅圖"],
            "lat": 47.6062,
            "lon": -122.3321,
            "temp_unit": "fahrenheit",
            "timezone": "America/Los_Angeles",
            "base_sigma": 2.6,
        },
        {
            "name": "Sao Paulo",
            "aliases": ["Sao Paulo", "Sao-Paulo", "São Paulo", "圣保罗", "聖保羅"],
            "lat": -23.5505,
            "lon": -46.6333,
            "temp_unit": "celsius",
            "timezone": "America/Sao_Paulo",
            "base_sigma": 2.0,
        },
        {
            "name": "Munich",
            "aliases": ["Munich", "Muenchen", "慕尼黑"],
            "lat": 48.1351,
            "lon": 11.5820,
            "temp_unit": "celsius",
            "timezone": "Europe/Berlin",
            "base_sigma": 2.4,
        },
        {
            "name": "Lucknow",
            "aliases": ["Lucknow", "勒克瑙", "勒克瑙"],
            "lat": 26.8467,
            "lon": 80.9462,
            "temp_unit": "celsius",
            "timezone": "Asia/Kolkata",
            "base_sigma": 2.6,
        },
    ]
    # 各城市默认航空站点（ICAO），用于 METAR/TAF 实况辅助
    CITY_ICAO_MAP: Dict[str, str] = {
        "new york": "KLGA",
        "miami": "KMIA",
        "chicago": "KORD",
        "atlanta": "KATL",
        "toronto": "CYYZ",
        "london": "EGLL",
        "paris": "LFPG",
        "brussels": "EBBR",
        "vienna": "LOWW",
        "seoul": "RKSI",
        "tokyo": "RJTT",
        "dallas": "KDFW",
        "ankara": "LTAC",
        "buenos aires": "SAEZ",
        "wellington": "NZWN",
        "sydney": "YSSY",
        "seattle": "KSEA",
        "sao paulo": "SBGR",
        "munich": "EDDM",
        "lucknow": "VILK",
    }

    def __init__(
        self,
        private_key: str,
        signature_type: int = 0,
        funder: Optional[str] = None,
        investment_usdc: float = 1.0,
        bankroll_fraction: float = 0.30,
        edge_threshold: float = 0.10,
        min_fair_prob: float = 0.15,
        min_market_price: float = 0.10,
        max_market_price: float = 0.70,
        min_edge_ratio: float = 1.25,
        min_confidence: float = 0.60,
        min_confidence_score: float = 0.45,
        temp_sigma_f: float = 2.0,
        stability_prob_threshold: float = 0.70,
        boundary_buffer_f: float = 0.8,
        max_position_shares_per_token: float = 250.0,
        max_total_position_shares_per_condition: float = 1200.0,
        min_position_to_sell: float = 1.0,
        reduce_fraction_on_negative_edge: float = 0.5,
        capital_utilization_per_trade: float = 0.30,
        capital_reserve_usdc: float = 0.5,
        min_trade_usdc: float = 0.3,
        max_trade_usdc: float = 2.0,
        exchange_min_buy_usdc: float = 1.0,
        max_token_exposure_ratio: float = 0.45,
        max_condition_exposure_ratio: float = 0.85,
        use_limit_buy_order: bool = True,
        limit_buy_slippage_tolerance: float = 0.02,
        take_profit_ratio: float = 1.12,
        stop_loss_ratio: float = 0.96,
        take_profit_sell_fraction: float = 0.8,
        min_hours_to_settlement_for_entry: float = 12.0,
        single_outcome_per_condition: bool = True,
        pre_settle_hours: float = 18.0,
        pre_settle_min_pnl_ratio: float = 0.05,
        pre_settle_reduce_fraction: float = 0.5,
        enable_daily_loss_standby: bool = True,
        daily_loss_limit_ratio: float = 0.30,
        dust_notional_threshold_usdc: float = 1.0,
        standby_force_exit_min_notional: float = 0.2,
        sell_limit_discount: float = 0.01,
        enable_synthetic_close_dust: bool = True,
        synthetic_close_min_notional_usdc: float = 1.0,
        synthetic_close_max_notional_usdc: float = 1.0,
        model_shift_exit_delta: float = 0.15,
        total_exposure_limit: float = 0.80,
        positions_cost_file: str = "positions_cost.json",
        opposite_token_map_file: str = "opposite_token_map.json",
        synthetic_hedge_state_file: str = "synthetic_hedge_state.json",
        daily_realized_pnl_file: str = "daily_realized_pnl.json",
        fair_prob_state_file: str = "fair_prob_state.json",
        source_reliability_file: str = "source_reliability.json",
        enable_db_dual_write: bool = False,
        db_host: str = "127.0.0.1",
        db_port: int = 3306,
        db_user: str = "root",
        db_password: str = "root",
        db_name: str = "quantify",
        db_connect_timeout_s: int = 5,
        report_dir: str = "reports",
        write_static_report: bool = True,
        write_history_report: bool = True,
        history_index_file: str = "history_index.json",
        cities: Optional[List[str]] = None,
        request_timeout_s: int = 20,
        dry_run: bool = False,
    ) -> None:
        # 策略与风控参数
        self.private_key = private_key
        # 账户签名模式（0=EOA；邮箱/代理钱包通常需要 1 或 2）
        self.signature_type = int(signature_type)
        # 资金方地址（代理钱包/托管账户需要）
        self.funder = (funder or "").strip() or None
        # 单笔投入 USDC
        self.investment_usdc = investment_usdc
        # 凯利下注资金使用比例（例如 0.2 表示只用资金的 20%）
        self.bankroll_fraction = min(1.0, max(0.01, bankroll_fraction))
        # 触发买入的最小 edge（fair_prob - market_price）
        self.edge_threshold = edge_threshold
        # 最低公平概率（防彩票单）
        self.min_fair_prob = max(0.0, min(1.0, min_fair_prob))
        # 最低市场价格（防极低流动性低价单）
        self.min_market_price = max(0.0, min(1.0, min_market_price))
        # 最高市场价格（防过度拥挤高价单）
        self.max_market_price = max(self.min_market_price, min(1.0, max_market_price))
        # 最低相对优势倍数 fair_prob/market_price
        self.min_edge_ratio = max(1.0, min_edge_ratio)
        # 最低置信度，低于该值不新开仓（新参数）
        self.min_confidence = min(1.0, max(0.0, float(min_confidence)))
        # 兼容旧字段：保持同值，避免历史代码路径引用报错
        self.min_confidence_score = self.min_confidence
        # 温度预测分布的标准差（华氏度）
        self.temp_sigma_f = temp_sigma_f
        # “稳定落入区间”的最小概率阈值
        self.stability_prob_threshold = stability_prob_threshold
        # 距离区间边界的安全缓冲（防边界抖动）
        self.boundary_buffer_f = boundary_buffer_f
        # 单个 token 最大允许持仓（份额）
        self.max_position_shares_per_token = max_position_shares_per_token
        # 单个 condition（同一天市场）下所有 outcome 合计最大持仓（份额）
        self.max_total_position_shares_per_condition = max_total_position_shares_per_condition
        # 小于该份额不触发卖出（避免微小 dust 反复交易）
        self.min_position_to_sell = min_position_to_sell
        # 负 edge 时的减仓比例（0.5 表示卖出一半仓位）
        self.reduce_fraction_on_negative_edge = reduce_fraction_on_negative_edge
        # 每笔最多使用可用资金比例（小资金场景下自动缩放）
        self.capital_utilization_per_trade = capital_utilization_per_trade
        # 保留不参与交易的 USDC（避免资金见底）
        self.capital_reserve_usdc = capital_reserve_usdc
        # 最小下单金额（Polymarket 硬规则：$1）
        self.min_trade_usdc = max(1.0, float(min_trade_usdc))
        # 单笔上限保护（小资金账户）
        self.max_trade_usdc = max(0.1, max_trade_usdc)
        # 交易所硬限制（market buy 最小金额，固定 $1，不允许下调）
        self.exchange_min_buy_usdc = 1.0
        # 单个 token 最大资金暴露比例（相对可用 USDC）
        self.max_token_exposure_ratio = max_token_exposure_ratio
        # 单个 condition 最大资金暴露比例（相对可用 USDC）
        self.max_condition_exposure_ratio = max_condition_exposure_ratio
        # BUY 是否优先使用限价单（FOK），降低天气市场滑点
        self.use_limit_buy_order = use_limit_buy_order
        # 限价容忍度（例如 0.02 = 在买一价基础上上浮 2%）
        self.limit_buy_slippage_tolerance = max(0.0, min(0.1, limit_buy_slippage_tolerance))
        # 止盈触发倍数（默认 +20%）
        self.take_profit_ratio = max(1.01, take_profit_ratio)
        # 止损触发倍数（默认 -10%）
        self.stop_loss_ratio = min(0.99, max(0.5, stop_loss_ratio))
        # 止盈分批卖出比例（默认卖出 50%）
        self.take_profit_sell_fraction = min(1.0, max(0.1, take_profit_sell_fraction))
        # 入场时间过滤：距结算小于该小时数，不开新仓
        self.min_hours_to_settlement_for_entry = max(0.0, float(min_hours_to_settlement_for_entry))
        # 同一 condition 默认只持有一个 outcome（避免自相对冲）
        self.single_outcome_per_condition = bool(single_outcome_per_condition)
        # 临近结算防守：距离结算小于该小时数且收益未达阈值，触发减仓/清仓
        self.pre_settle_hours = max(1.0, float(pre_settle_hours))
        self.pre_settle_min_pnl_ratio = max(-1.0, float(pre_settle_min_pnl_ratio))
        self.pre_settle_reduce_fraction = min(1.0, max(0.1, float(pre_settle_reduce_fraction)))
        # 卖出限价相对买一价折让比例（默认 1%）
        self.sell_limit_discount = min(0.05, max(0.001, sell_limit_discount))
        # Dust 仓位合成平仓（对侧对冲）开关与名义限额
        self.enable_synthetic_close_dust = bool(enable_synthetic_close_dust)
        self.synthetic_close_min_notional_usdc = max(1.0, float(synthetic_close_min_notional_usdc))
        self.synthetic_close_max_notional_usdc = max(
            self.synthetic_close_min_notional_usdc,
            float(synthetic_close_max_notional_usdc),
        )
        # 模型突变平仓阈值：当公平概率较上一轮下降超过该值，且 edge<=0 时平仓
        self.model_shift_exit_delta = min(0.9, max(0.01, float(model_shift_exit_delta)))
        # Dust 仓位定义阈值（用于更激进卖出）
        self.dust_notional_threshold_usdc = max(0.05, float(dust_notional_threshold_usdc))
        # Standby 平仓最小名义价值（低于该值可能为链上尘埃仓）
        self.standby_force_exit_min_notional = max(0.01, float(standby_force_exit_min_notional))
        # 日内亏损熔断参数：超阈值进入待机，仅做减仓/平仓
        self.enable_daily_loss_standby = bool(enable_daily_loss_standby)
        self.daily_loss_limit_ratio = min(0.95, max(0.05, float(daily_loss_limit_ratio)))
        # 全局总暴露上限（持仓市值 / 账户总权益）
        self.total_exposure_limit = min(0.95, max(0.1, total_exposure_limit))
        # 报告输出目录（JSON + HTML）
        self.report_dir = Path(report_dir)
        # 高频诊断日志文件（每轮覆盖写入最新完整决策上下文）
        self.diagnostics_file = self.report_dir / "diagnostics.json"
        # 是否每轮写静态报告
        self.write_static_report = write_static_report
        # 是否写历史快照与历史索引
        self.write_history_report = write_history_report
        # 历史索引文件（按日期倒序）
        self.history_index_file = history_index_file
        # 交易城市列表（不传则使用默认常见城市）
        self.city_configs = self._resolve_city_configs(cities)
        # 城市扫描轮转游标，避免每轮都让前几个城市先占用预算
        self._city_scan_cursor = 0
        self.request_timeout_s = request_timeout_s
        # dry_run=True 时只打信号不真实下单
        self.dry_run = dry_run
        # 数据双写（JSON + MySQL）
        self.enable_db_dual_write = bool(enable_db_dual_write)
        self.db_host = str(db_host or "127.0.0.1")
        self.db_port = int(db_port or 3306)
        self.db_user = str(db_user or "root")
        self.db_password = str(db_password or "")
        self.db_name = str(db_name or "quantify")
        self.db_connect_timeout_s = max(1, int(db_connect_timeout_s))
        self._db_ready = False

        # HTTP 会话复用（减少连接开销）
        self.session = requests.Session()
        # CLOB 客户端：使用 Polygon + 本地私钥
        self.client = ClobClient(
            self.CLOB_HOST,
            key=private_key,
            chain_id=POLYGON,
            signature_type=self.signature_type,
            funder=self.funder,
        )
        # 初始化/刷新 API 凭证
        self._refresh_api_creds()
        # 保存最近一次市场发现调试信息，供报告页面展示
        self.last_discovery_debug: List[Dict[str, Any]] = []
        # Weather 事件页缓存（避免每个城市重复全量拉取）
        self._weather_events_cache: Dict[str, Dict[str, Any]] = {}
        self._weather_events_cache_ts: float = 0.0
        # token 持仓成本（加权平均），结构：{token_id: {"avg_price": float, "shares": float}}
        self.positions_cost_file = self.report_dir / positions_cost_file
        self.positions_cost: Dict[str, Dict[str, float]] = {}
        self._load_positions_cost()
        # token 对侧映射（YES<->NO），用于 dust 合成平仓
        self.opposite_token_map_file = self.report_dir / opposite_token_map_file
        self.opposite_token_map: Dict[str, str] = {}
        self._load_opposite_token_map()
        # 合成平仓对冲状态：记录每个 token 已对冲份额，避免重复无效操作
        self.synthetic_hedge_state_file = self.report_dir / synthetic_hedge_state_file
        self.synthetic_hedge_state: Dict[str, Dict[str, float]] = {}
        self._load_synthetic_hedge_state()
        # 日内已实现盈亏，结构：{YYYY-MM-DD: pnl_usdc}
        self.daily_realized_pnl_file = self.report_dir / daily_realized_pnl_file
        self.daily_realized_pnl: Dict[str, float] = {}
        self._load_daily_realized_pnl()
        # token 公平概率状态缓存（用于检测模型概率突变）
        self.fair_prob_state_file = self.report_dir / fair_prob_state_file
        self.fair_prob_state: Dict[str, float] = {}
        self._load_fair_prob_state()
        # 多源可靠度状态缓存（0~1），用于融合动态加权
        self.source_reliability_file = self.report_dir / source_reliability_file
        self.source_reliability: Dict[str, float] = {}
        self._load_source_reliability()
        # 启动时回填一次真实持仓，避免脚本重启后看不到历史仓位成本
        self._bootstrap_positions_cost_from_live_positions()
        # Open-Meteo 预测模型选择缓存（优先 HRRR，否则 gfs_seamless）
        self._forecast_model_name: Optional[str] = None
        # 当前轮使用的模型来源（hrrr/fallback/gfs）
        self.current_model_source: str = "unknown"
        # 当前轮预测源明细（便于日志/排查）
        self.current_model_details: Dict[str, Any] = {}
        # 多源融合权重（可按可用源自动归一化）
        self.weather_source_weights: Dict[str, float] = {
            "openmeteo_hrrr": 0.35,
            "openmeteo_gfs_seamless": 0.20,
            "openmeteo_best_match": 0.15,
            "openmeteo_fallback": 0.12,
            "openmeteo_gfs": 0.10,
            "nws": 0.25,
            "metar": 0.08,
        }
        # NWS 对 User-Agent 有要求
        self.weather_http_headers: Dict[str, str] = {
            "User-Agent": "polymarket-weather-master/3.0 (local-bot; contact=codex@example.com)",
            "Accept": "application/geo+json, application/json",
        }
        # token -> city 轻量缓存（用于交易流水写库时反查城市）
        self.token_city_hint_map: Dict[str, str] = {}
        self._setup_db_if_enabled()

    def _setup_db_if_enabled(self) -> None:
        """初始化数据库双写能力（失败不影响主流程）。"""
        if not self.enable_db_dual_write:
            self._db_ready = False
            return
        if pymysql is None:
            LOGGER.warning("DB dual-write enabled but PyMySQL not installed. Disable DB writer.")
            self._db_ready = False
            return
        try:
            with pymysql.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name,
                charset="utf8mb4",
                connect_timeout=self.db_connect_timeout_s,
                autocommit=True,
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                self._init_db(conn)
            self._db_ready = True
            LOGGER.info("DB dual-write ready: %s:%s/%s", self.db_host, self.db_port, self.db_name)
        except Exception as exc:
            self._db_ready = False
            LOGGER.warning("DB dual-write init failed: %s", exc)

    def _init_db(self, conn) -> None:
        """初始化（或升级）数据库表结构。"""
        ddl_list = [
            """
            CREATE TABLE IF NOT EXISTS fact_bot_actions (
              id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              event_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
              city VARCHAR(50) NULL COMMENT '城市',
              date_label VARCHAR(50) NULL COMMENT '日期标签',
              condition_id TEXT NULL COMMENT '条件ID',
              token_id VARCHAR(100) NULL COMMENT 'Token ID',
              trade_signal VARCHAR(20) NULL COMMENT '信号(BUY/HOLD/REDUCE)',
              market_price DOUBLE NULL COMMENT '市场价格',
              fair_prob DOUBLE NULL COMMENT '公平概率',
              edge DOUBLE NULL COMMENT '边际优势',
              confidence_score DOUBLE NULL COMMENT '置信度',
              disagreement_index DOUBLE NULL COMMENT '分歧指数',
              dynamic_buy_usdc DOUBLE NULL COMMENT '动态买入金额(USDC)',
              hold_reason TEXT NULL COMMENT '持有原因',
              PRIMARY KEY (id),
              KEY idx_fact_actions_time (event_ts),
              KEY idx_fact_actions_city_date (city, date_label),
              KEY idx_fact_actions_signal (trade_signal),
              KEY idx_fact_actions_token (token_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_bot_diagnostics (
              id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              event_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
              city VARCHAR(50) NULL COMMENT '城市',
              date_label VARCHAR(50) NULL COMMENT '日期标签',
              forecast_max DOUBLE NULL COMMENT '预测最高温',
              confidence_score DOUBLE NULL COMMENT '置信度',
              disagreement_index DOUBLE NULL COMMENT '分歧指数',
              yes_token_id VARCHAR(100) NULL COMMENT 'YES Token ID',
              yes_market_price DOUBLE NULL COMMENT 'YES市场价',
              yes_fair_prob DOUBLE NULL COMMENT 'YES公平概率',
              yes_edge DOUBLE NULL COMMENT 'YES边际优势',
              no_token_id VARCHAR(100) NULL COMMENT 'NO Token ID',
              no_market_price DOUBLE NULL COMMENT 'NO市场价',
              no_fair_prob DOUBLE NULL COMMENT 'NO公平概率',
              no_edge DOUBLE NULL COMMENT 'NO边际优势',
              PRIMARY KEY (id),
              KEY idx_dim_diag_time (event_ts),
              KEY idx_dim_diag_city_date (city, date_label)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """,
            """
            CREATE TABLE IF NOT EXISTS trade_history (
              id INT AUTO_INCREMENT PRIMARY KEY,
              `timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
              bot_action VARCHAR(20),
              token_id VARCHAR(100),
              city VARCHAR(50),
              price DOUBLE,
              shares DOUBLE,
              notional DOUBLE,
              raw_json JSON
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """,
        ]
        with conn.cursor() as cur:
            for ddl in ddl_list:
                cur.execute(ddl)
            # 老版本兼容升级：把可能与关键字冲突的旧列名改为非关键字风格
            try:
                cur.execute("SHOW COLUMNS FROM fact_bot_actions LIKE 'timestamp'")
                if cur.fetchone():
                    cur.execute(
                        """
                        ALTER TABLE fact_bot_actions
                        CHANGE COLUMN `timestamp` event_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间'
                        """
                    )
            except Exception:
                pass
            try:
                cur.execute("SHOW COLUMNS FROM fact_bot_actions LIKE 'signal'")
                if cur.fetchone():
                    cur.execute(
                        """
                        ALTER TABLE fact_bot_actions
                        CHANGE COLUMN `signal` trade_signal VARCHAR(20) NULL COMMENT '信号(BUY/HOLD/REDUCE)'
                        """
                    )
            except Exception:
                pass
            try:
                cur.execute("SHOW COLUMNS FROM dim_bot_diagnostics LIKE 'timestamp'")
                if cur.fetchone():
                    cur.execute(
                        """
                        ALTER TABLE dim_bot_diagnostics
                        CHANGE COLUMN `timestamp` event_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间'
                        """
                    )
            except Exception:
                pass
            try:
                cur.execute("SHOW TABLES LIKE 'trade_history'")
                if cur.fetchone():
                    cur.execute("SHOW COLUMNS FROM trade_history LIKE 'action'")
                    if cur.fetchone():
                        cur.execute(
                            """
                            ALTER TABLE trade_history
                            CHANGE COLUMN `action` bot_action VARCHAR(20)
                            """
                        )
            except Exception:
                pass

    def _db_connect(self):
        """创建数据库连接（内部使用）。"""
        if not self.enable_db_dual_write or not self._db_ready or pymysql is None:
            return None
        return pymysql.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password,
            database=self.db_name,
            charset="utf8mb4",
            connect_timeout=self.db_connect_timeout_s,
            autocommit=True,
        )

    def _mysql_execute(self, sql: str, params: Tuple[Any, ...]) -> None:
        """执行单条 MySQL 语句（异常抛给上层处理）。"""
        conn = self._db_connect()
        if conn is None:
            return
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

    def _remember_token_city(self, token_id: str, city: str) -> None:
        """记录 token 与城市映射，供成交写库时兜底反查。"""
        tk = str(token_id or "").strip()
        ct = str(city or "").strip()
        if tk and ct:
            self.token_city_hint_map[tk] = ct

    def _resolve_city_by_token(self, token_id: str) -> str:
        """通过 token 反查城市，查不到返回空字符串。"""
        tk = str(token_id or "").strip()
        if not tk:
            return ""
        return str(self.token_city_hint_map.get(tk, "") or "")

    @staticmethod
    def _safe_float(v: Any) -> Optional[float]:
        try:
            if v in ("", None):
                return None
            return float(v)
        except Exception:
            return None

    @staticmethod
    def _safe_int(v: Any) -> Optional[int]:
        try:
            if v in ("", None):
                return None
            return int(v)
        except Exception:
            return None

    @staticmethod
    def _truncate_text(v: Any, max_len: int) -> Optional[str]:
        s = str(v or "").strip()
        if not s:
            return None
        if len(s) > max_len:
            return s[:max_len]
        return s

    def _db_write_run_actions(
        self,
        generated_at: str,
        generated_at_iso: str,
        date_key: str,
        actions: List[Dict[str, Any]],
        run_summary: Dict[str, Any],
        source_file: str = "",
        progress: Optional[Dict[str, Any]] = None,
        payload_json: Optional[str] = None,
    ) -> None:
        """把一次 run 的 summary + actions 写入 MySQL。"""
        if not self.enable_db_dual_write or not self._db_ready:
            return
        phase = ""
        if isinstance(progress, dict):
            phase = str(progress.get("stage", "") or "")
        run_uid = f"{generated_at_iso}|{source_file or 'latest'}|{phase or 'final'}"
        sig = run_summary.get("signal_summary", {}) if isinstance(run_summary, dict) else {}
        dis = run_summary.get("discovery_summary", {}) if isinstance(run_summary, dict) else {}
        progress = progress if isinstance(progress, dict) else {}
        try:
            conn = self._db_connect()
            if conn is None:
                return
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO bot_runs (
                          run_uid, generated_at_iso, generated_at_local, date_key, source_file,
                          total_rows, buy_count, hold_count, reduce_count, discovery_found, discovery_skip,
                          progress_stage, progress_city, progress_city_index, progress_total_cities, payload_json
                        ) VALUES (
                          %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                        )
                        ON DUPLICATE KEY UPDATE
                          generated_at_local=VALUES(generated_at_local),
                          date_key=VALUES(date_key),
                          source_file=VALUES(source_file),
                          total_rows=VALUES(total_rows),
                          buy_count=VALUES(buy_count),
                          hold_count=VALUES(hold_count),
                          reduce_count=VALUES(reduce_count),
                          discovery_found=VALUES(discovery_found),
                          discovery_skip=VALUES(discovery_skip),
                          progress_stage=VALUES(progress_stage),
                          progress_city=VALUES(progress_city),
                          progress_city_index=VALUES(progress_city_index),
                          progress_total_cities=VALUES(progress_total_cities),
                          payload_json=VALUES(payload_json)
                        """,
                        (
                            run_uid,
                            generated_at_iso,
                            generated_at,
                            date_key,
                            source_file,
                            self._safe_int(run_summary.get("total_rows") if isinstance(run_summary, dict) else None),
                            self._safe_int(sig.get("BUY") if isinstance(sig, dict) else None),
                            self._safe_int(sig.get("HOLD") if isinstance(sig, dict) else None),
                            self._safe_int(sig.get("REDUCE") if isinstance(sig, dict) else None),
                            self._safe_int(dis.get("FOUND") if isinstance(dis, dict) else None),
                            self._safe_int(dis.get("SKIP") if isinstance(dis, dict) else None),
                            str(progress.get("stage") or "") or None,
                            str(progress.get("city") or "") or None,
                            self._safe_int(progress.get("city_index")),
                            self._safe_int(progress.get("total_cities")),
                            payload_json,
                        ),
                    )
                    cur.execute("SELECT id FROM bot_runs WHERE run_uid=%s LIMIT 1", (run_uid,))
                    row = cur.fetchone()
                    if not row:
                        return
                    run_id = int(row[0])
                    cur.execute("DELETE FROM bot_actions WHERE run_id=%s", (run_id,))
                    if actions:
                        sql = """
                            INSERT INTO bot_actions (
                              run_id, action_index, city, action_date, date_label, action_signal, action_side, label,
                              token_id, opposite_token_id, condition_id, question,
                              market_price, fair_prob, edge, edge_ratio, hold_reason, exit_reason, raw_json
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """
                        rows = []
                        for idx, act in enumerate(actions):
                            if not isinstance(act, dict):
                                continue
                            rows.append(
                                (
                                    run_id,
                                    idx,
                                    str(act.get("city") or "") or None,
                                    str(act.get("date") or "") or None,
                                    str(act.get("date_label") or "") or None,
                                    str(act.get("signal") or "") or None,
                                    str(act.get("side") or "") or None,
                                    str(act.get("label") or "") or None,
                                    str(act.get("token_id") or "") or None,
                                    str(act.get("opposite_token_id") or "") or None,
                                    str(act.get("condition_id") or "") or None,
                                    str(act.get("question") or "") or None,
                                    self._safe_float(act.get("market_price")),
                                    self._safe_float(act.get("fair_prob")),
                                    self._safe_float(act.get("edge")),
                                    self._safe_float(act.get("edge_ratio")),
                                    str(act.get("hold_reason") or "") or None,
                                    str(act.get("exit_reason") or "") or None,
                                    json.dumps(act, ensure_ascii=False),
                                )
                            )
                        if rows:
                            cur.executemany(sql, rows)
        except Exception as exc:
            LOGGER.warning("DB write run/actions failed: %s", exc)

    def _db_write_diagnostics(self, payload: Dict[str, Any]) -> None:
        """把 diagnostics.json 同步写入 MySQL。"""
        if not self.enable_db_dual_write or not self._db_ready or not isinstance(payload, dict):
            return
        generated_at_iso = str(payload.get("generated_at_iso") or "")
        if not generated_at_iso:
            return
        generated_at = str(payload.get("generated_at") or "")
        mode = str(payload.get("mode") or "")
        sig = payload.get("signal_summary", {}) if isinstance(payload.get("signal_summary"), dict) else {}
        rows = payload.get("rows", [])
        diag_uid = f"{generated_at_iso}|{mode or 'unknown'}"
        try:
            conn = self._db_connect()
            if conn is None:
                return
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO bot_diagnostics (
                          diag_uid, generated_at_iso, generated_at_local, mode,
                          buy_count, hold_count, reduce_count, rows_count, payload_json
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          generated_at_local=VALUES(generated_at_local),
                          mode=VALUES(mode),
                          buy_count=VALUES(buy_count),
                          hold_count=VALUES(hold_count),
                          reduce_count=VALUES(reduce_count),
                          rows_count=VALUES(rows_count),
                          payload_json=VALUES(payload_json)
                        """,
                        (
                            diag_uid,
                            generated_at_iso,
                            generated_at,
                            mode,
                            self._safe_int(sig.get("BUY") if isinstance(sig, dict) else None),
                            self._safe_int(sig.get("HOLD") if isinstance(sig, dict) else None),
                            self._safe_int(sig.get("REDUCE") if isinstance(sig, dict) else None),
                            len(rows) if isinstance(rows, list) else 0,
                            json.dumps(payload, ensure_ascii=False),
                        ),
                    )
        except Exception as exc:
            LOGGER.warning("DB write diagnostics failed: %s", exc)

    def _write_actions_to_db(self, actions: List[Dict[str, Any]]) -> None:
        """扁平化写入 fact_bot_actions（不影响交易主流程）。"""
        if not self.enable_db_dual_write or not self._db_ready:
            return
        if not isinstance(actions, list) or not actions:
            return
        rows: List[Tuple[Any, ...]] = []
        for act in actions:
            if not isinstance(act, dict):
                continue
            try:
                self._remember_token_city(str(act.get("token_id") or ""), str(act.get("city") or ""))
                rows.append(
                    (
                        self._truncate_text(act.get("city"), 50),
                        self._truncate_text(act.get("date_label"), 50),
                        self._truncate_text(act.get("condition_id"), 255),
                        self._truncate_text(act.get("token_id"), 100),
                        self._truncate_text(act.get("signal"), 20),
                        self._safe_float(act.get("market_price")),
                        self._safe_float(act.get("fair_prob")),
                        self._safe_float(act.get("edge")),
                        self._safe_float(act.get("confidence_score")),
                        self._safe_float(act.get("disagreement_index")),
                        self._safe_float(act.get("dynamic_buy_usdc")),
                        self._truncate_text(act.get("hold_reason"), 65535),
                    )
                )
            except Exception as exc:
                LOGGER.warning("Flatten action row parse failed, skip one row: %s", exc)
                continue
        if not rows:
            return
        try:
            conn = self._db_connect()
            if conn is None:
                return
            with conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO fact_bot_actions (
                          city, date_label, condition_id, token_id, trade_signal,
                          market_price, fair_prob, edge, confidence_score, disagreement_index,
                          dynamic_buy_usdc, hold_reason
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        rows,
                    )
        except Exception as exc:
            LOGGER.warning("Flatten actions DB write failed: %s", exc)

    def _write_diagnostics_to_db(self, diagnostics_payload: Dict[str, Any]) -> None:
        """扁平化写入 dim_bot_diagnostics（不影响交易主流程）。"""
        if not self.enable_db_dual_write or not self._db_ready:
            return
        if not isinstance(diagnostics_payload, dict):
            return
        rows_payload = diagnostics_payload.get("rows", [])
        if not isinstance(rows_payload, list) or not rows_payload:
            return
        rows: List[Tuple[Any, ...]] = []
        for item in rows_payload:
            if not isinstance(item, dict):
                continue
            try:
                yes = item.get("yes", {}) if isinstance(item.get("yes"), dict) else {}
                no = item.get("no", {}) if isinstance(item.get("no"), dict) else {}
                rows.append(
                    (
                        self._truncate_text(item.get("city"), 50),
                        self._truncate_text(item.get("date_label"), 50),
                        self._safe_float(item.get("forecast_max")),
                        self._safe_float(item.get("confidence_score")),
                        self._safe_float(item.get("disagreement_index")),
                        self._truncate_text(yes.get("token_id"), 100),
                        self._safe_float(yes.get("market_price")),
                        self._safe_float(yes.get("fair_prob")),
                        self._safe_float(yes.get("edge")),
                        self._truncate_text(no.get("token_id"), 100),
                        self._safe_float(no.get("market_price")),
                        self._safe_float(no.get("fair_prob")),
                        self._safe_float(no.get("edge")),
                    )
                )
            except Exception as exc:
                LOGGER.warning("Flatten diagnostics row parse failed, skip one row: %s", exc)
                continue
        if not rows:
            return
        try:
            conn = self._db_connect()
            if conn is None:
                return
            with conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO dim_bot_diagnostics (
                          city, date_label, forecast_max, confidence_score, disagreement_index,
                          yes_token_id, yes_market_price, yes_fair_prob, yes_edge,
                          no_token_id, no_market_price, no_fair_prob, no_edge
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        rows,
                    )
        except Exception as exc:
            LOGGER.warning("Flatten diagnostics DB write failed: %s", exc)

    def _write_trade_to_db(
        self,
        bot_action: str,
        token_id: str,
        city: str,
        price: float,
        shares: float,
        notional: float,
        raw_data: Dict[str, Any],
    ) -> None:
        """专门用于记录真实发生的买卖交易流水。"""
        if not self.enable_db_dual_write or not self._db_ready:
            return
        sql = """
            INSERT INTO trade_history
            (bot_action, token_id, city, price, shares, notional, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            token_id_s = str(token_id or "").strip()
            city_s = str(city or "").strip() or self._resolve_city_by_token(token_id_s)
            raw_json_str = json.dumps(raw_data, ensure_ascii=False) if raw_data else "{}"
            self._mysql_execute(
                sql,
                (
                    str(bot_action or ""),
                    token_id_s,
                    city_s,
                    self._safe_float(price),
                    self._safe_float(shares),
                    self._safe_float(notional),
                    raw_json_str,
                ),
            )
        except Exception as e:
            LOGGER.warning("[MySQL] 写入 trade_history 失败: %s", e)

    def _load_positions_cost(self) -> None:
        """从本地 JSON 读取持仓成本，避免重启后丢失。"""
        try:
            if self.positions_cost_file.exists():
                raw = json.loads(self.positions_cost_file.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    cleaned: Dict[str, Dict[str, float]] = {}
                    for token_id, item in raw.items():
                        if not isinstance(item, dict):
                            continue
                        avg_price = float(item.get("avg_price", 0.0))
                        shares = float(item.get("shares", 0.0))
                        highest_price_seen = float(item.get("highest_price_seen", avg_price))
                        if avg_price > 0 and shares > 0:
                            cleaned[str(token_id)] = {
                                "avg_price": avg_price,
                                "shares": shares,
                                "highest_price_seen": max(avg_price, highest_price_seen),
                            }
                    self.positions_cost = cleaned
        except Exception as exc:
            LOGGER.warning("Failed to load positions cost file: %s", exc)
            self.positions_cost = {}

    def _save_positions_cost(self) -> None:
        """持久化持仓成本到本地 JSON。"""
        try:
            self.report_dir.mkdir(parents=True, exist_ok=True)
            self.positions_cost_file.write_text(
                json.dumps(self.positions_cost, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            LOGGER.warning("Failed to save positions cost file: %s", exc)

    def _load_opposite_token_map(self) -> None:
        """加载 token 对侧映射（YES<->NO）。"""
        try:
            if not self.opposite_token_map_file.exists():
                self.opposite_token_map = {}
                return
            raw = json.loads(self.opposite_token_map_file.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                self.opposite_token_map = {}
                return
            cleaned: Dict[str, str] = {}
            for k, v in raw.items():
                tk = str(k or "").strip()
                tv = str(v or "").strip()
                if tk and tv and tk != tv:
                    cleaned[tk] = tv
            self.opposite_token_map = cleaned
        except Exception as exc:
            LOGGER.warning("Failed to load opposite token map: %s", exc)
            self.opposite_token_map = {}

    def _save_opposite_token_map(self) -> None:
        """持久化 token 对侧映射。"""
        try:
            self.report_dir.mkdir(parents=True, exist_ok=True)
            self.opposite_token_map_file.write_text(
                json.dumps(self.opposite_token_map, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            LOGGER.warning("Failed to save opposite token map: %s", exc)

    def _load_synthetic_hedge_state(self) -> None:
        """加载合成平仓状态（兼容旧版: token->shares；新版: token->{hedged_shares,total_spent_usdc}）。"""
        try:
            if not self.synthetic_hedge_state_file.exists():
                self.synthetic_hedge_state = {}
                return
            raw = json.loads(self.synthetic_hedge_state_file.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                self.synthetic_hedge_state = {}
                return
            cleaned: Dict[str, Dict[str, float]] = {}
            for k, v in raw.items():
                try:
                    token_id = str(k or "").strip()
                    if not token_id:
                        continue
                    # 兼容旧结构：直接是数值（仅 hedged_shares）
                    if isinstance(v, (int, float, str)):
                        hedged = float(v)
                        if hedged > 0:
                            cleaned[token_id] = {
                                "hedged_shares": hedged,
                                "total_spent_usdc": 0.0,
                            }
                        continue
                    # 新结构
                    if isinstance(v, dict):
                        hedged = float(v.get("hedged_shares", 0.0) or 0.0)
                        spent = float(v.get("total_spent_usdc", 0.0) or 0.0)
                        if hedged > 0 or spent > 0:
                            cleaned[token_id] = {
                                "hedged_shares": max(0.0, hedged),
                                "total_spent_usdc": max(0.0, spent),
                            }
                except Exception:
                    continue
            self.synthetic_hedge_state = cleaned
        except Exception as exc:
            LOGGER.warning("Failed to load synthetic hedge state: %s", exc)
            self.synthetic_hedge_state = {}

    def _save_synthetic_hedge_state(self) -> None:
        """持久化合成平仓对冲份额状态。"""
        try:
            self.report_dir.mkdir(parents=True, exist_ok=True)
            self.synthetic_hedge_state_file.write_text(
                json.dumps(self.synthetic_hedge_state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            LOGGER.warning("Failed to save synthetic hedge state: %s", exc)

    def _get_hedge_entry(self, token_id: str) -> Dict[str, float]:
        """读取单 token 合成平仓状态。"""
        tk = str(token_id or "").strip()
        if not tk:
            return {"hedged_shares": 0.0, "total_spent_usdc": 0.0}
        raw = self.synthetic_hedge_state.get(tk, {})
        if isinstance(raw, dict):
            return {
                "hedged_shares": max(0.0, float(raw.get("hedged_shares", 0.0) or 0.0)),
                "total_spent_usdc": max(0.0, float(raw.get("total_spent_usdc", 0.0) or 0.0)),
            }
        # 兼容异常旧值
        try:
            return {"hedged_shares": max(0.0, float(raw or 0.0)), "total_spent_usdc": 0.0}
        except Exception:
            return {"hedged_shares": 0.0, "total_spent_usdc": 0.0}

    def _set_hedge_entry(self, token_id: str, hedged_shares: float, total_spent_usdc: float) -> None:
        """写入单 token 合成平仓状态。"""
        tk = str(token_id or "").strip()
        if not tk:
            return
        hs = max(0.0, float(hedged_shares or 0.0))
        spent = max(0.0, float(total_spent_usdc or 0.0))
        if hs <= 1e-8 and spent <= 1e-8:
            self.synthetic_hedge_state.pop(tk, None)
            return
        self.synthetic_hedge_state[tk] = {"hedged_shares": hs, "total_spent_usdc": spent}

    def _effective_unhedged_shares(self, token_id: str, total_shares: float) -> float:
        """返回扣除已合成对冲后的未对冲份额。"""
        hedged = float(self._get_hedge_entry(token_id).get("hedged_shares", 0.0))
        return max(0.0, float(total_shares) - hedged)

    def _register_synthetic_hedge(self, token_id: str, hedged_shares: float, spent_usdc: float) -> None:
        """累加 token 的已对冲份额和累计花费。"""
        if hedged_shares <= 0 and spent_usdc <= 0:
            return
        tk = str(token_id or "")
        if not tk:
            return
        prev = self._get_hedge_entry(tk)
        new_hedged = float(prev.get("hedged_shares", 0.0)) + max(0.0, float(hedged_shares))
        new_spent = float(prev.get("total_spent_usdc", 0.0)) + max(0.0, float(spent_usdc))
        self._set_hedge_entry(tk, new_hedged, new_spent)
        self._save_synthetic_hedge_state()

    def _load_daily_realized_pnl(self) -> None:
        """从本地 JSON 读取按日期累计的已实现盈亏。"""
        try:
            if not self.daily_realized_pnl_file.exists():
                self.daily_realized_pnl = {}
                return
            raw = json.loads(self.daily_realized_pnl_file.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                self.daily_realized_pnl = {}
                return
            cleaned: Dict[str, float] = {}
            for k, v in raw.items():
                try:
                    cleaned[str(k)] = float(v)
                except Exception:
                    continue
            self.daily_realized_pnl = cleaned
        except Exception as exc:
            LOGGER.warning("Failed to load daily realized pnl file: %s", exc)
            self.daily_realized_pnl = {}

    def _load_fair_prob_state(self) -> None:
        """加载 token 公平概率状态缓存。"""
        try:
            if not self.fair_prob_state_file.exists():
                self.fair_prob_state = {}
                return
            raw = json.loads(self.fair_prob_state_file.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                self.fair_prob_state = {}
                return
            cleaned: Dict[str, float] = {}
            for k, v in raw.items():
                try:
                    fv = float(v)
                    if 0.0 <= fv <= 1.0:
                        cleaned[str(k)] = fv
                except Exception:
                    continue
            self.fair_prob_state = cleaned
        except Exception as exc:
            LOGGER.warning("Failed to load fair prob state file: %s", exc)
            self.fair_prob_state = {}

    def _save_fair_prob_state(self) -> None:
        """保存 token 公平概率状态缓存。"""
        try:
            self.report_dir.mkdir(parents=True, exist_ok=True)
            self.fair_prob_state_file.write_text(
                json.dumps(self.fair_prob_state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            LOGGER.warning("Failed to save fair prob state file: %s", exc)

    def _load_source_reliability(self) -> None:
        """加载多源可靠度缓存。"""
        try:
            if not self.source_reliability_file.exists():
                self.source_reliability = {}
                return
            raw = json.loads(self.source_reliability_file.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                self.source_reliability = {}
                return
            cleaned: Dict[str, float] = {}
            for k, v in raw.items():
                try:
                    rv = float(v)
                    cleaned[str(k)] = min(1.0, max(0.05, rv))
                except Exception:
                    continue
            self.source_reliability = cleaned
        except Exception as exc:
            LOGGER.warning("Failed to load source reliability file: %s", exc)
            self.source_reliability = {}

    def _save_source_reliability(self) -> None:
        """保存多源可靠度缓存。"""
        try:
            self.report_dir.mkdir(parents=True, exist_ok=True)
            self.source_reliability_file.write_text(
                json.dumps(self.source_reliability, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            LOGGER.warning("Failed to save source reliability file: %s", exc)

    def _save_daily_realized_pnl(self) -> None:
        """持久化按日期累计的已实现盈亏。"""
        try:
            self.report_dir.mkdir(parents=True, exist_ok=True)
            self.daily_realized_pnl_file.write_text(
                json.dumps(self.daily_realized_pnl, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            LOGGER.warning("Failed to save daily realized pnl file: %s", exc)

    def _today_nyc_key(self) -> str:
        """当前纽约日期键（YYYY-MM-DD）。"""
        return datetime.now(self.NYC_TZ).strftime("%Y-%m-%d")

    def _get_today_realized_pnl(self) -> float:
        """获取当日（纽约日期）累计已实现盈亏。"""
        # 每轮读取最新文件，确保手动清零/修正后立即生效
        self._load_daily_realized_pnl()
        return float(self.daily_realized_pnl.get(self._today_nyc_key(), 0.0))

    def _record_realized_pnl(self, pnl_usdc: float) -> None:
        """累加当日已实现盈亏。"""
        day = self._today_nyc_key()
        old = float(self.daily_realized_pnl.get(day, 0.0))
        self.daily_realized_pnl[day] = old + float(pnl_usdc)
        self._save_daily_realized_pnl()

    def _positions_query_user(self) -> str:
        """持仓查询用户标识：优先 funder（代理钱包），其次主地址。"""
        user = (self.funder or "").strip().lower()
        if user:
            return user
        addr = (self.client.get_address() or "").strip().lower()
        return addr

    def _fetch_live_positions(self) -> List[Dict[str, Any]]:
        """
        从 Data API 获取当前账户持仓。
        返回结构化列表，统一字段命名，便于风控与报告复用。
        """
        user = self._positions_query_user()
        if not user:
            return []
        try:
            payload = self._request_json(
                "GET",
                f"{self.DATA_API_BASE}/positions",
                params={"user": user},
                max_retries=3,
            )
            if not isinstance(payload, list):
                return []
            rows: List[Dict[str, Any]] = []
            for p in payload:
                if not isinstance(p, dict):
                    continue
                token_id = str(p.get("asset") or "").strip()
                if not token_id:
                    continue
                size = float(p.get("size", 0.0) or 0.0)
                if size <= 0:
                    continue
                rows.append(
                    {
                        "token_id": token_id,
                        "condition_id": str(p.get("conditionId") or ""),
                        "size": size,
                        "avg_price": float(p.get("avgPrice", 0.0) or 0.0),
                        "cur_price": float(p.get("curPrice", 0.0) or 0.0),
                        "title": str(p.get("title") or ""),
                        "slug": str(p.get("slug") or ""),
                        "outcome": str(p.get("outcome") or ""),
                        "end_date": str(p.get("endDate") or ""),
                    }
                )
            return rows
        except Exception as exc:
            LOGGER.warning("Fetch live positions failed: %s", exc)
            return []

    def _sync_positions_cost_from_live_positions(
        self,
        live_positions: List[Dict[str, Any]],
        bootstrap_only_missing: bool = False,
    ) -> None:
        """
        用真实持仓同步本地成本缓存：
        - bootstrap_only_missing=True: 仅补齐缺失 token
        - bootstrap_only_missing=False: 同步份额并清理已平仓 token
        """
        if not live_positions:
            return
        live_token_set = set()
        changed = False
        for p in live_positions:
            token_id = str(p.get("token_id") or "")
            if not token_id:
                continue
            live_token_set.add(token_id)
            live_shares = float(p.get("size", 0.0) or 0.0)
            if live_shares <= 0:
                continue
            # 对冲份额不能超过真实持仓份额
            if token_id in self.synthetic_hedge_state:
                entry = self._get_hedge_entry(token_id)
                old_hedged = float(entry.get("hedged_shares", 0.0))
                new_hedged = min(old_hedged, live_shares)
                if abs(new_hedged - old_hedged) > 1e-8:
                    self._set_hedge_entry(token_id, new_hedged, float(entry.get("total_spent_usdc", 0.0)))
                    changed = True
            market_avg = float(p.get("avg_price", 0.0) or 0.0)
            if market_avg <= 0:
                market_avg = float(p.get("cur_price", 0.0) or 0.0)
            if market_avg <= 0:
                continue
            local = self.positions_cost.get(token_id, {})
            local_avg = float(local.get("avg_price", 0.0) or 0.0)
            local_shares = float(local.get("shares", 0.0) or 0.0)
            if bootstrap_only_missing:
                if token_id not in self.positions_cost:
                    self.positions_cost[token_id] = {
                        "avg_price": market_avg,
                        "shares": live_shares,
                        "highest_price_seen": market_avg,
                    }
                    changed = True
                continue
            next_avg = local_avg if local_avg > 0 else market_avg
            if abs(local_shares - live_shares) > 1e-8 or abs(next_avg - local_avg) > 1e-8:
                next_high = max(
                    float(local.get("highest_price_seen", 0.0) or 0.0),
                    float(p.get("cur_price", 0.0) or 0.0),
                    next_avg,
                )
                self.positions_cost[token_id] = {
                    "avg_price": next_avg,
                    "shares": live_shares,
                    "highest_price_seen": next_high,
                }
                changed = True

        if not bootstrap_only_missing:
            for token_id in list(self.positions_cost.keys()):
                if token_id not in live_token_set:
                    self.positions_cost.pop(token_id, None)
                    changed = True
            for token_id in list(self.synthetic_hedge_state.keys()):
                if token_id not in live_token_set:
                    self.synthetic_hedge_state.pop(token_id, None)
                    changed = True
        if changed:
            self._save_positions_cost()
            self._save_synthetic_hedge_state()

    def _bootstrap_positions_cost_from_live_positions(self) -> None:
        """启动时执行一次成本回填，避免“有仓位但没有 entry_price”。"""
        live_positions = self._fetch_live_positions()
        if not live_positions:
            return
        before = len(self.positions_cost)
        self._sync_positions_cost_from_live_positions(live_positions, bootstrap_only_missing=True)
        after = len(self.positions_cost)
        LOGGER.info("Positions bootstrap finished: live=%d local_before=%d local_after=%d", len(live_positions), before, after)

    def _refresh_api_creds(self) -> None:
        """创建或派生 CLOB API 凭证，并设置到客户端。"""
        creds = self.client.create_or_derive_api_creds()
        self.client.set_api_creds(creds)
        LOGGER.info("CLOB API credentials refreshed.")

    def _resolve_city_configs(self, cities: Optional[List[str]]) -> List[Dict[str, Any]]:
        """
        解析城市配置：
        - cities=None: 使用默认常见城市
        - cities=['New York', 'London']: 只保留指定城市
        """
        if not cities:
            return [dict(c) for c in self.COMMON_CITIES]

        wanted = {c.strip().lower() for c in cities if c and c.strip()}
        selected: List[Dict[str, Any]] = []
        for cfg in self.COMMON_CITIES:
            names = {str(cfg["name"]).lower()}
            names.update({str(a).lower() for a in cfg.get("aliases", [])})
            if names & wanted:
                selected.append(dict(cfg))

        if not selected:
            raise ValueError(f"No configured city matched input: {cities}")
        return selected

    def _request_json(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        max_retries: int = 3,
    ) -> Any:
        """
        通用 HTTP JSON 请求，内置容错：
        - 429 限流：指数退避重试
        - 网络异常：指数退避重试
        """
        for attempt in range(max_retries):
            try:
                resp = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=headers,
                    timeout=self.request_timeout_s,
                )
                if resp.status_code == 429:
                    # 处理 API 频率限制：优先尊重 Retry-After，其次使用指数退避+抖动
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after and str(retry_after).isdigit():
                        sleep_s = min(12.0, float(retry_after))
                    else:
                        base = min(6.0, 0.8 * (2**attempt))
                        sleep_s = base + random.uniform(0.1, 0.6)
                    LOGGER.warning("Rate limited: %s. Sleep %.2fs", url, sleep_s)
                    time.sleep(sleep_s)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                # 4xx（除 429）通常是参数问题，重试无意义，直接抛出
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                if status_code is not None and 400 <= status_code < 500 and status_code != 429:
                    raise
                if attempt == max_retries - 1:
                    # 最后一次仍失败，抛出给上层处理
                    raise
                base = min(5.0, 0.7 * (2**attempt))
                sleep_s = base + random.uniform(0.1, 0.5)
                LOGGER.warning("Request failed (%s): %s. Retry in %.2fs", url, exc, sleep_s)
                time.sleep(sleep_s)

        raise RuntimeError(f"Unexpected request failure: {url}")

    @staticmethod
    def _date_label(dt: datetime) -> str:
        """把日期格式化成 Polymarket 标题习惯，如 'March 4'。"""
        return f"{dt.strftime('%B')} {dt.day}"

    @staticmethod
    def _date_aliases(dt: datetime) -> List[str]:
        """生成日期匹配别名，兼容 March 4 / Mar 4 / march-4 / mar-4。"""
        full = f"{dt.strftime('%B')} {dt.day}"
        short = f"{dt.strftime('%b')} {dt.day}"
        full_slug = f"{dt.strftime('%B').lower()}-{dt.day}"
        short_slug = f"{dt.strftime('%b').lower()}-{dt.day}"
        return [full.lower(), short.lower(), full_slug.lower(), short_slug.lower()]

    def _build_t0_t1_t2(self) -> List[Tuple[str, datetime, str]]:
        """
        基于纽约时间生成 T0/T1/T2 三天日期。
        返回结构：[(date_key, datetime_obj, date_label), ...]
        """
        now_et = datetime.now(self.NYC_TZ)
        dates: List[Tuple[str, datetime, str]] = []
        for offset in range(3):
            d = now_et + timedelta(days=offset)
            date_key = d.strftime("%Y-%m-%d")
            dates.append((date_key, d, self._date_label(d)))
        return dates

    @staticmethod
    def _parse_json_if_needed(v: Any) -> List[str]:
        """
        Gamma 返回字段有时是 list，有时是 JSON 字符串。
        该函数统一转换成字符串列表。
        """
        if isinstance(v, list):
            return [str(x) for x in v]
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return [str(x) for x in parsed]
            except json.JSONDecodeError:
                return []
        return []

    def _score_market_match(
        self,
        question: str,
        event_title: str,
        event_slug: str,
        date_aliases: List[str],
        city_cfg: Dict[str, Any],
    ) -> int:
        """
        对候选市场做文本打分，挑选最像目标的 NYC 当日最高温市场。
        分值越高越匹配。
        """
        q = self._normalize_text_for_match(question)
        t = self._normalize_text_for_match(event_title)
        s = self._normalize_text_for_match(event_slug)
        city_name = str(city_cfg.get("name", "")).strip()
        city_name_norm = self._normalize_text_for_match(city_name)
        date_tokens = [x for x in date_aliases if x]
        score = 0

        # slug 更稳定，给更高权重
        if "highest-temperature" in s:
            score += 4
        if city_name_norm and city_name_norm.replace(" ", "-") in s:
            score += 6
        if any(d.replace(" ", "-") in s for d in date_tokens):
            score += 6

        if "highest temperature" in q:
            score += 4
        if f"highest temperature in {city_name_norm}" in q:
            score += 6
        if "highest temperature" in t:
            score += 3

        city_tokens = [str(city_cfg.get("name", "")).lower()] + [
            str(a).lower() for a in city_cfg.get("aliases", [])
        ]
        if any(tok and tok in q for tok in city_tokens):
            score += 3
        if any(tok and tok in t for tok in city_tokens):
            score += 2

        if any(d in q for d in date_tokens):
            score += 4
        if any(d in t for d in date_tokens):
            score += 3

        return score

    @staticmethod
    def _normalize_text_for_match(text: str) -> str:
        """统一文本格式，降低问号、空格和符号差异导致的误判。"""
        s = (text or "").strip().lower()
        s = s.replace("？", "?")
        # 去除大部分标点，避免问号等符号导致精确匹配失败
        s = re.sub(r"[^\w\s\-]", " ", s)
        s = re.sub(r"\s+", " ", s)
        return s

    def _fetch_weather_events(
        self,
        city_cfg: Dict[str, Any],
        limit: int = 200,
        ttl_s: int = 90,
    ) -> List[Dict[str, Any]]:
        """
        拉取 Weather 分类事件（含 markets 嵌套数据）。
        这里不依赖 query 搜索，直接分页扫描天气分类，稳定性更高。
        """
        city_name = str(city_cfg.get("name", "")).strip().lower()
        now_ts = time.time()
        cache = self._weather_events_cache if isinstance(self._weather_events_cache, dict) else {}
        if city_name in cache:
            hit = cache.get(city_name, {})
            if isinstance(hit, dict) and (now_ts - float(hit.get("ts", 0))) < ttl_s:
                return hit.get("events", []) or []

        query = f"{city_name} temperature".strip()
        events_raw = self._request_json(
            "GET",
            f"{self.GAMMA_BASE}/events",
            params={"query": query, "limit": limit, "active": "true", "closed": "false"},
        )
        if not isinstance(events_raw, list):
            events_raw = []

        # 仅处理 active=true 且 closed=false 的事件
        events = [
            evt
            for evt in events_raw
            if bool(evt.get("active", False)) and not bool(evt.get("closed", True))
        ]

        # 部分时间段 query 相关性较差：回退到 weather 标签池再本地匹配
        has_temp_event = any(
            "highest temperature" in f"{str(x.get('title', '')).lower()} {str(x.get('slug', '')).lower()}"
            for x in events
        )
        if not has_temp_event:
            fallback_raw = self._request_json(
                "GET",
                f"{self.GAMMA_BASE}/events",
                params={"tag_slug": "weather", "limit": max(200, limit), "active": "true", "closed": "false"},
            )
            if isinstance(fallback_raw, list):
                events = [
                    evt
                    for evt in fallback_raw
                    if bool(evt.get("active", False)) and not bool(evt.get("closed", True))
                ]

        if not isinstance(self._weather_events_cache, dict):
            self._weather_events_cache = {}
        self._weather_events_cache[city_name] = {"ts": now_ts, "events": events}
        self._weather_events_cache_ts = now_ts
        return events

    @staticmethod
    def _extract_band_label_from_question(question: str, date_label: str) -> Optional[str]:
        """
        从二元天气题目里抽取温度区间文本：
        例：Will the highest temperature in Miami be between 74-75°F on March 3?
        -> between 74-75F
        """
        q = (question or "").strip()
        if not q:
            return None

        lower_q = q.lower()
        lower_date = date_label.lower()
        start_idx = lower_q.find(" be ")
        if start_idx < 0:
            return None

        end_idx = lower_q.find(f" on {lower_date}")
        if end_idx < 0:
            end_idx = lower_q.find(" on ")
        if end_idx < 0:
            end_idx = q.rfind("?")
            if end_idx < 0:
                end_idx = len(q)

        raw = q[start_idx + 4 : end_idx].strip()
        if not raw:
            return None

        cleaned = (
            raw.replace("°", "")
            .replace("｡", "")
            .replace("紮", "F")
            .replace("掳", "")
            .strip()
        )
        cleaned = re.sub(r"\s+", " ", cleaned)
        return cleaned or None

    def _discover_from_weather_events(
        self,
        date_key: str,
        date_label: str,
        city_cfg: Dict[str, Any],
    ) -> Optional[DailyMarket]:
        """
        从 Weather 事件中发现某城市某日市场。
        兼容 Polymarket 当前“一个城市日期下多个二元区间题”的结构。
        """
        city_name = str(city_cfg.get("name", "")).strip()
        date_dt = datetime.strptime(date_key, "%Y-%m-%d")
        date_aliases = self._date_aliases(date_dt)
        city_tokens = [city_name] + [str(a).strip() for a in city_cfg.get("aliases", [])]
        exact_titles = [
            self._normalize_text_for_match(f"Highest temperature in {token} on {date_label}?")
            for token in city_tokens
            if token
        ]
        exact_titles += [t[:-1] for t in exact_titles if t.endswith("?")]

        events = self._fetch_weather_events(city_cfg)
        best_event: Optional[Dict[str, Any]] = None
        best_score = -1

        for evt in events:
            title = str(evt.get("title") or "")
            slug = str(evt.get("slug") or "")
            norm_title = self._normalize_text_for_match(title)
            if norm_title in exact_titles:
                score = 20
            else:
                score = self._score_market_match(title, title, slug, date_aliases, city_cfg)
            if score > best_score:
                best_event = evt
                best_score = score

        if not best_event or best_score < 6:
            return None

        outcomes: List[OutcomeToken] = []
        markets = best_event.get("markets") or []
        for m in markets:
            # 只保留当前可交易子市场，避免拿到已关闭/无订单簿 token
            if not bool(m.get("active", True)):
                continue
            if bool(m.get("closed", False)) or bool(m.get("archived", False)):
                continue
            if m.get("acceptingOrders") is False:
                continue

            q = str(m.get("question") or "")
            q_norm = self._normalize_text_for_match(q)
            # 只选当前城市 + 当前日期 + highest temperature 的子市场
            if "highest temperature" not in q_norm:
                continue
            if not any(self._normalize_text_for_match(tok) in q_norm for tok in city_tokens if tok):
                continue
            if self._normalize_text_for_match(date_label) not in q_norm:
                continue

            token_ids = self._parse_json_if_needed(m.get("clobTokenIds"))
            outcome_labels = self._parse_json_if_needed(m.get("outcomes"))
            if len(token_ids) != len(outcome_labels) or not token_ids:
                continue

            yes_idx: Optional[int] = None
            no_idx: Optional[int] = None
            for idx, lab in enumerate(outcome_labels):
                norm = str(lab).strip().lower()
                if norm == "yes":
                    yes_idx = idx
                elif norm == "no":
                    no_idx = idx
            if yes_idx is None or no_idx is None:
                continue

            band_label = self._extract_band_label_from_question(q, date_label) or q
            yes_token_id = str(token_ids[yes_idx]).strip()
            no_token_id = str(token_ids[no_idx]).strip()
            if not yes_token_id or not no_token_id:
                continue
            outcomes.append(OutcomeToken(label=band_label, yes_token_id=yes_token_id, no_token_id=no_token_id))

        if not outcomes:
            return None

        # 按区间下界排序，方便策略和页面阅读
        outcomes.sort(
            key=lambda o: (
                -9999.0 if self._parse_outcome_temp_band(o.label)[0] is None else self._parse_outcome_temp_band(o.label)[0],
                9999.0 if self._parse_outcome_temp_band(o.label)[1] is None else self._parse_outcome_temp_band(o.label)[1],
            )
        )

        event_id = str(best_event.get("id") or "")
        question = str(best_event.get("title") or "")
        settle_time_iso = str(best_event.get("endDate") or "")
        return DailyMarket(
            date_key=date_key,
            date_label=date_label,
            question=question,
            condition_id=f"event:{event_id}",
            outcomes=outcomes,
            settle_time_iso=settle_time_iso,
        )

    def _fetch_market_candidates(self, query: str) -> List[Dict[str, Any]]:
        """通过 Gamma /markets 拉取候选市场列表。"""
        markets = self._request_json(
            "GET",
            f"{self.GAMMA_BASE}/markets",
            params={"query": query, "limit": 200, "closed": "false", "archived": "false"},
        )
        if not isinstance(markets, list):
            return []
        return markets

    def _scan_markets_for_date(
        self,
        date_label: str,
        city_cfg: Dict[str, Any],
        max_scan: int = 3000,
    ) -> List[Dict[str, Any]]:
        """
        当 query 搜索不可靠时，分页扫描 markets 并在本地做文本过滤。
        这样即使服务端忽略 query，也能发现目标天气市场。
        """
        result: List[Dict[str, Any]] = []
        target_date = date_label.lower()
        city_tokens = [str(city_cfg.get("name", "")).lower()] + [
            str(a).lower() for a in city_cfg.get("aliases", [])
        ]
        for offset in range(0, max_scan, 200):
            page = self._request_json(
                "GET",
                f"{self.GAMMA_BASE}/markets",
                params={"limit": 200, "offset": offset, "closed": "false", "archived": "false"},
            )
            if not isinstance(page, list) or not page:
                break

            for m in page:
                q = str(m.get("question") or "").lower()
                if "highest temperature" not in q:
                    continue
                if not any(tok and tok in q for tok in city_tokens):
                    continue
                if target_date not in q:
                    continue
                result.append(m)

            # 已经找到候选就提前退出，降低 API 压力
            if result:
                break
        return result

    def discover_daily_markets(self, city_cfg: Dict[str, Any]) -> Dict[str, DailyMarket]:
        """
        动态发现 T0/T1/T2 三天市场（探测先行）：
        1) 先查 /events（query=\"{city} temperature\"）
        2) 过滤 active=true && closed=false
        3) 按 title/slug + 日期别名评分锁定事件
        4) 提取可交易区间 token
        """
        result: Dict[str, DailyMarket] = {}
        self.last_discovery_debug = []
        city_name = str(city_cfg.get("name", "")).strip()
        events = self._fetch_weather_events(city_cfg)

        for date_key, _, date_label in self._build_t0_t1_t2():
            date_dt = datetime.strptime(date_key, "%Y-%m-%d")
            date_aliases = self._date_aliases(date_dt)

            best_event: Optional[Dict[str, Any]] = None
            best_score = -1
            candidates = 0
            for evt in events:
                title = str(evt.get("title") or "")
                slug = str(evt.get("slug") or "")
                text = f"{title} {slug}".lower()
                # 仅考虑明显的“最高气温”事件
                if "highest temperature" not in text and "highest-temperature" not in text:
                    continue
                score = self._score_market_match(title, title, slug, date_aliases, city_cfg)
                candidates += 1
                if score > best_score:
                    best_score = score
                    best_event = evt

            if not best_event or best_score < 10:
                LOGGER.warning("%s skip %s: event not found (best_score=%s).", city_name, date_label, best_score)
                self.last_discovery_debug.append(
                    {
                        "city": city_name,
                        "date": date_key,
                        "date_label": date_label,
                        "status": "SKIP",
                        "candidates": candidates,
                        "best_score": best_score,
                        "reason": "event_not_found_or_score_too_low",
                    }
                )
                continue

            markets = best_event.get("markets") or []
            outcomes: List[OutcomeToken] = []
            condition_ids: List[str] = []
            for m in markets:
                if not bool(m.get("active", False)) or bool(m.get("closed", True)):
                    continue
                q = str(m.get("question") or "")
                q_norm = self._normalize_text_for_match(q)
                if "highest temperature" not in q_norm:
                    continue
                if not any(alias in q_norm for alias in date_aliases):
                    continue
                token_ids = self._parse_json_if_needed(m.get("clobTokenIds"))
                labels = self._parse_json_if_needed(m.get("outcomes"))
                if len(token_ids) != len(labels) or not token_ids:
                    continue
                yes_idx: Optional[int] = None
                no_idx: Optional[int] = None
                for idx, lab in enumerate(labels):
                    norm = str(lab).strip().lower()
                    if norm == "yes":
                        yes_idx = idx
                    elif norm == "no":
                        no_idx = idx
                if yes_idx is None or no_idx is None:
                    continue
                yes_token_id = str(token_ids[yes_idx]).strip()
                no_token_id = str(token_ids[no_idx]).strip()
                if not yes_token_id or not no_token_id:
                    continue
                band_label = self._extract_band_label_from_question(q, date_label) or q
                outcomes.append(
                    OutcomeToken(
                        label=band_label,
                        yes_token_id=yes_token_id,
                        no_token_id=no_token_id,
                    )
                )
                cid = str(m.get("conditionId") or "")
                if cid:
                    condition_ids.append(cid)

            if not outcomes:
                self.last_discovery_debug.append(
                    {
                        "city": city_name,
                        "date": date_key,
                        "date_label": date_label,
                        "status": "SKIP",
                        "candidates": candidates,
                        "best_score": best_score,
                        "reason": "event_found_but_no_tradable_outcomes",
                    }
                )
                continue

            outcomes.sort(
                key=lambda o: (
                    -9999.0 if self._parse_outcome_temp_band(o.label)[0] is None else self._parse_outcome_temp_band(o.label)[0],
                    9999.0 if self._parse_outcome_temp_band(o.label)[1] is None else self._parse_outcome_temp_band(o.label)[1],
                )
            )
            question = str(best_event.get("title") or "")
            condition_id = f"event:{best_event.get('id')}"
            if condition_ids:
                condition_id = "|".join(sorted(set(condition_ids)))

            result[date_key] = DailyMarket(
                date_key=date_key,
                date_label=date_label,
                question=question,
                condition_id=condition_id,
                outcomes=outcomes,
                settle_time_iso=str(best_event.get("endDate") or ""),
            )

            LOGGER.info(
                "Discovered %s %s | condition=%s | outcomes=%d",
                city_name,
                date_label,
                condition_id,
                len(outcomes),
            )
            self.last_discovery_debug.append(
                {
                    "city": city_name,
                    "date": date_key,
                    "date_label": date_label,
                    "status": "FOUND",
                    "candidates": candidates,
                    "best_score": best_score,
                    "condition_id": condition_id,
                    "question": question,
                    "outcomes_count": len(outcomes),
                    "source": "events_probe",
                }
            )
        if not result:
            LOGGER.warning("No temperature markets discovered for %s in T0/T1/T2.", city_name)
        return result

    def _required_date_keys(self) -> List[str]:
        """当前轮所需日期键（T0/T1/T2）。"""
        return [k for k, _, _ in self._build_t0_t1_t2()]

    def _aggregate_daily_max(
        self,
        time_list: List[Any],
        temp_list: List[Any],
        timezone_name: str,
    ) -> Dict[str, float]:
        """将任意时间序列温度聚合成按日期最高温。"""
        if not time_list or not temp_list or len(time_list) != len(temp_list):
            return {}
        tz = pytz.timezone(timezone_name)
        out: Dict[str, float] = {}
        for ts, temp in zip(time_list, temp_list):
            try:
                if isinstance(ts, (int, float)):
                    dt = datetime.fromtimestamp(float(ts), tz=pytz.UTC).astimezone(tz)
                else:
                    tss = str(ts).replace("Z", "+00:00")
                    dt = datetime.fromisoformat(tss)
                    if dt.tzinfo is None:
                        dt = tz.localize(dt)
                    else:
                        dt = dt.astimezone(tz)
                date_key = dt.strftime("%Y-%m-%d")
                v = float(temp)
                if date_key not in out:
                    out[date_key] = v
                else:
                    out[date_key] = max(out[date_key], v)
            except Exception:
                continue
        return out

    def _fetch_open_meteo_daily_max_for_model(
        self,
        city_cfg: Dict[str, Any],
        temp_unit: str,
        model_name: Optional[str] = None,
    ) -> Dict[str, float]:
        """拉取单一 Open-Meteo 模型并转换成日最高温。"""
        params = {
            "latitude": city_cfg["lat"],
            "longitude": city_cfg["lon"],
            "hourly": "temperature_2m",
            "temperature_unit": temp_unit,
            "timezone": city_cfg.get("timezone", "America/New_York"),
            "forecast_days": 4,
        }
        if model_name:
            params["models"] = model_name
            payload = self._request_json("GET", self.OPEN_METEO_FORECAST, params=params, max_retries=2)
        else:
            payload = self._request_json("GET", self.OPEN_METEO_FORECAST, params=params, max_retries=2)
        if not isinstance(payload, dict):
            return {}
        hourly = payload.get("hourly", {}) if isinstance(payload.get("hourly"), dict) else {}
        hours = hourly.get("time") or []
        temps = hourly.get("temperature_2m") or []
        return self._aggregate_daily_max(hours, temps, str(city_cfg.get("timezone", "America/New_York")))

    def _fetch_open_meteo_gfs_daily_max(self, city_cfg: Dict[str, Any], temp_unit: str) -> Dict[str, float]:
        """Open-Meteo /v1/gfs 兜底日最高温。"""
        params = {
            "latitude": city_cfg["lat"],
            "longitude": city_cfg["lon"],
            "hourly": "temperature_2m",
            "temperature_unit": temp_unit,
            "timezone": city_cfg.get("timezone", "America/New_York"),
            "forecast_days": 4,
        }
        payload = self._request_json("GET", self.OPEN_METEO_GFS, params=params, max_retries=2)
        if not isinstance(payload, dict):
            return {}
        hourly = payload.get("hourly", {}) if isinstance(payload.get("hourly"), dict) else {}
        hours = hourly.get("time") or []
        temps = hourly.get("temperature_2m") or []
        return self._aggregate_daily_max(hours, temps, str(city_cfg.get("timezone", "America/New_York")))

    def _fetch_nws_daily_max_forecast(self, city_cfg: Dict[str, Any], temp_unit: str) -> Dict[str, float]:
        """
        NWS 预报（美国官方源）：
        points -> forecastHourly -> periods.temperature
        """
        lat = city_cfg.get("lat")
        lon = city_cfg.get("lon")
        if lat is None or lon is None:
            return {}
        points_url = f"{self.NWS_POINTS_API}/{lat},{lon}"
        points = self._request_json("GET", points_url, headers=self.weather_http_headers, max_retries=2)
        if not isinstance(points, dict):
            return {}
        props = points.get("properties", {}) if isinstance(points.get("properties"), dict) else {}
        hourly_url = str(props.get("forecastHourly") or "").strip()
        if not hourly_url:
            return {}
        hourly_payload = self._request_json("GET", hourly_url, headers=self.weather_http_headers, max_retries=2)
        if not isinstance(hourly_payload, dict):
            return {}
        periods = (
            hourly_payload.get("properties", {}).get("periods", [])
            if isinstance(hourly_payload.get("properties"), dict)
            else []
        )
        times: List[str] = []
        temps: List[float] = []
        for p in periods:
            if not isinstance(p, dict):
                continue
            st = p.get("startTime")
            tv = p.get("temperature")
            tu = str(p.get("temperatureUnit") or "").upper()
            if st is None or tv is None:
                continue
            try:
                val = float(tv)
            except Exception:
                continue
            src_unit = "fahrenheit" if tu == "F" else "celsius"
            dst_val = self._convert_temperature(val, src_unit, temp_unit)
            times.append(str(st))
            temps.append(float(dst_val))
        return self._aggregate_daily_max(times, temps, str(city_cfg.get("timezone", "America/New_York")))

    def _fetch_aviation_metar_latest_temp(self, city_cfg: Dict[str, Any], temp_unit: str) -> Dict[str, float]:
        """
        AviationWeather METAR 最新观测温度（用于 T0 修正与源补充）。
        返回 {YYYY-MM-DD: temp}
        """
        city_name = str(city_cfg.get("name", "")).strip().lower()
        station = str(city_cfg.get("station") or self.CITY_ICAO_MAP.get(city_name, "")).strip().upper()
        if not station:
            return {}
        payload = self._request_json(
            "GET",
            f"{self.AVIATION_WEATHER_API}/metar",
            params={"ids": station, "format": "json", "hours": 6},
            max_retries=2,
        )
        if not isinstance(payload, list) or not payload:
            return {}
        latest = payload[0] if isinstance(payload[0], dict) else {}
        temp_raw = latest.get("temp")
        if temp_raw is None:
            return {}
        try:
            temp_c = float(temp_raw)
        except Exception:
            return {}
        local_tz = pytz.timezone(str(city_cfg.get("timezone", "America/New_York")))
        ts = latest.get("reportTime") or latest.get("receiptTime") or latest.get("obsTime")
        if ts is None:
            dt = datetime.now(local_tz)
        else:
            try:
                if isinstance(ts, (int, float)):
                    dt = datetime.fromtimestamp(float(ts), tz=pytz.UTC).astimezone(local_tz)
                else:
                    dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = local_tz.localize(dt)
                    else:
                        dt = dt.astimezone(local_tz)
            except Exception:
                dt = datetime.now(local_tz)
        date_key = dt.strftime("%Y-%m-%d")
        temp_v = self._convert_temperature(temp_c, "celsius", temp_unit)
        return {date_key: float(temp_v)}

    def _source_temporal_fit(self, src_name: str, date_key: str) -> float:
        """
        不同源在不同预测日的时效匹配度：
        - metar 更适合 T0（实况）
        - nws 更适合近 1-2 天
        - open-meteo 模型适合全区间
        """
        src = str(src_name or "").lower()
        try:
            d = datetime.strptime(str(date_key), "%Y-%m-%d").date()
            now_d = datetime.now(self.NYC_TZ).date()
            horizon = (d - now_d).days
        except Exception:
            horizon = 0
        if "metar" in src:
            if horizon <= 0:
                return 1.0
            if horizon == 1:
                return 0.15
            return 0.05
        if src == "nws":
            if horizon <= 0:
                return 0.95
            if horizon == 1:
                return 0.85
            return 0.60
        return 0.85 if horizon <= 1 else 1.0

    def _blend_sources_for_date(
        self,
        date_key: str,
        source_daily: Dict[str, Dict[str, float]],
        temp_unit: str,
    ) -> Tuple[Optional[float], Dict[str, float], float, float]:
        """
        对单日做鲁棒融合，返回：
        (blended_temp, normalized_weights, disagreement_index, confidence_score)
        """
        rows: List[Tuple[str, float]] = []
        for src_name, daily in source_daily.items():
            if date_key in daily:
                try:
                    rows.append((src_name, float(daily[date_key])))
                except Exception:
                    continue
        if not rows:
            return None, {}, 0.0, 0.0

        values = [v for _, v in rows]
        median_v = statistics.median(values)
        abs_dev = [abs(v - median_v) for v in values]
        mad = statistics.median(abs_dev) if abs_dev else 0.0
        robust_scale = max(0.35, 1.4826 * mad)

        raw_weights: Dict[str, float] = {}
        weighted_sum = 0.0
        weight_sum = 0.0
        for src_name, val in rows:
            base_w = float(self.weather_source_weights.get(src_name, 0.1))
            rel_w = float(self.source_reliability.get(src_name, 0.75))
            temp_fit = self._source_temporal_fit(src_name, date_key)
            z = abs(val - median_v) / robust_scale
            robust_penalty = 1.0 / (1.0 + z * z)
            w = max(0.001, base_w * rel_w * temp_fit * robust_penalty)
            raw_weights[src_name] = w
            weighted_sum += w * val
            weight_sum += w

        if weight_sum <= 0:
            return None, {}, 0.0, 0.0
        blended = weighted_sum / weight_sum
        norm_weights = {k: (v / weight_sum) for k, v in raw_weights.items()}
        disagreement = float(statistics.pstdev(values)) if len(values) >= 2 else 0.0

        # 置信度：源数量 + 分歧惩罚
        source_count_factor = min(1.0, len(rows) / 4.0)
        disagree_scale = 2.5 if temp_unit == "fahrenheit" else 1.4
        disagreement_penalty = math.exp(-max(0.0, disagreement) / disagree_scale)
        confidence = max(0.05, min(1.0, source_count_factor * disagreement_penalty))

        # 用“与融合结果一致性”轻量更新可靠度（替代固定权重）
        for src_name, val in rows:
            old = float(self.source_reliability.get(src_name, 0.75))
            err = abs(val - blended)
            err_scale = 3.0 if temp_unit == "fahrenheit" else 1.7
            score = math.exp(-err / err_scale)
            self.source_reliability[src_name] = min(1.0, max(0.05, old * 0.98 + 0.02 * score))

        return blended, norm_weights, disagreement, confidence

    def fetch_city_daily_max_forecast(self, city_cfg: Dict[str, Any]) -> Dict[str, float]:
        """
        多源预测融合（T0/T1/T2）：
        1) Open-Meteo 多模型：hrrr/gfs_seamless/best_match/fallback/gfs
        2) NWS（可用时）
        3) AviationWeather METAR（T0 实况辅助）
        返回：{YYYY-MM-DD: 日最高温}
        """
        temp_unit = str(city_cfg.get("temp_unit", "fahrenheit")).lower()
        if temp_unit not in ("fahrenheit", "celsius"):
            temp_unit = "fahrenheit"
        required_dates = self._required_date_keys()

        source_daily: Dict[str, Dict[str, float]] = {}
        self.current_model_source = "unknown"
        self.current_model_details = {"city": city_cfg.get("name", ""), "unit": temp_unit, "sources": {}}

        # Open-Meteo 多模型并行尝试
        open_meteo_models = ["hrrr", "gfs_seamless", "best_match"]
        for model_name in open_meteo_models:
            try:
                daily = self._fetch_open_meteo_daily_max_for_model(city_cfg, temp_unit, model_name=model_name)
                if daily:
                    source_daily[f"openmeteo_{model_name}"] = daily
            except requests.RequestException as exc:
                code = getattr(getattr(exc, "response", None), "status_code", None)
                if code == 400:
                    LOGGER.warning("Open-Meteo model unsupported: %s for %s", model_name, city_cfg["name"])
                else:
                    LOGGER.warning("Open-Meteo model failed: %s | %s", model_name, exc)
            except Exception as exc:
                LOGGER.warning("Open-Meteo model error: %s | %s", model_name, exc)

        # 不带 models 的 fallback
        try:
            fallback_daily = self._fetch_open_meteo_daily_max_for_model(city_cfg, temp_unit, model_name=None)
            if fallback_daily:
                source_daily["openmeteo_fallback"] = fallback_daily
        except Exception as exc:
            LOGGER.warning("Open-Meteo fallback(no-models) failed for %s: %s", city_cfg["name"], exc)

        # /v1/gfs 最后兜底
        try:
            gfs_daily = self._fetch_open_meteo_gfs_daily_max(city_cfg, temp_unit)
            if gfs_daily:
                source_daily["openmeteo_gfs"] = gfs_daily
        except Exception as exc:
            LOGGER.warning("Open-Meteo gfs endpoint failed for %s: %s", city_cfg["name"], exc)

        # NWS（美国区域可用）
        try:
            nws_daily = self._fetch_nws_daily_max_forecast(city_cfg, temp_unit)
            if nws_daily:
                source_daily["nws"] = nws_daily
        except Exception as exc:
            LOGGER.info("NWS source unavailable for %s: %s", city_cfg["name"], exc)

        # METAR（作为实况辅助，仅影响对应日期）
        try:
            metar_daily = self._fetch_aviation_metar_latest_temp(city_cfg, temp_unit)
            if metar_daily:
                source_daily["metar"] = metar_daily
        except Exception as exc:
            LOGGER.info("METAR source unavailable for %s: %s", city_cfg["name"], exc)

        if not source_daily:
            raise RuntimeError(f"All forecast sources failed for {city_cfg.get('name')}")

        # 对可用源做加权融合（可靠度 + 时效匹配 + 鲁棒降权）
        blended: Dict[str, float] = {}
        confidence_by_date: Dict[str, float] = {}
        disagreement_by_date: Dict[str, float] = {}
        source_weights_by_date: Dict[str, Dict[str, float]] = {}
        for date_key in required_dates:
            v, w_map, disagree, conf = self._blend_sources_for_date(date_key, source_daily, temp_unit)
            if v is not None:
                blended[date_key] = float(v)
                source_weights_by_date[date_key] = {k: round(float(vv), 4) for k, vv in w_map.items()}
                disagreement_by_date[date_key] = round(float(disagree), 4)
                confidence_by_date[date_key] = round(float(conf), 4)

        missing = [k for k in required_dates if k not in blended]
        if missing:
            # 时区跨日会导致源返回日期与纽约 T0/T1/T2 偏移 1 天
            # 这里做顺序对齐兜底：先基于“全部可用日期”做加权，再映射到 required_dates
            all_dates = sorted({d for daily in source_daily.values() for d in daily.keys()})
            aligned_pool: List[float] = []
            for date_key in all_dates:
                weighted_sum = 0.0
                weight_sum = 0.0
                for src_name, daily in source_daily.items():
                    if date_key not in daily:
                        continue
                    v = float(daily[date_key])
                    w = float(self.weather_source_weights.get(src_name, 0.1))
                    weighted_sum += v * w
                    weight_sum += w
                if weight_sum > 0:
                    aligned_pool.append(weighted_sum / weight_sum)
            if len(aligned_pool) >= len(required_dates):
                blended = {dk: aligned_pool[idx] for idx, dk in enumerate(required_dates)}
                for dk in required_dates:
                    confidence_by_date.setdefault(dk, 0.35)
                    disagreement_by_date.setdefault(dk, 0.0)
                    source_weights_by_date.setdefault(dk, {})
                missing = []
            else:
                raise RuntimeError(f"Forecast missing dates after blend: {missing}")

        self.current_model_details["sources"] = source_daily
        self.current_model_details["confidence_by_date"] = confidence_by_date
        self.current_model_details["disagreement_by_date"] = disagreement_by_date
        self.current_model_details["source_weights_by_date"] = source_weights_by_date
        src_keys = sorted(source_daily.keys())
        self.current_model_source = f"blend:{'+'.join(src_keys)}"
        self._save_source_reliability()
        LOGGER.info(
            "Forecast blend %s | sources=%s | daily_max=%s | conf=%s | disagree=%s",
            city_cfg.get("name", ""),
            src_keys,
            {k: round(v, 2) for k, v in blended.items()},
            confidence_by_date,
            disagreement_by_date,
        )
        return {k: blended[k] for k in required_dates}

    @staticmethod
    def _extract_numbers(s: str) -> List[float]:
        """从字符串中抽取数字（支持整数/小数/负数）。"""
        nums = re.findall(r"-?\d+(?:\.\d+)?", s)
        return [float(n) for n in nums]

    @staticmethod
    def _detect_temp_unit(label: str) -> Optional[str]:
        """从标签中识别温标。"""
        s = (label or "").lower()
        if "celsius" in s or "°c" in s or re.search(r"\bc\b", s):
            return "celsius"
        if "fahrenheit" in s or "°f" in s or re.search(r"\bf\b", s):
            return "fahrenheit"
        return None

    @staticmethod
    def _normalize_temp_unit(unit: str) -> Optional[str]:
        """把不同写法归一到 celsius/fahrenheit。"""
        s = str(unit or "").strip().lower()
        if s in ("c", "celsius", "centigrade", "degc", "°c"):
            return "celsius"
        if s in ("f", "fahrenheit", "degf", "°f"):
            return "fahrenheit"
        return None

    @staticmethod
    def _convert_temperature(value: float, from_unit: str, to_unit: str) -> float:
        """温标转换，仅支持 C/F。"""
        f = (from_unit or "").lower()
        t = (to_unit or "").lower()
        if f == t or not f or not t:
            return value
        if f == "fahrenheit" and t == "celsius":
            return (value - 32.0) / 1.8
        if f == "celsius" and t == "fahrenheit":
            return value * 1.8 + 32.0
        return value

    def _parse_outcome_temp_band(self, label: str) -> Tuple[Optional[float], Optional[float]]:
        """
        解析 outcome 文本温度区间。
        返回 (lo, hi)：
        - x or higher -> (x, None)
        - x or lower  -> (None, x)
        - x to y      -> (x, y)
        """
        # 预处理：兼容 °/掳/紮/℃/℉ 等符号，稳定提取数字
        s = (
            (label or "")
            .lower()
            .replace("°", "")
            .replace("掳", "")
            .replace("紮", "f")
            .replace("℃", "c")
            .replace("℉", "f")
        )
        nums = self._extract_numbers(s)

        if not nums:
            return None, None
        if any(k in s for k in ["or higher", "and above", "+", "above"]):
            return nums[0], None
        if any(k in s for k in ["or lower", "or less", "below", "under", "and below"]):
            return None, nums[0]
        if len(nums) >= 2:
            lo, hi = min(nums[0], nums[1]), max(nums[0], nums[1])
            return lo, hi
        return nums[0], nums[0]

    def _should_use_discrete_resolution(
        self,
        outcome_label: str,
        lo: Optional[float],
        hi: Optional[float],
        fallback_unit: Optional[str] = None,
    ) -> bool:
        """
        是否启用“整度离散结算”概率模型。
        对类似天气最高温区间（如 44-45F / 13C）启用离散处理。
        """
        unit = self._detect_temp_unit(outcome_label) or self._normalize_temp_unit(fallback_unit or "")
        if unit not in ("fahrenheit", "celsius"):
            return False
        for v in (lo, hi):
            if v is None:
                continue
            # 非整度区间（例如 1.05-1.09C）不做整度离散
            if abs(float(v) - round(float(v))) > 0.05:
                return False
        return True

    @staticmethod
    def _temp_band_contains_int_degree(
        deg: int,
        lo: Optional[float],
        hi: Optional[float],
    ) -> bool:
        """判断整数温度是否落在 outcome 区间内（闭区间语义）。"""
        x = float(deg)
        if lo is not None and x < float(lo):
            return False
        if hi is not None and x > float(hi):
            return False
        return True

    def _discrete_degree_band_probability(
        self,
        mu: float,
        sigma: float,
        lo: Optional[float],
        hi: Optional[float],
    ) -> float:
        """
        按“整度”计算区间概率：
        P(T in band) = Σ_k P(T=k), 其中
        P(T=k)=CDF(k+0.5)-CDF(k-0.5)
        """
        sigma = max(0.2, float(sigma))
        center = float(mu)
        spread = max(18.0, 8.0 * sigma)
        k_min = int(math.floor(center - spread))
        k_max = int(math.ceil(center + spread))

        # one-sided 区间把边界纳入扫描范围，避免截断丢失概率
        if lo is not None:
            k_min = min(k_min, int(math.floor(float(lo) - 2)))
            k_max = max(k_max, int(math.ceil(float(lo) + spread)))
        if hi is not None:
            k_min = min(k_min, int(math.floor(float(hi) - spread)))
            k_max = max(k_max, int(math.ceil(float(hi) + 2)))

        # 合理物理范围裁剪，避免异常参数导致过大循环
        k_min = max(-150, k_min)
        k_max = min(180, k_max)
        if k_min > k_max:
            return 0.0

        p = 0.0
        for k in range(k_min, k_max + 1):
            if not self._temp_band_contains_int_degree(k, lo, hi):
                continue
            mass = self._norm_cdf(k + 0.5, center, sigma) - self._norm_cdf(k - 0.5, center, sigma)
            if mass > 0:
                p += mass
        return max(0.0, min(1.0, p))

    @staticmethod
    def _norm_cdf(x: float, mu: float, sigma: float) -> float:
        """正态分布 CDF，用于把温度预测转换为区间概率。"""
        z = (x - mu) / (sigma * math.sqrt(2))
        return 0.5 * (1 + math.erf(z))

    @staticmethod
    def _effective_probability_bounds(
        lo: Optional[float],
        hi: Optional[float],
    ) -> Tuple[Optional[float], Optional[float]]:
        """
        将“整度结算区间”映射到物理温度积分边界：
        - [L, H] -> [L-0.5, H+0.5)
        - (None, H] -> (-inf, H+0.5)
        - [L, None) -> [L-0.5, +inf)
        """
        eff_lo = None if lo is None else float(lo) - 0.5
        eff_hi = None if hi is None else float(hi) + 0.5
        return eff_lo, eff_hi

    @staticmethod
    def _nearest_half_step_distance(x: float) -> float:
        """返回与最近 n+0.5 临界点的距离。"""
        k = math.floor(float(x))
        half = float(k) + 0.5
        # 最近临界点只可能是当前 half 或相邻整数上的 half
        d0 = abs(float(x) - half)
        d1 = abs(float(x) - (half - 1.0))
        d2 = abs(float(x) - (half + 1.0))
        return min(d0, d1, d2)

    def _dynamic_sigma(self, base_sigma: float, settle_time_iso: str = "") -> float:
        """
        动态 sigma：
        距离结算每近 12 小时，sigma 下调 15%。
        """
        sigma = max(0.2, float(base_sigma))
        if not settle_time_iso:
            return sigma
        try:
            iso = str(settle_time_iso).replace("Z", "+00:00")
            settle_dt = datetime.fromisoformat(iso)
            if settle_dt.tzinfo is None:
                settle_dt = pytz.UTC.localize(settle_dt)
            now_utc = datetime.now(pytz.UTC)
            hours_left = (settle_dt.astimezone(pytz.UTC) - now_utc).total_seconds() / 3600.0
            # 以 72 小时为参考窗口，越接近结算 sigma 越小
            steps = max(0, int((72.0 - max(0.0, hours_left)) // 12.0))
            sigma = sigma * (0.85**steps)
        except Exception:
            return max(0.2, float(base_sigma))
        return max(0.2, sigma)

    @staticmethod
    def _hours_to_settle(settle_time_iso: str) -> Optional[float]:
        """计算距离结算剩余小时数，解析失败返回 None。"""
        if not settle_time_iso:
            return None
        try:
            iso = str(settle_time_iso).replace("Z", "+00:00")
            settle_dt = datetime.fromisoformat(iso)
            if settle_dt.tzinfo is None:
                settle_dt = pytz.UTC.localize(settle_dt)
            now_utc = datetime.now(pytz.UTC)
            return (settle_dt.astimezone(pytz.UTC) - now_utc).total_seconds() / 3600.0
        except Exception:
            return None

    def model_probability(
        self,
        forecast_max: float,
        outcome_label: str,
        forecast_unit: str = "fahrenheit",
        settle_time_iso: str = "",
        base_sigma_f: Optional[float] = None,
        forecast_dispersion: Optional[float] = None,
        return_effective_range: bool = False,
    ) -> Union[float, Tuple[float, Optional[Dict[str, Any]]]]:
        """
        给定预测最高温，计算落入 outcome 区间的“模型概率”。
        这里用正态近似：N(mu=forecast_max, sigma=temp_sigma_f)
        """
        lo, hi = self._parse_outcome_temp_band(outcome_label)
        forecast_unit_norm = self._normalize_temp_unit(forecast_unit) or "fahrenheit"
        outcome_unit = self._detect_temp_unit(outcome_label)
        effective_unit = outcome_unit or forecast_unit_norm
        # 若 outcome 明确了温标，自动转换预测值到相同单位
        if outcome_unit:
            forecast_max = self._convert_temperature(forecast_max, forecast_unit, outcome_unit)

        # sigma 基于 outcome 单位做缩放（F -> C）
        sigma_source = float(base_sigma_f) if base_sigma_f and float(base_sigma_f) > 0 else self.temp_sigma_f
        sigma_base = sigma_source / 1.8 if effective_unit == "celsius" else sigma_source
        sigma_base = self._dynamic_sigma(sigma_base, settle_time_iso)
        # 当 HRRR 不可用时，对模型不确定性做惩罚，降低过度自信
        src_text = str(self.current_model_source or "").lower()
        has_hrrr = ("hrrr" in src_text)
        if not has_hrrr:
            sigma_base = sigma_base * 1.8
        # 预测离散度动态 sigma：多源分歧越大，sigma 越大（不确定性放大）
        if isinstance(forecast_dispersion, (int, float)) and float(forecast_dispersion) > 0:
            disp = float(forecast_dispersion)
            if effective_unit == "celsius":
                disp = self._convert_temperature(disp, forecast_unit, "celsius")
            sigma_base = sigma_base + (0.7 * max(0.0, disp))
        # 给 sigma 设置下限，防止过小导致数值过于极端
        sigma = max(0.5, sigma_base)

        if lo is None and hi is None:
            return (0.0, None) if return_effective_range else 0.0

        eff_lo, eff_hi = self._effective_probability_bounds(lo, hi)
        effective_range: Optional[Dict[str, Any]] = {
            "raw_lo": lo,
            "raw_hi": hi,
            "effective_lo": eff_lo,
            "effective_hi": eff_hi,
            "unit": effective_unit,
            "display": (
                f"Range: {lo if lo is not None else '-inf'}-{hi if hi is not None else '+inf'} | "
                f"Effective: {eff_lo if eff_lo is not None else '-inf'}-{eff_hi if eff_hi is not None else '+inf'}"
            ),
        }

        # 与 Polymarket 最高温市场常见结算规则对齐：按整度温度离散求和
        if self._should_use_discrete_resolution(outcome_label, lo, hi, fallback_unit=effective_unit):
            p = self._discrete_degree_band_probability(forecast_max, sigma, lo, hi)
            return (p, effective_range) if return_effective_range else p

        # 连续近似（用于非整度区间）
        if eff_lo is None:
            p = self._norm_cdf(eff_hi, forecast_max, sigma)
            return (p, effective_range) if return_effective_range else p
        if eff_hi is None:
            p = 1.0 - self._norm_cdf(eff_lo, forecast_max, sigma)
            return (p, effective_range) if return_effective_range else p
        p = max(0.0, self._norm_cdf(eff_hi, forecast_max, sigma) - self._norm_cdf(eff_lo, forecast_max, sigma))
        return (p, effective_range) if return_effective_range else p

    def _is_stable_interval(
        self,
        forecast_max: float,
        outcome_label: str,
        probability: float,
        forecast_unit: str = "fahrenheit",
    ) -> bool:
        """
        “稳定落入区间”判定：
        1) 概率达到阈值
        2) 离上下边界有 buffer（避免边界抖动频繁反转）
        """
        if probability < self.stability_prob_threshold:
            return False
        outcome_unit = self._detect_temp_unit(outcome_label)
        boundary_buffer = float(self.boundary_buffer_f)
        if outcome_unit:
            forecast_max = self._convert_temperature(forecast_max, forecast_unit, outcome_unit)
            # boundary_buffer_f 按华氏度配置；若 outcome 为摄氏度，先换算后再比较
            if outcome_unit == "celsius":
                boundary_buffer = boundary_buffer / 1.8
        lo, hi = self._parse_outcome_temp_band(outcome_label)
        # 结算临界点保护：若预测值靠近任意 .5 刻度（例如 47.5/48.5），判定为不稳定
        # 由于 .5 间距固定为 1.0，距离最近临界点最大仅 0.5，这里将缓冲上限裁到 0.49
        # 避免 boundary_buffer_f 配置过大导致“永远不稳定”。
        half_step_buffer = min(0.49, max(0.0, boundary_buffer))
        if self._nearest_half_step_distance(float(forecast_max)) < half_step_buffer:
            return False
        if lo is not None and forecast_max < lo + boundary_buffer:
            return False
        if hi is not None and forecast_max > hi - boundary_buffer:
            return False
        return True

    def _safe_get_price(self, token_id: str) -> float:
        """
        安全获取 BUY 价格。
        若遇到 401/403，自动刷新 API creds 后重试一次。
        """
        return self._safe_get_price_by_side(token_id, side="BUY")

    def _safe_get_price_by_side(self, token_id: str, side: str = "BUY") -> float:
        """按方向安全获取价格（BUY=买一侧成交参考，SELL=卖出参考）。"""
        side = str(side or "BUY").upper()
        for attempt in range(2):
            try:
                payload = self.client.get_price(token_id, side=side)
                return float(payload["price"])
            except PolyApiException as exc:
                if attempt == 0 and exc.status_code in (401, 403):
                    LOGGER.warning("CLOB auth expired when fetching price. Refresh creds.")
                    self._refresh_api_creds()
                    continue
                raise
        raise RuntimeError(f"Unable to fetch price for token {token_id} side={side}")

    @staticmethod
    def _extract_fill_price_size(order_result: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        """
        从下单回包中尽力提取成交均价与成交份额。
        兼容不同字段命名，提取失败则返回 (None, None)。
        """
        if not isinstance(order_result, dict):
            return None, None

        def _pick_float(keys: List[str], root: Dict[str, Any]) -> Optional[float]:
            for k in keys:
                if k in root:
                    try:
                        v = float(root[k])
                        if v > 0:
                            return v
                    except Exception:
                        continue
            return None

        price_keys = ["avg_price", "average_price", "price", "matched_price", "fill_price"]
        size_keys = ["size", "filled_size", "matched_size", "executed_size", "shares"]
        price = _pick_float(price_keys, order_result)
        size = _pick_float(size_keys, order_result)

        if price is None or size is None:
            order_obj = order_result.get("order")
            if isinstance(order_obj, dict):
                if price is None:
                    price = _pick_float(price_keys, order_obj)
                if size is None:
                    size = _pick_float(size_keys, order_obj)

        return price, size

    def _update_cost_on_buy(self, token_id: str, fill_price: float, fill_size: float) -> None:
        """买入后更新 token 的加权平均成本。"""
        if fill_price <= 0 or fill_size <= 0:
            return
        token_id = str(token_id)
        old = self.positions_cost.get(token_id, {"avg_price": 0.0, "shares": 0.0})
        old_avg = float(old.get("avg_price", 0.0))
        old_shares = float(old.get("shares", 0.0))
        old_high = float(old.get("highest_price_seen", old_avg))
        new_shares = old_shares + fill_size
        if new_shares <= 0:
            return
        new_avg = (old_avg * old_shares + fill_price * fill_size) / new_shares
        self.positions_cost[token_id] = {
            "avg_price": float(new_avg),
            "shares": float(new_shares),
            "highest_price_seen": float(max(old_high, fill_price)),
        }
        self._save_positions_cost()

    def _update_cost_on_sell(self, token_id: str, sell_size: float) -> None:
        """卖出后同步减少成本记录里的仓位份额（均价保持不变）。"""
        if sell_size <= 0:
            return
        token_id = str(token_id)
        item = self.positions_cost.get(token_id)
        if not item:
            return
        old_shares = float(item.get("shares", 0.0))
        remain = max(0.0, old_shares - sell_size)
        if remain <= 1e-8:
            self.positions_cost.pop(token_id, None)
        else:
            old_high = float(item.get("highest_price_seen", item.get("avg_price", 0.0)))
            self.positions_cost[token_id] = {
                "avg_price": float(item.get("avg_price", 0.0)),
                "shares": remain,
                "highest_price_seen": old_high,
            }
        if token_id in self.synthetic_hedge_state:
            entry = self._get_hedge_entry(token_id)
            prev_hedged = float(entry.get("hedged_shares", 0.0))
            next_hedged = max(0.0, prev_hedged - float(sell_size))
            if next_hedged <= 1e-8:
                self.synthetic_hedge_state.pop(token_id, None)
            else:
                self._set_hedge_entry(token_id, next_hedged, float(entry.get("total_spent_usdc", 0.0)))
        self._save_positions_cost()
        self._save_synthetic_hedge_state()

    @staticmethod
    def _extract_balance_value(payload: Dict[str, Any]) -> float:
        """
        从 get_balance_allowance 返回中提取可用余额（份额）。
        不同接口版本字段名可能不同，这里做兼容处理。
        """
        keys = [
            "balance",
            "available",
            "availableBalance",
            "asset_balance",
            "amount",
        ]
        for key in keys:
            if key in payload:
                try:
                    return float(payload[key])
                except (TypeError, ValueError):
                    continue
        return 0.0

    @staticmethod
    def _extract_allowance_value(payload: Dict[str, Any]) -> float:
        """
        从余额返回里提取 allowance（取最大 spender 授权值）。
        """
        allows = payload.get("allowances")
        if isinstance(allows, dict) and allows:
            vals: List[float] = []
            for v in allows.values():
                try:
                    vals.append(float(v))
                except Exception:
                    continue
            if vals:
                return max(vals)
        if "allowance" in payload:
            try:
                return float(payload["allowance"])
            except Exception:
                return 0.0
        return 0.0

    @staticmethod
    def _normalize_usdc_units(v: float) -> float:
        """
        兼容两类返回：
        - 已是 USDC 单位（例如 8.71）
        - 最小单位（6 decimals，例如 8705330）
        """
        x = float(v)
        if x >= 100000:  # 100k USDC 对本策略账户不现实，判定为 6-decimal 原始值
            return x / 1_000_000.0
        return x

    def _normalize_conditional_units(self, v: float) -> float:
        """
        条件 token 份额归一化：
        在 signature_type=1 账户中，balance 常以 1e6 最小单位返回。
        """
        x = float(v)
        if self.signature_type == 1:
            # 对代理钱包统一按 6 decimals 归一化
            return x / 1_000_000.0
        # 兜底启发式（极大值时也做归一化）
        if x >= 100000:
            return x / 1_000_000.0
        return x

    def _diagnose_account_status(self) -> Dict[str, Any]:
        """
        交易前诊断：地址、USDC 余额、allowance、暴露比。
        """
        addr = self.client.get_address()
        collateral_payload: Dict[str, Any] = {}
        try:
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            p = self.client.get_balance_allowance(params)
            if isinstance(p, dict):
                collateral_payload = p
        except Exception as exc:
            LOGGER.warning("Account diagnose failed when reading collateral payload: %s", exc)

        balance = self._extract_balance_value(collateral_payload) if collateral_payload else 0.0
        balance = self._normalize_usdc_units(balance)
        allowance = self._extract_allowance_value(collateral_payload) if collateral_payload else 0.0
        exposure = self._estimate_portfolio_exposure_usdc()
        equity = balance + exposure
        exposure_ratio = (exposure / equity) if equity > 0 else 0.0
        ready = balance >= self.min_trade_usdc and allowance > 0

        diag = {
            "wallet_address": addr or "",
            "signature_type": self.signature_type,
            "funder": self.funder or "",
            "usdc_balance": round(balance, 6),
            "usdc_allowance": round(allowance, 6),
            "estimated_exposure": round(exposure, 6),
            "estimated_equity": round(equity, 6),
            "exposure_ratio": round(exposure_ratio, 6),
            "trade_ready": bool(ready),
        }
        return diag

    def _resolve_forecast_model(self) -> str:
        """
        自动协商可用模型：
        1) hrrr（若平台恢复支持将自动启用）
        2) gfs_seamless（官方说明为 GFS+HRRR 融合）
        3) best_match（最后兜底）
        """
        if self._forecast_model_name:
            return self._forecast_model_name

        candidates = ["hrrr", "gfs_seamless", "best_match"]
        for model_name in candidates:
            try:
                probe = self._request_json(
                    "GET",
                    self.OPEN_METEO_FORECAST,
                    params={
                        "latitude": 40.7769,
                        "longitude": -73.8740,
                        "hourly": "temperature_2m",
                        "timezone": "America/New_York",
                        "forecast_days": 1,
                        "models": model_name,
                    },
                    max_retries=1,
                )
                if isinstance(probe, dict) and probe.get("hourly"):
                    self._forecast_model_name = model_name
                    break
            except Exception:
                continue

        if not self._forecast_model_name:
            self._forecast_model_name = "best_match"

        if self._forecast_model_name == "hrrr":
            LOGGER.info("Forecast model selected: hrrr")
        elif self._forecast_model_name == "gfs_seamless":
            LOGGER.warning("Forecast model selected: gfs_seamless (GFS+HRRR blended)")
        else:
            LOGGER.warning("Forecast model selected: best_match fallback")
        return self._forecast_model_name

    def _safe_get_token_position(self, token_id: str) -> float:
        """
        读取指定 outcome token 的当前持仓份额。
        若鉴权过期，刷新后重试一次。
        """
        params = BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
        for attempt in range(2):
            try:
                payload = self.client.get_balance_allowance(params)
                if isinstance(payload, dict):
                    raw = self._extract_balance_value(payload)
                    return max(0.0, self._normalize_conditional_units(raw))
                return 0.0
            except PolyApiException as exc:
                if attempt == 0 and exc.status_code in (401, 403):
                    LOGGER.warning("CLOB auth expired when fetching position. Refresh creds.")
                    self._refresh_api_creds()
                    continue
                raise
        raise RuntimeError(f"Unable to fetch position for token {token_id}")

    def _safe_get_collateral_balance_usdc(self) -> float:
        """
        读取当前账户可用 USDC 余额（Collateral）。
        若鉴权过期，刷新后重试一次。
        """
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        for attempt in range(2):
            try:
                payload = self.client.get_balance_allowance(params)
                if isinstance(payload, dict):
                    raw = self._extract_balance_value(payload)
                    return max(0.0, self._normalize_usdc_units(raw))
                return 0.0
            except PolyApiException as exc:
                if attempt == 0 and exc.status_code in (401, 403):
                    LOGGER.warning("CLOB auth expired when fetching USDC balance. Refresh creds.")
                    self._refresh_api_creds()
                    continue
                raise
        raise RuntimeError("Unable to fetch collateral USDC balance")

    def _estimate_portfolio_exposure_usdc(self) -> float:
        """
        估算账户总持仓市值（USDC）：
        基于 positions_cost 中已知 token，读取实时仓位与价格。
        """
        total = 0.0
        for token_id in list(self.positions_cost.keys()):
            try:
                shares = self._safe_get_token_position(token_id)
                if shares <= 0:
                    continue
                px = self._safe_get_price_by_side(token_id, side="SELL")
                total += shares * max(0.0, px)
            except Exception:
                # 单 token 估值失败不应阻塞主流程
                continue
        return max(0.0, total)

    def _kelly_fraction(self, fair_prob: float, market_price: float) -> float:
        """
        凯利最优仓位比例：
        optimal_f = (p*(1/price)-1) / ((1/price)-1)
        """
        p = float(fair_prob)
        price = float(market_price)
        if price <= 0.0 or price >= 1.0:
            return 0.0
        b = (1.0 / price) - 1.0
        if b <= 0:
            return 0.0
        f = (p * (1.0 / price) - 1.0) / b
        return max(0.0, min(1.0, f))

    def _compute_dynamic_buy_amount(
        self,
        available_usdc: float,
        fair_prob: float,
        market_price: float,
        token_exposure_usdc: float,
        condition_exposure_usdc: float,
        total_exposure_usdc: float,
        edge_abs: Optional[float] = None,
        confidence_score: Optional[float] = None,
    ) -> float:
        """
        基于凯利公式 + 多层风控计算本次 BUY 金额（USDC）。
        """
        if available_usdc <= self.capital_reserve_usdc:
            return 0.0

        # 半凯利：降低模型误差导致的过度下注风险
        kelly_f = self._kelly_fraction(fair_prob, market_price)
        kelly_budget = available_usdc * (kelly_f * 0.5) * self.bankroll_fraction

        free_budget = max(0.0, available_usdc - self.capital_reserve_usdc)
        ratio_budget = available_usdc * self.capital_utilization_per_trade
        token_cap_budget = max(0.0, available_usdc * self.max_token_exposure_ratio - token_exposure_usdc)
        condition_cap_budget = max(0.0, available_usdc * self.max_condition_exposure_ratio - condition_exposure_usdc)

        # 全局暴露限制：总持仓市值不超过账户总权益 * limit
        total_equity = available_usdc + max(0.0, total_exposure_usdc)
        global_cap_budget = max(0.0, total_equity * self.total_exposure_limit - total_exposure_usdc)

        budget = min(
            kelly_budget,
            free_budget,
            ratio_budget,
            token_cap_budget,
            condition_cap_budget,
            global_cap_budget,
            self.max_trade_usdc,
        )
        # 先在内部做置信度缩放，避免外部再次打折导致补偿失效
        conf = 0.0 if confidence_score is None else float(confidence_score)
        budget = budget * max(0.2, min(1.0, conf))

        # 小额账户起注补偿（收紧版）：
        # 仅在“强信号”时才把不足 $1 的下单抬到 $1，避免噪音单硬凑起注。
        if edge_abs is None:
            edge_abs = fair_prob - market_price
        edge_ratio = (fair_prob / market_price) if market_price > 0 else 0.0
        if (
            0 < budget < self.exchange_min_buy_usdc
            and available_usdc >= self.exchange_min_buy_usdc
            and fair_prob >= self.min_fair_prob
            and market_price >= self.min_market_price
            and market_price <= self.max_market_price
            and float(edge_abs) >= 0.20
            and conf >= 0.75
            and edge_ratio >= self.min_edge_ratio
        ):
            budget = min(self.exchange_min_buy_usdc, self.max_trade_usdc)
        return max(0.0, budget)

    def _check_spread_filter(self, token_id: str) -> Dict[str, Any]:
        """
        盘口价差过滤：
        spread_ratio = (ask - bid) / ask
        ask: side=BUY（买入成交参考，通常更高）
        bid: side=SELL（卖出成交参考，通常更低）
        """
        ask_price = self._safe_get_price_by_side(token_id, side="BUY")
        bid_price = self._safe_get_price_by_side(token_id, side="SELL")
        spread_ratio = ((ask_price - bid_price) / ask_price) if ask_price > 0 else 1.0
        ok = spread_ratio <= 0.04
        return {
            "ok": ok,
            "ask_price": float(ask_price),
            "bid_price": float(bid_price),
            "spread_ratio": float(spread_ratio),
        }

    def _execute_buy(
        self,
        token_id: str,
        amount_usdc: float,
        fair_price: Optional[float] = None,
        city: str = "",
    ) -> Dict[str, Any]:
        """
        市价 BUY（FOK）：
        - 直接按金额下市价单，减少挂限价导致的错失与抖动
        - 成交后回写成本与仓位
        """
        for attempt in range(2):
            try:
                if amount_usdc < self.exchange_min_buy_usdc:
                    return {
                        "order_style": "SKIP_MIN_BUY",
                        "token_id": token_id,
                        "amount_usdc": round(amount_usdc, 4),
                        "reason": f"amount<{self.exchange_min_buy_usdc}",
                    }
                spread = self._check_spread_filter(token_id)
                if not bool(spread.get("ok", False)):
                    LOGGER.warning(
                        "SKIP_WIDE_SPREAD token=%s ask=%.4f bid=%.4f spread=%.2f%%",
                        token_id,
                        float(spread.get("ask_price", 0.0)),
                        float(spread.get("bid_price", 0.0)),
                        float(spread.get("spread_ratio", 0.0)) * 100.0,
                    )
                    return {
                        "order_style": "SKIP_WIDE_SPREAD",
                        "token_id": token_id,
                        "amount_usdc": round(amount_usdc, 4),
                        "ask_price": round(float(spread.get("ask_price", 0.0)), 4),
                        "bid_price": round(float(spread.get("bid_price", 0.0)), 4),
                        "spread_ratio": round(float(spread.get("spread_ratio", 0.0)), 6),
                    }
                notional = round(float(amount_usdc), 2)
                order_args = MarketOrderArgs(token_id=token_id, amount=notional, side="BUY")
                signed = self.client.create_market_order(order_args)
                result = self.client.post_order(signed, orderType="FOK")
                result["order_style"] = "MARKET_FOK"
                result["amount_usdc"] = notional
                result["ask_price"] = round(float(spread.get("ask_price", 0.0)), 4)
                result["bid_price"] = round(float(spread.get("bid_price", 0.0)), 4)
                result["spread_ratio"] = round(float(spread.get("spread_ratio", 0.0)), 6)
                fill_price, fill_size = self._extract_fill_price_size(result)
                fallback_price = self._safe_get_price(token_id)
                fallback_size = notional / max(fallback_price, 0.01)
                exec_price = float(fill_price or fallback_price or 0.0)
                exec_size = float(fill_size or fallback_size or 0.0)
                self._update_cost_on_buy(token_id, exec_price, exec_size)
                if exec_price > 0 and exec_size > 0:
                    self._write_trade_to_db(
                        bot_action="Buy",
                        token_id=token_id,
                        city=city,
                        price=exec_price,
                        shares=exec_size,
                        notional=float(notional),
                        raw_data=result if isinstance(result, dict) else {"result": str(result)},
                    )
                return result
            except PolyApiException as exc:
                if attempt == 0 and exc.status_code in (401, 403):
                    LOGGER.warning("CLOB auth expired when posting order. Refresh creds.")
                    self._refresh_api_creds()
                    continue
                raise

        raise RuntimeError(f"Unable to submit order for token {token_id}")

    def _try_synthetic_close_dust(
        self,
        token_id: str,
        size_shares: float,
        opposite_token_id: Optional[str],
        trigger_reason: str,
        market_price: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Dust 仓位合成平仓（对侧对冲）：
        - 当原仓位价值过小无法直接卖出时，尝试买入对侧 token 对冲方向风险。
        - 注意：py-clob-client 暂无 merge/redeem 直兑接口，此处为“风险对冲”，并非立即释放 USDC。
        """
        token_id = str(token_id or "")
        opp = str(opposite_token_id or "").strip()
        if not self.enable_synthetic_close_dust:
            return {"order_style": "SYNTHETIC_CLOSE_DISABLED", "token_id": token_id}
        if not token_id or not opp:
            return {
                "order_style": "SYNTHETIC_CLOSE_UNAVAILABLE",
                "token_id": token_id,
                "reason": "missing_opposite_token",
            }

        unhedged = self._effective_unhedged_shares(token_id, size_shares)
        if unhedged <= 1e-8:
            return {
                "order_style": "SYNTHETIC_ALREADY_HEDGED",
                "token_id": token_id,
                "opposite_token_id": opp,
                "hedged_shares": round(size_shares, 6),
            }

        try:
            opp_buy_price = self._safe_get_price_by_side(opp, side="BUY")
        except Exception as exc:
            return {
                "order_style": "SYNTHETIC_CLOSE_UNAVAILABLE",
                "token_id": token_id,
                "opposite_token_id": opp,
                "reason": f"opposite_price_failed:{exc}",
            }
        if opp_buy_price <= 0:
            return {
                "order_style": "SYNTHETIC_CLOSE_UNAVAILABLE",
                "token_id": token_id,
                "opposite_token_id": opp,
                "reason": "invalid_opposite_price",
            }

        full_hedge_notional = unhedged * opp_buy_price
        hedge_entry = self._get_hedge_entry(token_id)
        already_spent = float(hedge_entry.get("total_spent_usdc", 0.0))
        max_budget = float(self.synthetic_close_max_notional_usdc)
        remaining_budget = max(0.0, max_budget - already_spent)
        if remaining_budget <= 1e-8:
            return {
                "order_style": "SYNTHETIC_BUDGET_EXHAUSTED",
                "token_id": token_id,
                "opposite_token_id": opp,
                "trigger_reason": trigger_reason,
                "already_spent_usdc": round(already_spent, 4),
                "budget_cap_usdc": round(max_budget, 4),
            }
        # 先算理论对冲金额，再受剩余预算硬限制
        buy_notional = min(remaining_budget, full_hedge_notional)
        # 剩余预算不足最小下单额时，不再继续投入
        if buy_notional < self.synthetic_close_min_notional_usdc:
            return {
                "order_style": "SYNTHETIC_BUDGET_REMAIN_TOO_SMALL",
                "token_id": token_id,
                "opposite_token_id": opp,
                "trigger_reason": trigger_reason,
                "already_spent_usdc": round(already_spent, 4),
                "remaining_budget_usdc": round(remaining_budget, 4),
                "min_notional_usdc": round(float(self.synthetic_close_min_notional_usdc), 4),
            }
        est_hedged_shares = min(unhedged, buy_notional / max(opp_buy_price, 0.01))

        result: Dict[str, Any]
        if self.dry_run:
            LOGGER.warning(
                "\x1b[31mSYNTHETIC_CLOSE_SIGNAL\x1b[0m reason=%s token=%s opp=%s unhedged=%.4f est_hedged=%.4f buy=$%.2f mkt=%.4f",
                trigger_reason,
                token_id,
                opp,
                unhedged,
                est_hedged_shares,
                buy_notional,
                market_price,
            )
            result = {
                "order_style": "SYNTHETIC_DRY_RUN",
                "token_id": token_id,
                "opposite_token_id": opp,
                "trigger_reason": trigger_reason,
                "market_price": round(float(market_price), 6),
                "opposite_buy_price": round(float(opp_buy_price), 6),
                "unhedged_shares": round(float(unhedged), 6),
                "buy_notional": round(float(buy_notional), 4),
                "estimated_hedged_shares": round(float(est_hedged_shares), 6),
            }
        else:
            buy_res = self._execute_buy(
                opp,
                float(buy_notional),
                city=self._resolve_city_by_token(token_id),
            )
            fill_price, fill_size = self._extract_fill_price_size(buy_res)
            actual_hedged_shares = float(fill_size or est_hedged_shares or 0.0)
            spent_usdc = float(buy_res.get("amount_usdc", buy_notional) or buy_notional)
            spent_usdc = max(0.0, min(spent_usdc, remaining_budget))
            if actual_hedged_shares > 0:
                self._register_synthetic_hedge(
                    token_id,
                    min(unhedged, actual_hedged_shares),
                    spent_usdc,
                )
            result = {
                "order_style": "SYNTHETIC_HEDGE",
                "token_id": token_id,
                "opposite_token_id": opp,
                "trigger_reason": trigger_reason,
                "market_price": round(float(market_price), 6),
                "opposite_buy_price": round(float(fill_price or opp_buy_price), 6),
                "buy_notional": round(float(buy_notional), 4),
                "already_spent_usdc": round(already_spent, 4),
                "remaining_budget_usdc": round(remaining_budget, 4),
                "spent_usdc": round(spent_usdc, 4),
                "hedged_shares": round(float(actual_hedged_shares), 6),
                "buy_order_result": buy_res,
            }

        return result

    def check_and_exit_unmanaged_positions(
        self,
        covered_token_ids: set,
        live_positions: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        扫描并处理“不在本轮策略市场范围内”的持仓：
        - 仅基于账户盈亏做止盈/止损（因为无模型 fair_prob/edge）
        - 防止历史持仓长期无人管理
        """
        exits: List[Dict[str, Any]] = []
        for p in live_positions:
            token_id = str(p.get("token_id") or "")
            if not token_id or token_id in covered_token_ids:
                continue
            raw_pos = float(p.get("size", 0.0) or 0.0)
            pos = self._effective_unhedged_shares(token_id, raw_pos)
            if pos < self.min_position_to_sell:
                continue
            entry_price = float(self.positions_cost.get(token_id, {}).get("avg_price", 0.0) or 0.0)
            if entry_price <= 0:
                entry_price = float(p.get("avg_price", 0.0) or 0.0)
            market_price = float(p.get("cur_price", 0.0) or 0.0)
            if market_price <= 0:
                try:
                    market_price = self._safe_get_price(token_id)
                except Exception:
                    continue
            if entry_price <= 0:
                # 无法得到成本价时，不做盈亏驱动的卖出
                continue

            pnl_ratio = (market_price - entry_price) / entry_price
            exit_reason = ""
            sell_size = 0.0
            if market_price >= entry_price * self.take_profit_ratio:
                exit_reason = "take_profit_unmanaged"
                sell_size = min(pos, max(self.min_position_to_sell, pos * self.take_profit_sell_fraction))
            elif market_price <= entry_price * self.stop_loss_ratio:
                exit_reason = "hard_stop_loss_unmanaged"
                sell_size = pos
            if not exit_reason or sell_size < self.min_position_to_sell:
                continue

            action = {
                "city": "账户持仓",
                "date": str(p.get("end_date") or ""),
                "date_label": str(p.get("end_date") or ""),
                "condition_id": str(p.get("condition_id") or ""),
                "question": str(p.get("title") or ""),
                "label": f"[UNMANAGED] {str(p.get('outcome') or 'Yes')} {str(p.get('title') or '')}".strip(),
                "token_id": token_id,
                "forecast_max": "",
                "forecast_unit": "",
                "market_price": round(market_price, 4),
                "fair_prob": "",
                "edge": "",
                "edge_ratio": "",
                "model_source": "",
                "stable": "",
                "current_position_shares": round(raw_pos, 4),
                "unhedged_position_shares": round(pos, 4),
                "signal": "REDUCE",
                "reduce_size_shares": round(sell_size, 4),
                "entry_price": round(entry_price, 4),
                "unrealized_pnl": round(pnl_ratio, 4),
                "exit_reason": exit_reason,
                "hold_reason": "outside_strategy_scope",
            }
            exits.append(action)

            if self.dry_run:
                LOGGER.warning(
                    "\x1b[31mUNMANAGED_EXIT_SIGNAL\x1b[0m reason=%s token=%s entry=%.4f now=%.4f pnl=%.2f%% size=%.4f",
                    exit_reason,
                    token_id,
                    entry_price,
                    market_price,
                    pnl_ratio * 100,
                    sell_size,
                )
            else:
                try:
                    est_notional = max(0.0, market_price * sell_size)
                    if est_notional < self.exchange_min_buy_usdc:
                        synth = self._try_synthetic_close_dust(
                            token_id=token_id,
                            size_shares=sell_size,
                            opposite_token_id=self.opposite_token_map.get(token_id),
                            trigger_reason=f"dust_{exit_reason}_unmanaged",
                            market_price=market_price,
                        )
                        action["signal"] = "HOLD"
                        action["reduce_size_shares"] = 0.0
                        action["exit_reason"] = "synthetic_close_dust"
                        action["hold_reason"] = "synthetic_hedged_wait_settlement"
                        action["order_result"] = synth
                    else:
                        result = self._execute_sell(
                            token_id,
                            sell_size,
                            city=str(action.get("city") or ""),
                        )
                        action["order_result"] = result
                        LOGGER.info("Unmanaged exit submitted reason=%s token=%s size=%.4f", exit_reason, token_id, sell_size)
                except Exception as exc:
                    action["signal"] = "HOLD"
                    action["reduce_size_shares"] = 0.0
                    action["exit_reason"] = "synthetic_close_dust"
                    action["hold_reason"] = f"unmanaged_exit_failed:{exc}"
                    action["order_result"] = self._try_synthetic_close_dust(
                        token_id=token_id,
                        size_shares=sell_size,
                        opposite_token_id=self.opposite_token_map.get(token_id),
                        trigger_reason=f"sell_failed_{exit_reason}_unmanaged",
                        market_price=market_price,
                    )
        return exits

    def _standby_flatten_live_positions(
        self,
        live_positions: List[Dict[str, Any]],
        skip_tokens: Optional[set] = None,
    ) -> List[Dict[str, Any]]:
        """
        Standby 模式下尝试平仓：
        - 停止所有新买入
        - 对可成交仓位尽量清仓
        """
        skip = skip_tokens or set()
        actions: List[Dict[str, Any]] = []
        for p in live_positions:
            token_id = str(p.get("token_id") or "")
            if not token_id or token_id in skip:
                continue
            raw_pos = float(p.get("size", 0.0) or 0.0)
            pos = self._effective_unhedged_shares(token_id, raw_pos)
            if pos <= 0:
                continue
            market_price = float(p.get("cur_price", 0.0) or 0.0)
            if market_price <= 0:
                try:
                    market_price = self._safe_get_price(token_id)
                except Exception:
                    market_price = 0.0
            est_notional = max(0.0, pos * market_price)
            if est_notional < self.standby_force_exit_min_notional:
                # 尘埃仓位仅记录，避免反复无效下单
                actions.append(
                    {
                        "city": "账户持仓",
                        "date": str(p.get("end_date") or ""),
                        "date_label": str(p.get("end_date") or ""),
                        "condition_id": str(p.get("condition_id") or ""),
                        "question": str(p.get("title") or ""),
                        "label": f"[STANDBY] {str(p.get('outcome') or 'Yes')} {str(p.get('title') or '')}".strip(),
                        "token_id": token_id,
                        "market_price": round(market_price, 4),
                        "fair_prob": "",
                        "edge": "",
                        "edge_ratio": "",
                        "model_source": "",
                        "stable": "",
                        "current_position_shares": round(raw_pos, 4),
                        "unhedged_position_shares": round(pos, 4),
                        "signal": "HOLD",
                        "reduce_size_shares": 0.0,
                        "entry_price": round(float(self.positions_cost.get(token_id, {}).get("avg_price", 0.0) or 0.0), 4),
                        "unrealized_pnl": "",
                        "exit_reason": "",
                        "hold_reason": "standby_dust_unsellable",
                    }
                )
                continue

            action = {
                "city": "账户持仓",
                "date": str(p.get("end_date") or ""),
                "date_label": str(p.get("end_date") or ""),
                "condition_id": str(p.get("condition_id") or ""),
                "question": str(p.get("title") or ""),
                "label": f"[STANDBY] {str(p.get('outcome') or 'Yes')} {str(p.get('title') or '')}".strip(),
                "token_id": token_id,
                "market_price": round(market_price, 4),
                "fair_prob": "",
                "edge": "",
                "edge_ratio": "",
                "model_source": "",
                "stable": "",
                "current_position_shares": round(raw_pos, 4),
                "unhedged_position_shares": round(pos, 4),
                "signal": "REDUCE",
                "reduce_size_shares": round(pos, 4),
                "entry_price": round(float(self.positions_cost.get(token_id, {}).get("avg_price", 0.0) or 0.0), 4),
                "unrealized_pnl": "",
                "exit_reason": "standby_flatten",
                "hold_reason": "",
            }
            if self.dry_run:
                LOGGER.warning(
                    "\x1b[31mSTANDBY_FLATTEN_SIGNAL\x1b[0m token=%s size=%.4f est_notional=%.4f",
                    token_id,
                    pos,
                    est_notional,
                )
            else:
                try:
                    if est_notional < self.exchange_min_buy_usdc:
                        synth = self._try_synthetic_close_dust(
                            token_id=token_id,
                            size_shares=pos,
                            opposite_token_id=self.opposite_token_map.get(token_id),
                            trigger_reason="standby_dust_flatten",
                            market_price=market_price,
                        )
                        action["signal"] = "HOLD"
                        action["reduce_size_shares"] = 0.0
                        action["exit_reason"] = "synthetic_close_dust"
                        action["hold_reason"] = "synthetic_hedged_wait_settlement"
                        action["order_result"] = synth
                    else:
                        result = self._execute_sell(
                            token_id,
                            pos,
                            city=str(action.get("city") or ""),
                        )
                        action["order_result"] = result
                except Exception as exc:
                    action["signal"] = "HOLD"
                    action["exit_reason"] = "synthetic_close_dust"
                    action["hold_reason"] = f"standby_flatten_failed:{exc}"
                    action["order_result"] = self._try_synthetic_close_dust(
                        token_id=token_id,
                        size_shares=pos,
                        opposite_token_id=self.opposite_token_map.get(token_id),
                        trigger_reason="standby_sell_failed",
                        market_price=market_price,
                    )
            actions.append(action)
        return actions

    def _execute_sell(self, token_id: str, size_shares: float, city: str = "") -> Dict[str, Any]:
        """
        提交 SELL 单（优先 FOK，失败时回退 GTC）：
        - 对低价值 Dust 仓位，使用更激进折价，提升成交概率
        - 记录已实现盈亏，支撑日内亏损熔断
        """
        for attempt in range(2):
            try:
                ref_sell_price = self._safe_get_price_by_side(token_id, side="SELL")
                est_notional = max(0.0, ref_sell_price * max(0.0, size_shares))
                discount = self.sell_limit_discount
                if est_notional < self.dust_notional_threshold_usdc:
                    # Dust 仓位给更大让价，减少“挂不上/吃不到”的失败概率
                    discount = max(discount, 0.05)
                limit_price = max(0.01, ref_sell_price * (1.0 - discount))

                entry_price = float(self.positions_cost.get(str(token_id), {}).get("avg_price", 0.0) or 0.0)
                order_types = ["FOK", "GTC"]
                last_exc: Optional[Exception] = None
                for order_type in order_types:
                    try:
                        order_args = OrderArgs(token_id=token_id, price=limit_price, size=size_shares, side="SELL")
                        signed = self.client.create_order(order_args)
                        result = self.client.post_order(signed, orderType=order_type)
                        result["order_style"] = f"LIMIT_{order_type}"
                        result["limit_price"] = round(limit_price, 4)
                        result["size"] = round(size_shares, 6)
                        result["estimated_notional"] = round(est_notional, 6)
                        fill_price, fill_size = self._extract_fill_price_size(result)

                        realized_fill_size = 0.0
                        realized_fill_price = 0.0
                        if fill_size and float(fill_size) > 0:
                            realized_fill_size = float(fill_size)
                            realized_fill_price = float(fill_price or limit_price)
                            self._update_cost_on_sell(token_id, realized_fill_size)
                        elif order_type == "FOK":
                            # FOK 成交通常是全成全撤，若回包未给 size，按全成近似处理
                            realized_fill_size = float(size_shares)
                            realized_fill_price = float(fill_price or limit_price)
                            self._update_cost_on_sell(token_id, realized_fill_size)
                        else:
                            # GTC 且暂无成交，不更新成本，等待后续轮次同步真实仓位
                            realized_fill_size = 0.0

                        if entry_price > 0 and realized_fill_size > 0:
                            realized_pnl = (realized_fill_price - entry_price) * realized_fill_size
                            self._record_realized_pnl(realized_pnl)
                            result["realized_pnl"] = round(realized_pnl, 6)
                        if realized_fill_size > 0 and realized_fill_price > 0:
                            self._write_trade_to_db(
                                bot_action="Sell",
                                token_id=token_id,
                                city=city,
                                price=float(realized_fill_price),
                                shares=float(realized_fill_size),
                                notional=float(realized_fill_price * realized_fill_size),
                                raw_data=result if isinstance(result, dict) else {"result": str(result)},
                            )
                        return result
                    except PolyApiException as inner_exc:
                        last_exc = inner_exc
                        # FOK 失败时允许回退 GTC
                        if order_type == "FOK":
                            continue
                        raise
                if last_exc:
                    raise last_exc
            except PolyApiException as exc:
                if attempt == 0 and exc.status_code in (401, 403):
                    LOGGER.warning("CLOB auth expired when posting sell order. Refresh creds.")
                    self._refresh_api_creds()
                    continue
                raise

        raise RuntimeError(f"Unable to submit sell order for token {token_id}")
    @staticmethod
    def _signal_summary(actions: List[Dict[str, Any]]) -> Dict[str, int]:
        """统计各信号数量。"""
        summary = {"BUY": 0, "HOLD": 0, "REDUCE": 0}
        for item in actions:
            s = str(item.get("signal") or "")
            if s in summary:
                summary[s] += 1
        return summary

    @staticmethod
    def _discovery_summary(actions: List[Dict[str, Any]]) -> Dict[str, int]:
        """统计市场发现结果数量。"""
        summary = {"FOUND": 0, "SKIP": 0}
        for item in actions:
            label = str(item.get("label") or "")
            if label.startswith("[DISCOVERY]") and "FOUND" in label:
                summary["FOUND"] += 1
            elif label.startswith("[DISCOVERY]") and "SKIP" in label:
                summary["SKIP"] += 1
        return summary

    def _run_summary(self, actions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """单次运行的摘要。"""
        return {
            "total_rows": len(actions),
            "signal_summary": self._signal_summary(actions),
            "discovery_summary": self._discovery_summary(actions),
        }

    def _build_discovery_debug_rows(self) -> List[Dict[str, Any]]:
        """
        将市场发现调试信息转换为报告可展示的行。
        这样即使没有交易机会，也能在前端看到机器人做了什么。
        """
        rows: List[Dict[str, Any]] = []
        for item in self.last_discovery_debug:
            status = str(item.get("status") or "")
            signal = "HOLD" if status == "SKIP" else "DISCOVERY"
            reason = str(item.get("reason") or "")
            city = str(item.get("city") or "")
            if status == "FOUND":
                reason = f"discovered condition={item.get('condition_id','')}"
            rows.append(
                {
                    "city": city,
                    "date": item.get("date", ""),
                    "date_label": item.get("date_label", ""),
                    "signal": signal,
                    "label": f"[DISCOVERY] {city} {status}".strip(),
                    "token_id": "",
                    "condition_id": item.get("condition_id", ""),
                    "question": item.get("question", ""),
                    "market_price": "",
                    "fair_prob": "",
                    "edge": "",
                    "edge_ratio": "",
                    "model_source": "",
                    "current_position_shares": "",
                    "total_condition_position_shares": "",
                    "dynamic_buy_usdc": "",
                    "entry_price": "",
                    "unrealized_pnl": "",
                    "exit_reason": "",
                    "hold_reason": f"candidates={item.get('candidates','')} best_score={item.get('best_score','')} {reason}".strip(),
                }
            )
        return rows
    def _write_history_snapshot(self, generated_dt: datetime, actions: List[Dict[str, Any]]) -> None:
        """写历史快照文件并更新按日期倒序的索引。"""
        history_dir = self.report_dir / "history"
        history_dir.mkdir(parents=True, exist_ok=True)
        stamp = generated_dt.strftime("%Y-%m-%d_%H%M%S")
        date_key = generated_dt.strftime("%Y-%m-%d")
        generated_at = generated_dt.strftime("%Y-%m-%d %H:%M:%S %Z")

        rel_file = f"history/{stamp}.json"
        run_summary = self._run_summary(actions)
        snap_payload = {
            "generated_at": generated_at,
            "generated_at_iso": generated_dt.isoformat(),
            "date_key": date_key,
            "actions": actions,
            "run_summary": run_summary,
        }
        (self.report_dir / "history" / f"{stamp}.json").write_text(
            json.dumps(snap_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        idx_path = self.report_dir / self.history_index_file
        if idx_path.exists():
            try:
                idx_data = json.loads(idx_path.read_text(encoding="utf-8"))
            except Exception:
                idx_data = {"history": []}
        else:
            idx_data = {"history": []}

        entry = {
            "generated_at": generated_at,
            "generated_at_iso": generated_dt.isoformat(),
            "date_key": date_key,
            "file": rel_file,
            "actions_count": len(actions),
            "signal_summary": run_summary["signal_summary"],
            "discovery_summary": run_summary["discovery_summary"],
        }
        history = [x for x in idx_data.get("history", []) if x.get("file") != rel_file]
        history.append(entry)
        history.sort(key=lambda x: str(x.get("generated_at_iso", "")), reverse=True)
        idx_data["history"] = history
        idx_data["updated_at"] = generated_at
        idx_path.write_text(json.dumps(idx_data, ensure_ascii=False, indent=2), encoding="utf-8")
        self._db_write_run_actions(
            generated_at=generated_at,
            generated_at_iso=generated_dt.isoformat(),
            date_key=date_key,
            actions=actions,
            run_summary=run_summary,
            source_file=rel_file,
            progress=None,
            payload_json=json.dumps(snap_payload, ensure_ascii=False),
        )

        LOGGER.info("History snapshot written: %s | index=%s", rel_file, idx_path)

    def _write_static_report(
        self,
        actions: List[Dict[str, Any]],
        write_history: Optional[bool] = None,
        progress: Optional[Dict[str, Any]] = None,
    ) -> None:
        """写出静态报告文件，并可选写历史快照。"""
        self.report_dir.mkdir(parents=True, exist_ok=True)
        generated_dt = datetime.now(self.NYC_TZ)
        generated_at = generated_dt.strftime("%Y-%m-%d %H:%M:%S %Z")
        run_summary = self._run_summary(actions)
        payload = {
            "generated_at": generated_at,
            "generated_at_iso": generated_dt.isoformat(),
            "actions": actions,
            "run_summary": run_summary,
        }
        if isinstance(progress, dict):
            payload["progress"] = progress

        json_path = self.report_dir / "latest_actions.json"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        self._db_write_run_actions(
            generated_at=generated_at,
            generated_at_iso=generated_dt.isoformat(),
            date_key=generated_dt.strftime("%Y-%m-%d"),
            actions=actions,
            run_summary=run_summary,
            source_file="latest_actions.json",
            progress=progress,
            payload_json=json.dumps(payload, ensure_ascii=False),
        )
        LOGGER.info("Static report written: %s", json_path)

        should_write_history = self.write_history_report if write_history is None else bool(write_history)
        if should_write_history:
            self._write_history_snapshot(generated_dt, actions)

    def _compute_source_dispersion_for_date(self, date_key: str) -> float:
        """
        计算指定日期多气象源预测值的标准差（用于动态 sigma）。
        若源不足 2 个，返回 0。
        """
        details = self.current_model_details if isinstance(self.current_model_details, dict) else {}
        sources = details.get("sources", {}) if isinstance(details.get("sources", {}), dict) else {}
        vals: List[float] = []
        for daily in sources.values():
            if not isinstance(daily, dict):
                continue
            if date_key in daily:
                try:
                    vals.append(float(daily[date_key]))
                except Exception:
                    continue
        if len(vals) < 2:
            return 0.0
        try:
            return float(statistics.pstdev(vals))
        except Exception:
            return 0.0

    def _write_diagnostics(self, payload: Dict[str, Any]) -> None:
        """写出高频诊断文件 diagnostics.json（每轮覆盖，保留最新全量决策上下文）。"""
        try:
            self.report_dir.mkdir(parents=True, exist_ok=True)
            self.diagnostics_file.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self._db_write_diagnostics(payload)
            self._write_diagnostics_to_db(payload)
        except Exception as exc:
            LOGGER.warning("Failed to write diagnostics file: %s", exc)

    def check_and_exit_positions(
        self,
        city_name: str,
        market: DailyMarket,
        scored: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        卖出决策矩阵（账户盈亏 + 模型双驱动）：
        A) 止盈：价格 >= 成本 * take_profit_ratio，卖出 50%
        B) 硬止损：价格 <= 成本 * stop_loss_ratio，立即清仓
        C) 模型反转：edge <= -edge_threshold，立即清仓
        D) 临近结算保护：距结算 < pre_settle_hours 且收益 < pre_settle_min_pnl_ratio
           - pnl<0: 清仓
           - pnl>=0: 按 pre_settle_reduce_fraction 减仓
        """
        exits: List[Dict[str, Any]] = []
        hold_position_actions: List[Dict[str, Any]] = []
        hours_to_settle = self._hours_to_settle(market.settle_time_iso)
        highest_updated = False
        for item in scored:
            raw_pos = float(item.get("current_position_shares", 0.0))
            token_id = str(item.get("token_id") or "")
            pos = self._effective_unhedged_shares(token_id, raw_pos)
            if pos < self.min_position_to_sell:
                token_id_skip = token_id
                if token_id_skip:
                    try:
                        self.fair_prob_state[token_id_skip] = float(item.get("fair_prob", 0.0))
                    except Exception:
                        pass
                continue

            cost_info = self.positions_cost.get(token_id, {})
            entry_price = float(cost_info.get("avg_price", 0.0))
            if entry_price <= 0:
                try:
                    self.fair_prob_state[token_id] = float(item.get("fair_prob", 0.0))
                except Exception:
                    pass
                continue

            market_price = float(item.get("bid_price", item.get("market_price", 0.0)))
            edge = float(item.get("edge", 0.0))
            fair_prob_now = float(item.get("fair_prob", 0.0))
            fair_prob_prev = float(self.fair_prob_state.get(token_id, fair_prob_now))
            fair_prob_drop = fair_prob_prev - fair_prob_now
            model_shift_down = fair_prob_drop >= self.model_shift_exit_delta
            pnl_ratio = (market_price - entry_price) / entry_price if entry_price > 0 else 0.0
            highest_seen = float(cost_info.get("highest_price_seen", entry_price))
            highest_seen = max(highest_seen, market_price)
            if token_id in self.positions_cost:
                old_high = float(self.positions_cost[token_id].get("highest_price_seen", entry_price))
                if highest_seen > old_high + 1e-8:
                    self.positions_cost[token_id]["highest_price_seen"] = highest_seen
                    highest_updated = True

            exit_reason = ""
            sell_size = 0.0
            trailing_armed = highest_seen >= entry_price * 1.3
            trailing_stop_price = highest_seen * 0.9
            if model_shift_down and edge <= 0:
                exit_reason = "model_shift_exit"
                sell_size = pos
            elif trailing_armed and market_price <= trailing_stop_price:
                exit_reason = "trailing_stop_loss"
                sell_size = pos
            elif market_price >= entry_price * self.take_profit_ratio:
                exit_reason = "take_profit"
                sell_size = max(self.min_position_to_sell, pos * self.take_profit_sell_fraction)
                sell_size = min(sell_size, pos)
            elif market_price <= entry_price * self.stop_loss_ratio:
                exit_reason = "hard_stop_loss"
                sell_size = pos
            elif (
                hours_to_settle is not None
                and hours_to_settle <= self.pre_settle_hours
                and pnl_ratio < self.pre_settle_min_pnl_ratio
            ):
                exit_reason = "pre_settle_guard"
                if pnl_ratio < 0:
                    sell_size = pos
                else:
                    sell_size = min(pos, max(self.min_position_to_sell, pos * self.pre_settle_reduce_fraction))
            elif edge <= -self.edge_threshold:
                exit_reason = "model_reversal"
                sell_size = pos

            if not exit_reason or sell_size < self.min_position_to_sell:
                hold_position_actions.append(
                    {
                        "city": city_name,
                        "date": item.get("date"),
                        "date_label": item.get("date_label"),
                        "condition_id": market.condition_id,
                        "question": market.question,
                        "label": f"[POSITION] {item.get('label')}",
                        "token_id": token_id,
                        "forecast_max": item.get("forecast_max"),
                        "forecast_unit": item.get("forecast_unit"),
                        "market_price": round(market_price, 4),
                        "fair_prob": round(fair_prob_now, 4),
                        "edge": round(edge, 4),
                        "edge_ratio": item.get("edge_ratio", ""),
                        "model_source": item.get("model_source", ""),
                        "stable": item.get("stable"),
                        "current_position_shares": round(raw_pos, 4),
                        "unhedged_position_shares": round(pos, 4),
                        "signal": "HOLD",
                        "reduce_size_shares": 0.0,
                        "entry_price": round(entry_price, 4),
                        "unrealized_pnl": round(pnl_ratio, 4),
                        "exit_reason": "",
                        "hold_reason": "POSITION_KEEP",
                        "highest_price_seen": round(highest_seen, 4),
                        "trailing_stop_price": round(trailing_stop_price, 4) if trailing_armed else "",
                        "fair_prob_prev": round(fair_prob_prev, 4),
                        "fair_prob_drop": round(fair_prob_drop, 4),
                    }
                )
                self.fair_prob_state[token_id] = fair_prob_now
                continue

            action = {
                "city": city_name,
                "date": item.get("date"),
                "date_label": item.get("date_label"),
                "condition_id": market.condition_id,
                "question": market.question,
                "label": item.get("label"),
                "token_id": token_id,
                "forecast_max": item.get("forecast_max"),
                "forecast_unit": item.get("forecast_unit"),
                "market_price": round(market_price, 4),
                "fair_prob": item.get("fair_prob"),
                "edge": round(edge, 4),
                "edge_ratio": item.get("edge_ratio", ""),
                "model_source": item.get("model_source", ""),
                "stable": item.get("stable"),
                "current_position_shares": round(raw_pos, 4),
                "unhedged_position_shares": round(pos, 4),
                "signal": "REDUCE",
                "reduce_size_shares": round(sell_size, 4),
                "entry_price": round(entry_price, 4),
                "unrealized_pnl": round(pnl_ratio, 4),
                "exit_reason": exit_reason,
                "hold_reason": "",
                "highest_price_seen": round(highest_seen, 4),
                "trailing_stop_price": round(trailing_stop_price, 4) if trailing_armed else "",
                "fair_prob_prev": round(fair_prob_prev, 4),
                "fair_prob_drop": round(fair_prob_drop, 4),
            }
            self.fair_prob_state[token_id] = fair_prob_now

            est_notional = max(0.0, float(market_price) * float(sell_size))
            should_try_synth = (
                est_notional < self.exchange_min_buy_usdc
                and bool(item.get("opposite_token_id"))
            )

            if should_try_synth:
                synth = self._try_synthetic_close_dust(
                    token_id=token_id,
                    size_shares=sell_size,
                    opposite_token_id=str(item.get("opposite_token_id") or ""),
                    trigger_reason=f"dust_{exit_reason}",
                    market_price=market_price,
                )
                action["signal"] = "HOLD"
                action["reduce_size_shares"] = 0.0
                action["exit_reason"] = "synthetic_close_dust"
                action["hold_reason"] = "synthetic_hedged_wait_settlement"
                action["order_result"] = synth
                exits.append(action)
                continue

            exits.append(action)
            if self.dry_run:
                LOGGER.warning(
                    "\x1b[31mEXIT_SIGNAL\x1b[0m [%s %s] reason=%s token=%s entry=%.4f now=%.4f pnl=%.2f%% size=%.4f",
                    city_name,
                    item.get("date_label"),
                    exit_reason,
                    token_id,
                    entry_price,
                    market_price,
                    pnl_ratio * 100,
                    sell_size,
                )
            else:
                try:
                    result = self._execute_sell(token_id, sell_size, city=city_name)
                    action["order_result"] = result
                    LOGGER.info(
                        "Exit submitted [%s %s] reason=%s token=%s size=%.4f",
                        city_name,
                        item.get("date_label"),
                        exit_reason,
                        token_id,
                        sell_size,
                    )
                except Exception as exc:
                    synth = self._try_synthetic_close_dust(
                        token_id=token_id,
                        size_shares=sell_size,
                        opposite_token_id=str(item.get("opposite_token_id") or ""),
                        trigger_reason=f"sell_failed_{exit_reason}",
                        market_price=market_price,
                    )
                    action["signal"] = "HOLD"
                    action["reduce_size_shares"] = 0.0
                    action["exit_reason"] = "synthetic_close_dust"
                    action["hold_reason"] = f"sell_failed:{exc}"
                    action["order_result"] = synth
        if highest_updated:
            self._save_positions_cost()
        return exits + hold_position_actions

    def run_once(self) -> List[Dict[str, Any]]:
        """
        执行一次完整策略：
        1) 先探测并锁定市场（Discovery First）
        2) 仅在“锁定成功”后获取预测数据
        3) 对每个 outcome 计算 fair_prob / edge
        4) 选取当日最优 outcome 判定 BUY/HOLD
        """
        actions: List[Dict[str, Any]] = []
        diagnostics_rows: List[Dict[str, Any]] = []
        covered_token_ids: set = set()
        diag = self._diagnose_account_status()
        available_usdc = float(diag.get("usdc_balance", 0.0))
        live_positions = self._fetch_live_positions()
        self._sync_positions_cost_from_live_positions(live_positions, bootstrap_only_missing=False)
        base_total_exposure_usdc = self._estimate_portfolio_exposure_usdc()
        equity = float(diag.get("estimated_equity", available_usdc + base_total_exposure_usdc))
        realized_today = self._get_today_realized_pnl()
        standby_mode = bool(
            self.enable_daily_loss_standby
            and equity > 0
            and realized_today <= -(equity * self.daily_loss_limit_ratio)
        )
        LOGGER.info(
            "Account diagnose | address=%s sig_type=%s funder=%s balance=%.4f allowance=%.4f exposure=%.4f equity=%.4f exp_ratio=%.3f ready=%s today_realized=%.4f standby=%s",
            diag.get("wallet_address", ""),
            str(diag.get("signature_type", "")),
            str(diag.get("funder", "")),
            available_usdc,
            float(diag.get("usdc_allowance", 0.0)),
            float(diag.get("estimated_exposure", 0.0)),
            float(diag.get("estimated_equity", 0.0)),
            float(diag.get("exposure_ratio", 0.0)),
            str(diag.get("trade_ready", False)),
            realized_today,
            str(standby_mode),
        )
        if standby_mode:
            LOGGER.warning(
                "STANDBY ON: realized_today=%.4f <= -%.2f%% equity(%.4f). Buy disabled, flatten enabled.",
                realized_today,
                self.daily_loss_limit_ratio * 100.0,
                equity,
            )
        # 记录本轮已评估 token 的持仓市值，用于全局暴露限制
        exposure_by_token: Dict[str, float] = {}
        if self.city_configs:
            start_idx = self._city_scan_cursor % len(self.city_configs)
            city_order = self.city_configs[start_idx:] + self.city_configs[:start_idx]
            self._city_scan_cursor = (self._city_scan_cursor + 1) % len(self.city_configs)
        else:
            city_order = []
        total_cities = len(city_order)
        LOGGER.info("City scan order: %s", [c.get("name", "") for c in city_order])

        for city_idx, city_cfg in enumerate(city_order, start=1):
            city_name = city_cfg["name"]
            markets = self.discover_daily_markets(city_cfg)
            if not markets:
                LOGGER.warning("%s: no discovered markets this round.", city_name)
                actions.extend(self._build_discovery_debug_rows())
                if self.write_static_report:
                    self._write_static_report(
                        actions,
                        write_history=False,
                        progress={
                            "stage": "city_no_market",
                            "city": city_name,
                            "city_index": city_idx,
                            "total_cities": total_cities,
                        },
                    )
                continue

            # 仅当至少一个日期探测成功后，再请求天气预测
            try:
                forecasts = self.fetch_city_daily_max_forecast(city_cfg)
            except Exception as exc:
                LOGGER.warning("%s forecast fetch failed after discovery: %s", city_name, exc)
                actions.extend(self._build_discovery_debug_rows())
                if self.write_static_report:
                    self._write_static_report(
                        actions,
                        write_history=False,
                        progress={
                            "stage": "city_forecast_failed",
                            "city": city_name,
                            "city_index": city_idx,
                            "total_cities": total_cities,
                            "error": str(exc),
                        },
                    )
                continue
            forecast_unit_name = str(city_cfg.get("temp_unit", "fahrenheit")).lower()
            city_base_sigma = float(city_cfg.get("base_sigma", self.temp_sigma_f) or self.temp_sigma_f)
            for date_key, market in markets.items():
                if date_key not in forecasts:
                    LOGGER.warning("%s %s forecast missing after probe.", city_name, date_key)
                    continue
                forecast_max = forecasts[date_key]
                confidence_map = self.current_model_details.get("confidence_by_date", {})
                disagreement_map = self.current_model_details.get("disagreement_by_date", {})
                source_weights_map = self.current_model_details.get("source_weights_by_date", {})
                confidence_score = float(confidence_map.get(date_key, 0.35))
                disagreement_index = float(disagreement_map.get(date_key, 0.0))
                source_weights_used = source_weights_map.get(date_key, {}) if isinstance(source_weights_map, dict) else {}
                temp_unit_label = "F" if city_cfg.get("temp_unit", "fahrenheit") == "fahrenheit" else "C"
                LOGGER.info(
                    "%s Date %s (%s): forecast max %.2f%s | confidence=%.3f disagreement=%.3f",
                    city_name,
                    date_key,
                    market.date_label,
                    forecast_max,
                    temp_unit_label,
                    confidence_score,
                    disagreement_index,
                )

                scored: List[Dict[str, Any]] = []
                for out in market.outcomes:
                    covered_token_ids.add(str(out.yes_token_id))
                    covered_token_ids.add(str(out.no_token_id))
                    fair_prob_yes, effective_range = self.model_probability(
                        forecast_max,
                        out.label,
                        forecast_unit=forecast_unit_name,
                        settle_time_iso=market.settle_time_iso,
                        base_sigma_f=city_base_sigma,
                        return_effective_range=True,
                    )
                    fair_prob_no = max(0.0, min(1.0, 1.0 - float(fair_prob_yes)))
                    try:
                        market_price_yes = self._safe_get_price(out.yes_token_id)
                    except Exception as exc:
                        LOGGER.warning("Skip YES token price unavailable: token=%s err=%s", out.yes_token_id, exc)
                        continue
                    try:
                        bid_price_yes = self._safe_get_price_by_side(out.yes_token_id, side="SELL")
                    except Exception:
                        bid_price_yes = float(market_price_yes)
                    try:
                        market_price_no = self._safe_get_price(out.no_token_id)
                    except Exception as exc:
                        LOGGER.warning("Skip NO token price unavailable: token=%s err=%s", out.no_token_id, exc)
                        continue
                    try:
                        bid_price_no = self._safe_get_price_by_side(out.no_token_id, side="SELL")
                    except Exception:
                        bid_price_no = float(market_price_no)
                    try:
                        current_position_yes = self._safe_get_token_position(out.yes_token_id)
                        current_position_no = self._safe_get_token_position(out.no_token_id)
                    except Exception as exc:
                        LOGGER.warning(
                            "Skip token position unavailable: yes=%s no=%s err=%s",
                            out.yes_token_id,
                            out.no_token_id,
                            exc,
                        )
                        continue

                    # 维护 YES/NO 对侧映射，供 dust 合成平仓使用
                    yes_id = str(out.yes_token_id)
                    no_id = str(out.no_token_id)
                    if yes_id and no_id and yes_id != no_id:
                        self.opposite_token_map[yes_id] = no_id
                        self.opposite_token_map[no_id] = yes_id
                    self._remember_token_city(yes_id, city_name)
                    self._remember_token_city(no_id, city_name)

                    edge_yes = fair_prob_yes - market_price_yes
                    edge_no = fair_prob_no - market_price_no
                    edge_ratio_yes = (fair_prob_yes / market_price_yes) if market_price_yes > 0 else 0.0
                    edge_ratio_no = (fair_prob_no / market_price_no) if market_price_no > 0 else 0.0
                    stable_yes = self._is_stable_interval(
                        forecast_max,
                        out.label,
                        fair_prob_yes,
                        forecast_unit=forecast_unit_name,
                    )
                    # NO 方向稳定性不依赖区间边界，按概率阈值直接判定
                    stable_no = fair_prob_no >= self.stability_prob_threshold

                    slot_key = f"{market.condition_id}::{out.label}"
                    scored.append(
                        {
                            "city": city_name,
                            "date": date_key,
                            "date_label": market.date_label,
                            "condition_id": market.condition_id,
                            "question": market.question,
                            "base_label": out.label,
                            "label": f"{out.label} (YES)",
                            "side": "YES",
                            "slot_key": slot_key,
                            "token_id": out.yes_token_id,
                            "opposite_token_id": out.no_token_id,
                            "opposite_market_price": round(market_price_no, 4),
                            "forecast_max": round(forecast_max, 2),
                            "forecast_unit": temp_unit_label,
                            "market_price": round(market_price_yes, 4),
                            "bid_price": round(float(bid_price_yes), 4),
                            "fair_prob": round(fair_prob_yes, 4),
                            "edge": round(edge_yes, 4),
                            "edge_ratio": round(edge_ratio_yes, 4),
                            "effective_range": effective_range.get("display", "") if isinstance(effective_range, dict) else "",
                            "model_source": self.current_model_source,
                            "stable": stable_yes,
                            "current_position_shares": round(current_position_yes, 4),
                            "entry_price": round(
                                float(self.positions_cost.get(out.yes_token_id, {}).get("avg_price", 0.0)), 4
                            ),
                            "unrealized_pnl": round(
                                (
                                    (float(market_price_yes) - float(self.positions_cost.get(out.yes_token_id, {}).get("avg_price", 0.0)))
                                    / float(self.positions_cost.get(out.yes_token_id, {}).get("avg_price", 0.0))
                                )
                                if float(self.positions_cost.get(out.yes_token_id, {}).get("avg_price", 0.0)) > 0
                                else 0.0,
                                4,
                            ),
                            "exit_reason": "",
                            "base_sigma": round(city_base_sigma, 4),
                            "confidence_score": round(confidence_score, 4),
                            "disagreement_index": round(disagreement_index, 4),
                            "source_weights": source_weights_used,
                        }
                    )
                    scored.append(
                        {
                            "city": city_name,
                            "date": date_key,
                            "date_label": market.date_label,
                            "condition_id": market.condition_id,
                            "question": market.question,
                            "base_label": out.label,
                            "label": f"{out.label} (NO)",
                            "side": "NO",
                            "slot_key": slot_key,
                            "token_id": out.no_token_id,
                            "opposite_token_id": out.yes_token_id,
                            "opposite_market_price": round(market_price_yes, 4),
                            "forecast_max": round(forecast_max, 2),
                            "forecast_unit": temp_unit_label,
                            "market_price": round(market_price_no, 4),
                            "bid_price": round(float(bid_price_no), 4),
                            "fair_prob": round(fair_prob_no, 4),
                            "edge": round(edge_no, 4),
                            "edge_ratio": round(edge_ratio_no, 4),
                            "effective_range": effective_range.get("display", "") if isinstance(effective_range, dict) else "",
                            "model_source": self.current_model_source,
                            "stable": stable_no,
                            "current_position_shares": round(current_position_no, 4),
                            "entry_price": round(
                                float(self.positions_cost.get(out.no_token_id, {}).get("avg_price", 0.0)), 4
                            ),
                            "unrealized_pnl": round(
                                (
                                    (float(market_price_no) - float(self.positions_cost.get(out.no_token_id, {}).get("avg_price", 0.0)))
                                    / float(self.positions_cost.get(out.no_token_id, {}).get("avg_price", 0.0))
                                )
                                if float(self.positions_cost.get(out.no_token_id, {}).get("avg_price", 0.0)) > 0
                                else 0.0,
                                4,
                            ),
                            "exit_reason": "",
                            "base_sigma": round(city_base_sigma, 4),
                            "confidence_score": round(confidence_score, 4),
                            "disagreement_index": round(disagreement_index, 4),
                            "source_weights": source_weights_used,
                        }
                    )
                    diagnostics_rows.append(
                        {
                            "city": city_name,
                            "date": date_key,
                            "date_label": market.date_label,
                            "condition_id": market.condition_id,
                            "base_label": out.label,
                            "forecast_max": round(float(forecast_max), 4),
                            "forecast_unit": temp_unit_label,
                            "confidence_score": round(confidence_score, 4),
                            "disagreement_index": round(disagreement_index, 4),
                            "source_weights": source_weights_used,
                            "effective_range": effective_range if isinstance(effective_range, dict) else {},
                            "yes": {
                                "token_id": out.yes_token_id,
                                "market_price": round(float(market_price_yes), 6),
                                "fair_prob": round(float(fair_prob_yes), 6),
                                "edge": round(float(edge_yes), 6),
                                "edge_ratio": round(float(edge_ratio_yes), 6),
                                "position_shares": round(float(current_position_yes), 6),
                            },
                            "no": {
                                "token_id": out.no_token_id,
                                "market_price": round(float(market_price_no), 6),
                                "fair_prob": round(float(fair_prob_no), 6),
                                "edge": round(float(edge_no), 6),
                                "edge_ratio": round(float(edge_ratio_no), 6),
                                "position_shares": round(float(current_position_no), 6),
                            },
                        }
                    )
                    exposure_by_token[out.yes_token_id] = float(market_price_yes) * float(current_position_yes)
                    exposure_by_token[out.no_token_id] = float(market_price_no) * float(current_position_no)

                # 先做卖出决策（止盈/止损/模型反转），再评估是否买入
                exit_actions = self.check_and_exit_positions(city_name, market, scored)
                actions.extend(exit_actions)
                exited_tokens = {str(x.get("token_id") or "") for x in exit_actions}

                if scored:
                    prob_map_parts = [
                        f"{x['label']}:p={float(x['fair_prob']):.3f}|m={float(x['market_price']):.3f}|e={float(x['edge']):.3f}"
                        for x in scored
                    ]
                    LOGGER.info("ProbMap [%s %s] %s", city_name, market.date_label, " || ".join(prob_map_parts))

                scored.sort(key=lambda x: x["edge"], reverse=True)
                best = scored[0] if scored else None
                if not best:
                    continue

                token_exposure_usdc = best["current_position_shares"] * best["market_price"]
                total_condition_position = sum(float(x["current_position_shares"]) for x in scored)
                condition_exposure_usdc = sum(
                    float(x["current_position_shares"]) * float(x["market_price"]) for x in scored
                )
                total_exposure_usdc = base_total_exposure_usdc + sum(exposure_by_token.values())
                dynamic_buy_usdc = self._compute_dynamic_buy_amount(
                    available_usdc=available_usdc,
                    fair_prob=float(best["fair_prob"]),
                    market_price=float(best["market_price"]),
                    token_exposure_usdc=token_exposure_usdc,
                    condition_exposure_usdc=condition_exposure_usdc,
                    total_exposure_usdc=total_exposure_usdc,
                    edge_abs=float(best["edge"]),
                    confidence_score=float(confidence_score),
                )
                kelly_f = self._kelly_fraction(float(best["fair_prob"]), float(best["market_price"]))
                est_buy_shares = dynamic_buy_usdc / max(best["market_price"], 0.01)
                projected_position = best["current_position_shares"] + est_buy_shares
                projected_total_condition_position = total_condition_position + est_buy_shares
                can_add_position = projected_position <= self.max_position_shares_per_token
                can_add_condition_total = (
                    projected_total_condition_position <= self.max_total_position_shares_per_condition
                )
                other_condition_position = max(
                    0.0, total_condition_position - float(best["current_position_shares"])
                )
                opposite_slot_position = sum(
                    float(x.get("current_position_shares", 0.0))
                    for x in scored
                    if str(x.get("slot_key", "")) == str(best.get("slot_key", ""))
                    and str(x.get("side", "")) != str(best.get("side", ""))
                )
                can_add_slot_direction = opposite_slot_position <= 1e-8
                can_add_single_condition = (
                    (not self.single_outcome_per_condition)
                    or float(best["current_position_shares"]) > 0
                    or other_condition_position <= 1e-8
                )
                hours_to_settle = self._hours_to_settle(market.settle_time_iso)
                settle_time_ok = (
                    True
                    if hours_to_settle is None
                    else hours_to_settle >= self.min_hours_to_settlement_for_entry
                )
                effective_min_trade = max(self.min_trade_usdc, self.exchange_min_buy_usdc)
                can_trade_amount = dynamic_buy_usdc >= effective_min_trade
                prob_ok = float(best["fair_prob"]) >= self.min_fair_prob
                price_low_ok = float(best["market_price"]) >= self.min_market_price
                is_no_side = (str(best.get("side", "")) == "NO")
                effective_max_price = 0.55 if is_no_side else self.max_market_price
                price_high_ok = float(best["market_price"]) <= effective_max_price
                price_ok = price_low_ok and price_high_ok
                edge_abs_ok = float(best["edge"]) >= self.edge_threshold
                edge_threshold_eff = self.edge_threshold * (1.0 + (1.0 - confidence_score) * 1.5)
                edge_abs_ok = float(best["edge"]) >= edge_threshold_eff
                edge_ratio_v = (
                    float(best["fair_prob"]) / float(best["market_price"])
                    if float(best["market_price"]) > 0
                    else 0.0
                )
                min_edge_ratio_eff = self.min_edge_ratio + (1.0 - confidence_score) * 0.4
                edge_ratio_ok = edge_ratio_v >= min_edge_ratio_eff
                confidence_ok = confidence_score >= self.min_confidence
                should_buy = (
                    best["stable"]
                    and prob_ok
                    and price_ok
                    and edge_abs_ok
                    and edge_ratio_ok
                    and confidence_ok
                    and can_add_position
                    and can_add_condition_total
                    and can_add_slot_direction
                    and can_add_single_condition
                    and can_trade_amount
                    and settle_time_ok
                    and (not standby_mode)
                    and str(best["token_id"]) not in exited_tokens
                )
                best["signal"] = "BUY" if should_buy else "HOLD"
                best["available_usdc"] = round(available_usdc, 4)
                best["token_exposure_usdc"] = round(token_exposure_usdc, 4)
                best["condition_exposure_usdc"] = round(condition_exposure_usdc, 4)
                best["dynamic_buy_usdc"] = round(dynamic_buy_usdc, 4)
                best["kelly_fraction"] = round(kelly_f, 4)
                best["edge_ratio"] = round(edge_ratio_v, 4)
                best["confidence_score"] = round(confidence_score, 4)
                best["disagreement_index"] = round(disagreement_index, 4)
                best["source_weights"] = source_weights_used
                best["edge_threshold_eff"] = round(edge_threshold_eff, 4)
                best["min_edge_ratio_eff"] = round(min_edge_ratio_eff, 4)
                best["model_source"] = self.current_model_source
                best["total_exposure_usdc"] = round(total_exposure_usdc, 4)
                best["can_trade_amount"] = can_trade_amount
                best["projected_position_after_buy"] = round(projected_position, 4)
                best["can_add_position"] = can_add_position
                best["total_condition_position_shares"] = round(total_condition_position, 4)
                best["other_condition_position_shares"] = round(other_condition_position, 4)
                best["opposite_slot_position_shares"] = round(opposite_slot_position, 4)
                best["projected_total_condition_position_shares"] = round(projected_total_condition_position, 4)
                best["can_add_condition_total"] = can_add_condition_total
                best["can_add_slot_direction"] = can_add_slot_direction
                best["can_add_single_condition"] = can_add_single_condition
                best["hours_to_settle"] = (
                    round(hours_to_settle, 2) if isinstance(hours_to_settle, (int, float)) else ""
                )
                if not prob_ok:
                    best["hold_reason"] = "PROB_TOO_LOW"
                elif not price_low_ok:
                    best["hold_reason"] = "PRICE_TOO_LOW"
                elif not price_high_ok:
                    best["hold_reason"] = "PRICE_TOO_HIGH"
                elif not edge_abs_ok:
                    best["hold_reason"] = "EDGE_ABS_TOO_LOW"
                elif not edge_ratio_ok:
                    best["hold_reason"] = "EDGE_RATIO_TOO_LOW"
                elif not confidence_ok:
                    best["hold_reason"] = "CONFIDENCE_TOO_LOW"
                elif not settle_time_ok:
                    best["hold_reason"] = "SETTLE_TOO_NEAR"
                elif standby_mode:
                    best["hold_reason"] = "STANDBY_DAILY_LOSS"
                elif not can_trade_amount:
                    best["hold_reason"] = f"dynamic_buy_usdc<{effective_min_trade}"
                elif not can_add_position:
                    best["hold_reason"] = "token_position_limit"
                elif not can_add_condition_total:
                    best["hold_reason"] = "condition_position_limit"
                elif not can_add_slot_direction:
                    best["hold_reason"] = "slot_direction_conflict"
                elif not can_add_single_condition:
                    best["hold_reason"] = "condition_locked_other_outcome"
                elif str(best["token_id"]) in exited_tokens:
                    best["hold_reason"] = "exited_this_round"
                elif (available_usdc + total_exposure_usdc) > 0 and (
                    total_exposure_usdc / (available_usdc + total_exposure_usdc)
                ) >= self.total_exposure_limit:
                    best["hold_reason"] = "total_exposure_limit"
                actions.append(best)

                LOGGER.info(
                    "[%s %s] %s | fair=%.3f price=%.3f edge=%.3f ratio=%.3f model=%s stable=%s pos=%.2f cond_pos=%.2f amt=$%.2f -> %s",
                    city_name,
                    market.date_label,
                    best["label"],
                    best["fair_prob"],
                    best["market_price"],
                    best["edge"],
                    edge_ratio_v,
                    self.current_model_source,
                    best["stable"],
                    best["current_position_shares"],
                    best["total_condition_position_shares"],
                    best["dynamic_buy_usdc"],
                    best["signal"],
                )

                if should_buy:
                    if self.dry_run:
                        spread = self._check_spread_filter(best["token_id"])
                        if not bool(spread.get("ok", False)):
                            best["signal"] = "HOLD"
                            best["hold_reason"] = "SKIP_WIDE_SPREAD"
                            best["spread_ratio"] = round(float(spread.get("spread_ratio", 0.0)), 6)
                            best["ask_price"] = round(float(spread.get("ask_price", 0.0)), 4)
                            best["bid_price"] = round(float(spread.get("bid_price", 0.0)), 4)
                            LOGGER.warning(
                                "DRY_RUN SKIP_WIDE_SPREAD token=%s ask=%.4f bid=%.4f spread=%.2f%%",
                                best["token_id"],
                                float(spread.get("ask_price", 0.0)),
                                float(spread.get("bid_price", 0.0)),
                                float(spread.get("spread_ratio", 0.0)) * 100.0,
                            )
                        else:
                            LOGGER.info(
                                "DRY_RUN buy skipped: token=%s amount=$%.2f spread=%.2f%%",
                                best["token_id"],
                                dynamic_buy_usdc,
                                float(spread.get("spread_ratio", 0.0)) * 100.0,
                            )
                    else:
                        try:
                            result = self._execute_buy(
                                best["token_id"],
                                dynamic_buy_usdc,
                                fair_price=float(best["fair_prob"]),
                                city=str(best.get("city") or ""),
                            )
                            best["order_result"] = result
                            if str(result.get("order_style", "")) == "SKIP_WIDE_SPREAD":
                                best["signal"] = "HOLD"
                                best["hold_reason"] = "SKIP_WIDE_SPREAD"
                                best["spread_ratio"] = result.get("spread_ratio", "")
                                best["ask_price"] = result.get("ask_price", "")
                                best["bid_price"] = result.get("bid_price", "")
                            elif str(best.get("signal", "")) == "BUY":
                                spent = float(result.get("amount_usdc", dynamic_buy_usdc) or dynamic_buy_usdc)
                                available_usdc = max(0.0, available_usdc - max(0.0, spent))
                            LOGGER.info("Order submitted: %s", result)
                        except Exception as exc:
                            best["signal"] = "HOLD"
                            best["hold_reason"] = f"buy_failed:{exc}"
                            LOGGER.warning("Buy failed and downgraded to HOLD: %s", exc)

            actions.extend(self._build_discovery_debug_rows())
            if self.write_static_report:
                self._write_static_report(
                    actions,
                    write_history=False,
                    progress={
                        "stage": "city_done",
                        "city": city_name,
                        "city_index": city_idx,
                        "total_cities": total_cities,
                    },
                )

        # 对策略覆盖范围之外的持仓做止盈/止损，避免仓位长期“无人管理”
        unmanaged_exit_actions = self.check_and_exit_unmanaged_positions(covered_token_ids, live_positions)
        actions.extend(unmanaged_exit_actions)
        if standby_mode:
            sold_tokens = {str(x.get("token_id") or "") for x in actions if str(x.get("signal") or "") == "REDUCE"}
            standby_actions = self._standby_flatten_live_positions(live_positions, skip_tokens=sold_tokens)
            actions.extend(standby_actions)
        self._save_fair_prob_state()
        self._save_opposite_token_map()
        self._write_actions_to_db(actions)

        self._write_diagnostics(
            {
                "generated_at": datetime.now(self.NYC_TZ).strftime("%Y-%m-%d %H:%M:%S %Z"),
                "generated_at_iso": datetime.now(self.NYC_TZ).isoformat(),
                "mode": "yes_no_dual_side",
                "rows": diagnostics_rows,
                "signal_summary": self._signal_summary(actions),
            }
        )

        if self.write_static_report:
            self._write_static_report(actions)
        return actions

    def run_forever(self, interval_seconds: int = 600, heartbeat_seconds: int = 3600) -> None:
        """常驻循环运行策略。"""
        run_count = 0
        last_heartbeat_ts = time.time()
        while True:
            try:
                self.run_once()
                run_count += 1
                LOGGER.info("Run completed #%d, next run in %ds", run_count, interval_seconds)
            except Exception as exc:
                # 顶层兜底，确保单次异常不会导致进程退出
                LOGGER.exception("Run loop error: %s", exc)
            now_ts = time.time()
            if now_ts - last_heartbeat_ts >= heartbeat_seconds:
                last_heartbeat_ts = now_ts
                LOGGER.info(
                    "HEARTBEAT | alive=true runs=%d interval=%ds model=%s",
                    run_count,
                    interval_seconds,
                    self._forecast_model_name or "pending",
                )
            time.sleep(interval_seconds)


if __name__ == "__main__":
    # 配置日志输出格式
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    # 自动读取 .env，确保 watchdog/双击启动时也能拿到签名参数
    load_env_file(".env")
    if not acquire_single_instance_lock():
        LOGGER.error("Detected another Quantify.py instance. Exit this process.")
        raise SystemExit(0)

    # 从环境变量读取私钥，避免明文写入代码库
    PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
    if not re.fullmatch(r"0x[a-fA-F0-9]{64}", PRIVATE_KEY):
        raise ValueError(
            "未检测到有效私钥。请设置环境变量 POLYMARKET_PRIVATE_KEY=0x...（64位十六进制）"
        )
    SIGNATURE_TYPE = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0").strip() or "0")
    FUNDER = os.getenv("POLYMARKET_FUNDER", "").strip() or None

    LOOP_INTERVAL_SECONDS = int(os.getenv("POLY_LOOP_INTERVAL_SECONDS", "300").strip() or "300")
    HEARTBEAT_SECONDS = int(os.getenv("POLY_HEARTBEAT_SECONDS", "3600").strip() or "3600")
    DRY_RUN = (os.getenv("POLY_DRY_RUN", "false").strip().lower() in ("1", "true", "yes", "y"))
    ENABLE_DAILY_LOSS_STANDBY = (
        os.getenv("POLY_ENABLE_DAILY_LOSS_STANDBY", "false").strip().lower() in ("1", "true", "yes", "y")
    )
    ENABLE_DB_DUAL_WRITE = (
        os.getenv("POLY_ENABLE_DB_DUAL_WRITE", "false").strip().lower() in ("1", "true", "yes", "y")
    )
    DB_HOST = os.getenv("POLY_DB_HOST", "127.0.0.1").strip() or "127.0.0.1"
    DB_PORT = int(os.getenv("POLY_DB_PORT", "3306").strip() or "3306")
    DB_USER = os.getenv("POLY_DB_USER", "root").strip() or "root"
    DB_PASSWORD = os.getenv("POLY_DB_PASSWORD", "root")
    DB_NAME = os.getenv("POLY_DB_NAME", "quantify").strip() or "quantify"
    DB_CONNECT_TIMEOUT_S = int(os.getenv("POLY_DB_CONNECT_TIMEOUT_S", "5").strip() or "5")

    # 创建机器人实例
    bot = PolymarketWeatherMaster(
        private_key=PRIVATE_KEY,
        signature_type=SIGNATURE_TYPE,
        funder=FUNDER,
        investment_usdc=20.0,
        edge_threshold=0.12,
        min_confidence=0.60,
        min_fair_prob=0.20,
        max_trade_usdc=3.0,
        enable_daily_loss_standby=ENABLE_DAILY_LOSS_STANDBY,
        enable_db_dual_write=ENABLE_DB_DUAL_WRITE,
        db_host=DB_HOST,
        db_port=DB_PORT,
        db_user=DB_USER,
        db_password=DB_PASSWORD,
        db_name=DB_NAME,
        db_connect_timeout_s=DB_CONNECT_TIMEOUT_S,
        dry_run=DRY_RUN,
    )
    # 常驻运行（默认每 5 分钟一轮，每小时心跳）
    bot.run_forever(interval_seconds=LOOP_INTERVAL_SECONDS, heartbeat_seconds=HEARTBEAT_SECONDS)
