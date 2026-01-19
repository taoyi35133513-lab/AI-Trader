# 实时投资交易系统设计方案

## 一、概述

将现有的回测系统改造成实时投资交易系统，支持：
- **日频交易**：每个交易日开盘后某个时间运行（如 09:35）
- **小时频交易**：按照交易时间整点运行（如 10:35, 11:35, 14:05, 15:05）

---

## 二、整体架构设计

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          实时交易系统架构                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │   调度服务    │───→│   交易引擎   │───→│   执行服务    │              │
│  │  Scheduler   │    │  TradeEngine │    │  Executor    │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│         │                    │                    │                     │
│         ▼                    ▼                    ▼                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │  数据服务     │    │   AI 决策    │    │   券商接口   │              │
│  │ DataService  │    │  LLM Agent   │    │  BrokerAPI   │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│         │                    │                    │                     │
│         ▼                    ▼                    ▼                     │
│  ┌──────────────────────────────────────────────────────┐              │
│  │                    状态管理 + 持久化层                  │              │
│  │         (Redis/PostgreSQL + position.jsonl)          │              │
│  └──────────────────────────────────────────────────────┘              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 三、核心模块设计

### 1. 调度服务（Scheduler）

**职责**：根据交易时间触发交易任务

```python
# 设计思路：使用 APScheduler 或 Celery Beat

class TradingScheduler:
    """
    日频调度：
    - A股: 每个交易日 09:35 (开盘后5分钟，确保有价格数据)
    - 美股: 每个交易日 09:35 EST (21:35 北京时间)

    小时频调度：
    - A股: 10:35, 11:35, 14:05, 15:05 (每个时段开始后5分钟)
    - 美股: 每小时整点后5分钟
    """

    schedules = {
        "astock_daily": {
            "trigger": "cron",
            "hour": 9, "minute": 35,
            "timezone": "Asia/Shanghai",
            "day_of_week": "mon-fri"  # 需配合交易日历过滤
        },
        "astock_hourly": {
            "trigger": "cron",
            "hour": "10,11,14,15",
            "minute": 35,
            "timezone": "Asia/Shanghai"
        }
    }
```

**交易日历集成**：
```python
# 需要对接交易日历服务，过滤非交易日
class TradingCalendar:
    def is_trading_day(self, date, market) -> bool:
        """检查是否为交易日（排除节假日、周末）"""
        # 方案1: 调用 TuShare 交易日历 API
        # 方案2: 本地维护节假日列表
        # 方案3: 每天早上先查询当天是否有数据
```

---

### 2. 数据服务（DataService）

**职责**：获取实时/准实时市场数据

```python
class RealTimeDataService:
    """
    与回测系统的关键差异：
    - 回测：从 merged.jsonl 读取历史数据
    - 实盘：调用实时 API 获取最新价格
    """

    def get_current_prices(self, symbols: List[str], market: str) -> Dict:
        """获取当前价格"""
        if market == "cn":
            # 方案1: TuShare 实时行情
            # 方案2: 新浪财经 API
            # 方案3: 东方财富 API
            return self._fetch_astock_realtime(symbols)
        elif market == "us":
            return self._fetch_us_realtime(symbols)

    def _fetch_astock_realtime(self, symbols):
        """
        A股实时数据源选择：
        1. TuShare Pro (需要高级权限)
        2. akshare (免费)
        3. 券商接口 (延迟最低)
        """
        pass
```

**数据流改造对比**：

| 环节 | 回测系统 | 实盘系统 |
|------|---------|---------|
| 价格来源 | `merged.jsonl` 历史文件 | 实时 API 调用 |
| 日期来源 | 配置文件 `date_range` | 系统当前时间 |
| 数据延迟 | 无（历史数据） | 需考虑 API 延迟 |
| 数据格式 | 保持不变 | 统一转换为内部格式 |

---

### 3. 交易引擎（TradeEngine）

**职责**：协调数据获取、AI决策、交易执行

```python
class RealTimeTradeEngine:
    """
    核心流程与回测相同，主要改造点：
    1. 数据源切换为实时
    2. 增加实盘风控逻辑
    3. 增加交易确认机制
    """

    async def run_trading_session(self, market: str, frequency: str):
        """运行单次交易会话"""

        # 1. 获取当前时间（替代回测中的遍历日期）
        current_time = self._get_current_trading_time(market, frequency)

        # 2. 检查是否为交易时段
        if not self._is_trading_hours(current_time, market):
            logger.info(f"非交易时段: {current_time}")
            return

        # 3. 获取实时价格（替代从文件读取）
        prices = await self.data_service.get_current_prices(
            self.symbols, market
        )

        # 4. 读取当前持仓（保持不变）
        positions = get_today_init_position(self.signature)

        # 5. AI 决策（核心逻辑保持不变）
        decision = await self._run_ai_decision(
            current_time, prices, positions
        )

        # 6. 风控检查（新增）
        if not self._risk_check(decision, positions, prices):
            logger.warning("风控拦截")
            return

        # 7. 执行交易（对接券商 API）
        if decision.action != "HOLD":
            result = await self.executor.execute(decision)

        # 8. 更新持仓记录
        self._update_position_record(current_time, result)
```

---

### 4. 执行服务（Executor）

**职责**：对接券商/交易所 API 执行实际交易

```python
class TradeExecutor:
    """
    回测 vs 实盘的核心差异点

    回测：直接修改 position.jsonl（模拟交易）
    实盘：调用券商 API → 等待确认 → 更新持仓
    """

    def __init__(self, broker_config: dict):
        self.broker = self._init_broker(broker_config)

    async def execute(self, decision: TradeDecision) -> ExecutionResult:
        """
        执行流程：
        1. 提交订单到券商
        2. 等待订单确认
        3. 查询成交结果
        4. 更新本地持仓
        """

        # 提交订单
        order_id = await self.broker.submit_order(
            symbol=decision.symbol,
            side=decision.side,      # BUY / SELL
            quantity=decision.quantity,
            order_type="LIMIT",       # 限价单
            price=decision.price
        )

        # 等待成交（轮询或回调）
        execution = await self._wait_for_execution(order_id)

        return ExecutionResult(
            order_id=order_id,
            filled_quantity=execution.filled_qty,
            average_price=execution.avg_price,
            status=execution.status
        )
```

**券商接口抽象**：
```python
class BrokerInterface(ABC):
    """统一券商接口"""

    @abstractmethod
    async def submit_order(self, symbol, side, quantity, order_type, price): pass

    @abstractmethod
    async def cancel_order(self, order_id): pass

    @abstractmethod
    async def get_order_status(self, order_id): pass

    @abstractmethod
    async def get_positions(self): pass

    @abstractmethod
    async def get_balance(self): pass


# 具体实现
class EastMoneyBroker(BrokerInterface):
    """东方财富券商接口"""
    pass

class FutuBroker(BrokerInterface):
    """富途证券接口"""
    pass

class SimulatedBroker(BrokerInterface):
    """模拟交易（用于测试）"""
    pass
```

---

## 四、状态管理设计

### 1. 交易状态机

```
┌─────────┐     触发      ┌──────────┐    获取数据     ┌───────────┐
│  IDLE   │─────────────→ │ FETCHING │────────────────→│ ANALYZING │
└─────────┘               └──────────┘                 └───────────┘
     ▲                                                       │
     │                                                       ▼
     │                    ┌──────────┐    执行完成     ┌───────────┐
     └────────────────────│ EXECUTING│←───────────────│ DECIDING  │
                          └──────────┘                └───────────┘
                               │
                               ▼
                          ┌──────────┐
                          │ COMPLETED│
                          └──────────┘
```

### 2. 持仓状态同步

```python
class PositionManager:
    """
    持仓管理器

    设计要点：
    1. 本地 position.jsonl 作为审计日志
    2. 内存/Redis 缓存当前持仓（快速读取）
    3. 定期与券商持仓同步校验
    """

    def __init__(self):
        self.local_positions = {}   # 本地记录
        self.broker_positions = {}  # 券商实际持仓

    async def sync_with_broker(self):
        """与券商同步持仓"""
        broker_pos = await self.broker.get_positions()

        # 对比差异
        diff = self._compare_positions(self.local_positions, broker_pos)

        if diff:
            # 告警 + 以券商为准
            await self._alert_position_mismatch(diff)
            self.local_positions = broker_pos
```

---

## 五、配置文件改造

```json
{
  "mode": "live",  // 新增：live | backtest | paper

  "agent_type": "BaseAgentAStock",
  "market": "cn",
  "frequency": "daily",  // daily | hourly

  "schedule": {  // 新增：调度配置
    "daily": {
      "trigger_time": "09:35",
      "timezone": "Asia/Shanghai"
    },
    "hourly": {
      "trigger_times": ["10:35", "11:35", "14:05", "15:05"],
      "timezone": "Asia/Shanghai"
    }
  },

  "data_source": {  // 新增：数据源配置
    "type": "tushare",  // tushare | akshare | broker
    "api_key": "xxx",
    "use_cache": true,
    "cache_ttl_seconds": 60
  },

  "broker": {  // 新增：券商配置
    "type": "simulated",  // simulated | eastmoney | futu
    "api_key": "xxx",
    "api_secret": "xxx",
    "account_id": "xxx"
  },

  "risk_control": {  // 新增：风控配置
    "max_single_position_ratio": 0.3,   // 单只股票最大仓位
    "max_daily_loss_ratio": 0.05,       // 单日最大亏损
    "min_cash_reserve": 10000,          // 最低现金储备
    "enable_stop_loss": true,
    "stop_loss_ratio": 0.08
  },

  "models": [...],
  "agent_config": {...}
}
```

---

## 六、核心代码改造点

### 1. BaseAgent 基类改造

```python
class BaseAgent:
    """改造要点"""

    def __init__(self, config):
        self.mode = config.get("mode", "backtest")

        # 根据模式选择数据服务
        if self.mode == "live":
            self.data_service = RealTimeDataService(config["data_source"])
            self.executor = TradeExecutor(config["broker"])
        else:
            self.data_service = BacktestDataService()
            self.executor = SimulatedExecutor()

    # 原方法：get_trading_dates() - 返回日期列表
    # 新方法：get_current_trading_time() - 返回当前时间

    def get_current_trading_time(self) -> str:
        """获取当前交易时间点"""
        if self.mode == "backtest":
            return self._get_next_backtest_date()
        else:
            return datetime.now(self.timezone).strftime(self.time_format)

    # 原方法：run_date_range(init, end) - 遍历历史日期
    # 新方法：run_single_session() - 运行单次会话

    async def run_single_session(self):
        """实盘模式：只运行当前时间点"""
        current_time = self.get_current_trading_time()
        await self.run_trading_session(current_time)
```

### 2. 价格获取改造

```python
# tools/price_tools.py 改造

def get_current_prices(symbols: List[str], market: str, mode: str) -> Dict:
    """
    统一价格获取接口
    """
    if mode == "backtest":
        # 原逻辑：从 merged.jsonl 读取
        return get_open_prices_from_file(symbols, today_date, market)
    else:
        # 新逻辑：调用实时 API
        return fetch_realtime_prices(symbols, market)
```

### 3. 交易工具改造

```python
# agent_tools/tool_trade.py 改造

async def buy(symbol: str, amount: int, mode: str = "backtest") -> Dict:
    """
    买入股票
    """
    if mode == "backtest":
        # 原逻辑：直接更新 position.jsonl
        return _simulated_buy(symbol, amount)
    else:
        # 新逻辑：提交真实订单
        return await _live_buy(symbol, amount)

async def _live_buy(symbol: str, amount: int) -> Dict:
    """实盘买入"""
    # 1. 获取当前最新价
    price = await data_service.get_quote(symbol)

    # 2. 风控检查
    risk_check(symbol, amount, price)

    # 3. 提交订单到券商
    order = await broker.submit_order(
        symbol=symbol,
        side="BUY",
        quantity=amount,
        order_type="LIMIT",
        price=price * 1.01  # 略高于市价确保成交
    )

    # 4. 等待成交
    result = await broker.wait_for_fill(order.id)

    # 5. 更新本地持仓记录
    update_position_record(symbol, amount, result.avg_price)

    return {
        "status": "success",
        "filled_qty": result.filled_qty,
        "avg_price": result.avg_price
    }
```

---

## 七、调度服务实现方案

```python
# scheduler/trading_scheduler.py

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

class TradingScheduler:
    def __init__(self, config):
        self.scheduler = AsyncIOScheduler()
        self.config = config
        self.trade_engine = RealTimeTradeEngine(config)

    def start(self):
        """启动调度器"""
        market = self.config["market"]
        frequency = self.config["frequency"]

        if frequency == "daily":
            self._add_daily_job(market)
        elif frequency == "hourly":
            self._add_hourly_jobs(market)

        self.scheduler.start()

    def _add_daily_job(self, market):
        """添加日频任务"""
        if market == "cn":
            # A股：每个交易日 09:35
            self.scheduler.add_job(
                self._run_trading_with_calendar_check,
                CronTrigger(
                    hour=9, minute=35,
                    day_of_week='mon-fri',
                    timezone='Asia/Shanghai'
                ),
                args=[market, "daily"]
            )
        elif market == "us":
            # 美股：21:35 北京时间（09:35 EST）
            self.scheduler.add_job(
                self._run_trading_with_calendar_check,
                CronTrigger(
                    hour=21, minute=35,
                    day_of_week='mon-fri',
                    timezone='Asia/Shanghai'
                ),
                args=[market, "daily"]
            )

    def _add_hourly_jobs(self, market):
        """添加小时频任务"""
        if market == "cn":
            # A股：10:35, 11:35, 14:05, 15:05
            for hour, minute in [(10, 35), (11, 35), (14, 5), (15, 5)]:
                self.scheduler.add_job(
                    self._run_trading_with_calendar_check,
                    CronTrigger(
                        hour=hour, minute=minute,
                        day_of_week='mon-fri',
                        timezone='Asia/Shanghai'
                    ),
                    args=[market, "hourly"]
                )

    async def _run_trading_with_calendar_check(self, market, frequency):
        """带交易日历检查的交易执行"""
        today = datetime.now().strftime("%Y-%m-%d")

        # 检查是否为交易日
        if not self.trading_calendar.is_trading_day(today, market):
            logger.info(f"{today} 非交易日，跳过")
            return

        # 执行交易
        try:
            await self.trade_engine.run_single_session()
        except Exception as e:
            logger.error(f"交易执行失败: {e}")
            await self._send_alert(f"交易执行失败: {e}")
```

---

## 八、监控与告警设计

```python
class TradingMonitor:
    """交易监控服务"""

    def __init__(self):
        self.alert_channels = []  # 微信/邮件/钉钉

    async def check_heartbeat(self):
        """检查系统心跳"""
        pass

    async def check_position_sync(self):
        """检查持仓同步"""
        local = get_local_positions()
        broker = await get_broker_positions()

        if local != broker:
            await self.send_alert("持仓不同步", diff)

    async def check_daily_pnl(self):
        """检查每日盈亏"""
        pnl = calculate_daily_pnl()

        if pnl < -self.config["max_daily_loss"]:
            await self.send_alert("触发单日最大亏损", pnl)

    async def send_alert(self, title, content):
        """发送告警"""
        for channel in self.alert_channels:
            await channel.send(title, content)
```

---

## 九、部署架构建议

```
┌────────────────────────────────────────────────────────────┐
│                     生产环境部署架构                         │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐       │
│  │  调度服务   │    │  交易服务   │    │  监控服务   │       │
│  │ (Celery)   │    │ (FastAPI)  │    │(Prometheus)│       │
│  └────────────┘    └────────────┘    └────────────┘       │
│        │                 │                 │               │
│        └────────────────┼────────────────┘               │
│                         ▼                                  │
│  ┌──────────────────────────────────────────────┐         │
│  │              消息队列 (Redis/RabbitMQ)         │         │
│  └──────────────────────────────────────────────┘         │
│                         │                                  │
│        ┌───────────────┼───────────────┐                 │
│        ▼               ▼               ▼                  │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐            │
│  │PostgreSQL│    │  Redis   │    │   日志    │            │
│  │ (持仓)   │    │ (缓存)   │    │(Loki/ES) │            │
│  └──────────┘    └──────────┘    └──────────┘            │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

---

## 十、改造步骤建议

| 阶段 | 任务 | 说明 |
|------|------|------|
| **1** | 数据服务抽象 | 定义统一数据接口，实现回测/实时两种实现 |
| **2** | 执行服务抽象 | 定义统一执行接口，先实现模拟券商 |
| **3** | 调度服务开发 | 使用 APScheduler 实现定时触发 |
| **4** | 配置文件扩展 | 添加 mode、schedule、broker 等配置 |
| **5** | 风控模块开发 | 仓位控制、止损止盈 |
| **6** | 模拟盘测试 | 用模拟券商跑完整流程 |
| **7** | 对接真实券商 | 开发具体券商接口 |
| **8** | 监控告警 | 异常检测、持仓同步检查 |

---

## 十一、关键注意事项

1. **T+1 规则**：A股当天买入不能当天卖出，需要在持仓记录中标记买入日期

2. **涨跌停处理**：实盘中可能遇到涨跌停无法成交，需要处理订单超时

3. **网络异常**：增加重试机制和断线重连

4. **资金同步**：每次交易前应从券商获取最新可用资金

5. **日志审计**：所有交易决策和执行结果需要完整记录

6. **灰度上线**：建议先用小资金测试，验证稳定后再逐步增加

---

## 十二、与现有系统的兼容性

本设计保持了原有系统的核心架构（Agent + MCP Tools），主要改造点集中在：

| 模块 | 改造程度 | 说明 |
|------|---------|------|
| AI 决策逻辑 | 无改动 | 提示词和决策流程保持不变 |
| MCP 工具链 | 轻微改动 | 增加 mode 参数区分回测/实盘 |
| 数据获取 | 新增抽象层 | 统一接口，支持文件/API两种数据源 |
| 交易执行 | 新增抽象层 | 统一接口，支持模拟/真实券商 |
| 调度系统 | 全新开发 | 使用 APScheduler 实现 |
| 监控告警 | 全新开发 | 独立模块 |

---

## 十三、新增目录结构

```
AI-Trader/
├── scheduler/                      # 新增：调度服务
│   ├── __init__.py
│   ├── trading_scheduler.py       # 调度器实现
│   └── trading_calendar.py        # 交易日历
│
├── services/                       # 新增：服务抽象层
│   ├── __init__.py
│   ├── data_service.py            # 数据服务接口
│   ├── realtime_data_service.py   # 实时数据实现
│   ├── backtest_data_service.py   # 回测数据实现
│   ├── executor_service.py        # 执行服务接口
│   └── broker/                    # 券商实现
│       ├── __init__.py
│       ├── broker_interface.py    # 券商接口
│       ├── simulated_broker.py    # 模拟券商
│       ├── eastmoney_broker.py    # 东方财富
│       └── futu_broker.py         # 富途证券
│
├── monitor/                        # 新增：监控服务
│   ├── __init__.py
│   ├── trading_monitor.py         # 监控主逻辑
│   └── alert_channels/            # 告警渠道
│       ├── __init__.py
│       ├── wechat.py
│       ├── email.py
│       └── dingtalk.py
│
├── risk/                           # 新增：风控模块
│   ├── __init__.py
│   └── risk_controller.py         # 风控逻辑
│
└── run_live.py                     # 新增：实盘启动入口
```
