# 定时交易执行方案（轻量级）

## 一、目标

基于现有回测代码，实现：
- **日频交易**：每个交易日 09:35 自动执行一次
- **小时频交易**：每个交易日 10:35、11:35、14:05、15:05 自动执行

核心思路：**最小改动，复用现有代码**

---

## 二、与回测的关键差异

| 项目 | 回测模式 | 定时执行模式 |
|------|---------|-------------|
| 触发方式 | 手动运行，遍历历史日期 | 定时调度，只执行当前时间点 |
| 日期来源 | `date_range` 配置 | 系统当前时间 |
| 价格数据 | `merged.jsonl` 历史文件 | 实时 API 获取后追加到文件 |
| 执行逻辑 | `run_date_range()` | `run_single_session()` |
| 持仓记录 | 保持不变 | 保持不变 |

---

## 三、实现方案

### 方案概览

```
┌─────────────────────────────────────────────────────────┐
│                    定时交易执行流程                       │
├─────────────────────────────────────────────────────────┤
│                                                         │
│   ┌──────────┐      ┌──────────┐      ┌──────────┐     │
│   │ 定时触发  │ ───→ │ 获取实时  │ ───→ │ 追加到   │     │
│   │ (cron)   │      │ 价格数据  │      │ merged   │     │
│   └──────────┘      └──────────┘      └──────────┘     │
│                                              │          │
│                                              ▼          │
│   ┌──────────┐      ┌──────────┐      ┌──────────┐     │
│   │ 更新持仓  │ ←─── │ AI 决策  │ ←─── │ 执行交易  │     │
│   │ position │      │ (不变)   │      │ 会话     │     │
│   └──────────┘      └──────────┘      └──────────┘     │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### 核心改动点

1. **新增定时调度器** - `scheduler/live_scheduler.py`
2. **新增实时数据获取** - `data/fetch_realtime.py`
3. **新增运行入口** - `run_scheduled.py`
4. **配置文件扩展** - 增加 `mode` 和 `schedule` 字段

---

## 四、详细设计

### 1. 定时调度器

```python
# scheduler/live_scheduler.py

import asyncio
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

class LiveTradingScheduler:
    """
    轻量级定时调度器
    - 日频：09:35 执行
    - 小时频：10:35, 11:35, 14:05, 15:05 执行
    """

    def __init__(self, config_path: str, market: str, frequency: str):
        self.config_path = config_path
        self.market = market
        self.frequency = frequency
        self.scheduler = AsyncIOScheduler()
        self.tz = pytz.timezone('Asia/Shanghai' if market == 'cn' else 'America/New_York')

    def start(self):
        """启动调度器"""
        if self.frequency == "daily":
            self._add_daily_job()
        elif self.frequency == "hourly":
            self._add_hourly_jobs()

        print(f"[Scheduler] 启动定时任务 - 市场: {self.market}, 频率: {self.frequency}")
        self.scheduler.start()

    def _add_daily_job(self):
        """日频任务：每个工作日 09:35"""
        if self.market == "cn":
            self.scheduler.add_job(
                self._run_trading_session,
                CronTrigger(hour=9, minute=35, day_of_week='mon-fri', timezone=self.tz),
                id="daily_trading"
            )
            print("[Scheduler] 已添加日频任务: 每个工作日 09:35 (Asia/Shanghai)")

    def _add_hourly_jobs(self):
        """小时频任务：10:35, 11:35, 14:05, 15:05"""
        if self.market == "cn":
            schedule_times = [(10, 35), (11, 35), (14, 5), (15, 5)]
            for hour, minute in schedule_times:
                self.scheduler.add_job(
                    self._run_trading_session,
                    CronTrigger(hour=hour, minute=minute, day_of_week='mon-fri', timezone=self.tz),
                    id=f"hourly_trading_{hour}_{minute}"
                )
            print(f"[Scheduler] 已添加小时频任务: {schedule_times}")

    async def _run_trading_session(self):
        """执行交易会话"""
        now = datetime.now(self.tz)
        print(f"\n{'='*60}")
        print(f"[{now}] 触发定时交易任务")
        print(f"{'='*60}")

        try:
            # 1. 获取并更新实时价格
            from data.fetch_realtime import update_realtime_prices
            await update_realtime_prices(self.market, self.frequency)

            # 2. 执行交易会话
            await self._execute_trading(now)

        except Exception as e:
            print(f"[ERROR] 交易执行失败: {e}")
            import traceback
            traceback.print_exc()

    async def _execute_trading(self, current_time: datetime):
        """执行单次交易"""
        import json
        from main import run_single_model

        # 读取配置
        with open(self.config_path, 'r') as f:
            config = json.load(f)

        # 设置当前日期/时间
        if self.frequency == "daily":
            today_date = current_time.strftime("%Y-%m-%d")
        else:
            today_date = current_time.strftime("%Y-%m-%d %H:%M:%S")

        # 遍历启用的模型执行
        for model_config in config.get("models", []):
            if model_config.get("enabled", False):
                print(f"\n[Trading] 执行模型: {model_config['name']}, 时间: {today_date}")
                await run_single_model(config, model_config, today_date)

    def run_forever(self):
        """运行调度器（阻塞）"""
        try:
            asyncio.get_event_loop().run_forever()
        except (KeyboardInterrupt, SystemExit):
            print("\n[Scheduler] 停止调度器")
            self.scheduler.shutdown()
```

### 2. 实时数据获取

```python
# data/fetch_realtime.py

import json
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# 可选择的数据源
DATA_SOURCES = {
    "tushare": "_fetch_tushare",
    "akshare": "_fetch_akshare",
    "sina": "_fetch_sina",
}

async def update_realtime_prices(market: str, frequency: str):
    """
    获取实时价格并追加到 merged.jsonl

    流程：
    1. 读取现有 merged.jsonl 获取股票列表
    2. 调用实时 API 获取最新价格
    3. 将新数据追加到 merged.jsonl
    """
    print(f"[Data] 获取实时价格 - 市场: {market}, 频率: {frequency}")

    # 确定数据文件路径
    if market == "cn":
        if frequency == "daily":
            data_file = Path("data/A_stock/merged.jsonl")
        else:
            data_file = Path("data/A_stock/merged_hourly.jsonl")
    else:
        data_file = Path("data/merged.jsonl")

    # 读取股票列表
    symbols = _get_symbols_from_merged(data_file)
    print(f"[Data] 股票列表: {len(symbols)} 只")

    # 获取实时价格
    prices = await _fetch_realtime_prices(symbols, market)
    print(f"[Data] 获取到 {len(prices)} 只股票价格")

    # 追加到 merged.jsonl
    _append_prices_to_merged(data_file, prices, frequency)
    print(f"[Data] 价格数据已追加到 {data_file}")

def _get_symbols_from_merged(data_file: Path) -> List[str]:
    """从 merged.jsonl 读取股票代码列表"""
    symbols = []
    with open(data_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            symbol = data.get("Meta Data", {}).get("2. Symbol", "")
            if symbol:
                symbols.append(symbol)
    return symbols

async def _fetch_realtime_prices(symbols: List[str], market: str) -> Dict:
    """
    获取实时价格

    返回格式：
    {
        "600519.SH": {
            "open": 1800.00,
            "high": 1820.00,
            "low": 1790.00,
            "close": 1810.00,
            "volume": 1234567
        },
        ...
    }
    """
    if market == "cn":
        return await _fetch_astock_realtime(symbols)
    else:
        return await _fetch_us_realtime(symbols)

async def _fetch_astock_realtime(symbols: List[str]) -> Dict:
    """
    获取 A 股实时价格

    方案1: 使用 akshare（免费）
    方案2: 使用 tushare（需要积分）
    方案3: 使用新浪财经 API（免费）
    """
    try:
        import akshare as ak

        prices = {}
        for symbol in symbols:
            try:
                # 转换代码格式: 600519.SH -> sh600519
                code = symbol.split('.')[0]
                market_prefix = 'sh' if symbol.endswith('.SH') else 'sz'
                ak_symbol = f"{market_prefix}{code}"

                # 获取实时行情
                df = ak.stock_zh_a_spot_em()
                row = df[df['代码'] == code]

                if not row.empty:
                    prices[symbol] = {
                        "open": float(row['今开'].values[0]),
                        "high": float(row['最高'].values[0]),
                        "low": float(row['最低'].values[0]),
                        "close": float(row['最新价'].values[0]),
                        "volume": int(row['成交量'].values[0])
                    }
            except Exception as e:
                print(f"[Warning] 获取 {symbol} 价格失败: {e}")
                continue

        return prices

    except ImportError:
        print("[Error] 请安装 akshare: pip install akshare")
        return {}

async def _fetch_us_realtime(symbols: List[str]) -> Dict:
    """获取美股实时价格"""
    # TODO: 实现美股实时数据获取
    # 可使用 yfinance, alpha_vantage 等
    pass

def _append_prices_to_merged(data_file: Path, prices: Dict, frequency: str):
    """
    将新价格追加到 merged.jsonl

    逻辑：
    1. 读取现有文件
    2. 为每只股票追加新的时间点数据
    3. 写回文件
    """
    now = datetime.now()
    if frequency == "daily":
        time_key = now.strftime("%Y-%m-%d")
        time_series_key = "Time Series (Daily)"
    else:
        # 对齐到整点
        hour = now.hour
        if hour < 11:
            minute = 30
        elif hour < 14:
            minute = 30 if hour == 11 else 0
        elif hour < 15:
            minute = 0
        else:
            minute = 0

        time_key = now.strftime(f"%Y-%m-%d {hour:02d}:{minute:02d}:00")
        time_series_key = "Time Series (60min)"

    # 读取并更新
    updated_lines = []
    with open(data_file, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            symbol = data.get("Meta Data", {}).get("2. Symbol", "")

            if symbol in prices:
                price_data = prices[symbol]
                # 添加新的时间点数据
                data[time_series_key][time_key] = {
                    "1. buy price": str(price_data["open"]),
                    "2. high": str(price_data["high"]),
                    "3. low": str(price_data["low"]),
                    "4. sell price": str(price_data["close"]),
                    "5. volume": str(price_data["volume"])
                }
                # 更新最后刷新时间
                data["Meta Data"]["3. Last Refreshed"] = time_key

            updated_lines.append(json.dumps(data, ensure_ascii=False))

    # 写回文件
    with open(data_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(updated_lines))
```

### 3. 运行入口

```python
# run_scheduled.py

"""
定时交易执行入口

使用方式：
    # 日频交易
    python run_scheduled.py --config configs/astock_config.json --frequency daily

    # 小时频交易
    python run_scheduled.py --config configs/astock_hour_config.json --frequency hourly

    # 立即执行一次（测试用）
    python run_scheduled.py --config configs/astock_config.json --frequency daily --run-now
"""

import argparse
import asyncio
import json
from scheduler.live_scheduler import LiveTradingScheduler

def main():
    parser = argparse.ArgumentParser(description='定时交易执行')
    parser.add_argument('--config', '-c', required=True, help='配置文件路径')
    parser.add_argument('--frequency', '-f', choices=['daily', 'hourly'], required=True, help='交易频率')
    parser.add_argument('--run-now', action='store_true', help='立即执行一次（不启动定时器）')
    args = parser.parse_args()

    # 读取配置获取市场信息
    with open(args.config, 'r') as f:
        config = json.load(f)

    market = config.get("market", "cn")

    if args.run_now:
        # 立即执行一次
        print(f"[Mode] 立即执行模式")
        scheduler = LiveTradingScheduler(args.config, market, args.frequency)
        asyncio.run(scheduler._run_trading_session())
    else:
        # 启动定时调度
        print(f"[Mode] 定时调度模式")
        scheduler = LiveTradingScheduler(args.config, market, args.frequency)
        scheduler.start()
        scheduler.run_forever()

if __name__ == "__main__":
    main()
```

### 4. main.py 新增单次执行函数

```python
# main.py 中新增

async def run_single_model(config: dict, model_config: dict, today_date: str):
    """
    运行单个模型的单次交易会话

    与 run_date_range 的区别：
    - run_date_range: 遍历历史日期范围
    - run_single_model: 只执行指定的单个日期/时间点
    """
    agent_type = config.get("agent_type", "BaseAgent")
    agent_config = config.get("agent_config", {})
    log_path = config.get("log_config", {}).get("log_path", "./data/agent_data")

    # 动态导入 Agent 类
    if agent_type == "BaseAgentAStock":
        from agent.base_agent_astock.base_agent_astock import BaseAgentAStock as AgentClass
    elif agent_type == "BaseAgentAStock_Hour":
        from agent.base_agent_astock.base_agent_astock_hour import BaseAgentAStock_Hour as AgentClass
    elif agent_type == "BaseAgent_Hour":
        from agent.base_agent.base_agent_hour import BaseAgent_Hour as AgentClass
    else:
        from agent.base_agent.base_agent import BaseAgent as AgentClass

    # 创建 Agent 实例
    agent = AgentClass(
        name=model_config["name"],
        model=model_config["basemodel"],
        signature=model_config["signature"],
        log_path=log_path,
        **agent_config,
        **({"openai_base_url": model_config["openai_base_url"]} if "openai_base_url" in model_config else {}),
        **({"openai_api_key": model_config["openai_api_key"]} if "openai_api_key" in model_config else {})
    )

    # 初始化
    await agent.initialize()

    # 执行单次交易会话
    await agent.run_trading_session(today_date)

    # 输出结果
    agent.get_position_summary()
```

---

## 五、配置文件示例

```json
{
  "mode": "scheduled",  // backtest | scheduled
  "agent_type": "BaseAgentAStock",
  "market": "cn",

  "schedule": {
    "frequency": "daily",
    "timezone": "Asia/Shanghai",
    "daily_time": "09:35",
    "hourly_times": ["10:35", "11:35", "14:05", "15:05"]
  },

  "data_source": {
    "type": "akshare",
    "cache_minutes": 5
  },

  "models": [
    {
      "name": "gemini-2.5-flash",
      "basemodel": "gemini-2.5-flash",
      "signature": "gemini-2.5-flash",
      "enabled": true
    }
  ],

  "agent_config": {
    "max_steps": 30,
    "max_retries": 3,
    "initial_cash": 100000.0
  },

  "log_config": {
    "log_path": "./data/agent_data_astock"
  }
}
```

---

## 六、新增文件清单

```
AI-Trader/
├── scheduler/
│   ├── __init__.py
│   └── live_scheduler.py      # 定时调度器
│
├── data/
│   └── fetch_realtime.py      # 实时数据获取
│
├── run_scheduled.py           # 定时执行入口
│
└── configs/
    ├── astock_scheduled.json          # A股日频定时配置
    └── astock_hour_scheduled.json     # A股小时频定时配置
```

---

## 七、使用方式

### 1. 安装依赖

```bash
pip install apscheduler akshare pytz
```

### 2. 启动日频定时交易

```bash
# 前台运行
python run_scheduled.py -c configs/astock_config.json -f daily

# 后台运行（推荐）
nohup python run_scheduled.py -c configs/astock_config.json -f daily > logs/daily.log 2>&1 &
```

### 3. 启动小时频定时交易

```bash
python run_scheduled.py -c configs/astock_hour_config.json -f hourly
```

### 4. 立即测试执行

```bash
# 不启动定时器，立即执行一次（用于测试）
python run_scheduled.py -c configs/astock_config.json -f daily --run-now
```

### 5. 使用 systemd 管理（Linux 生产环境）

```ini
# /etc/systemd/system/ai-trader-daily.service

[Unit]
Description=AI Trader Daily Scheduled Trading
After=network.target

[Service]
Type=simple
User=trader
WorkingDirectory=/home/trader/AI-Trader
ExecStart=/home/trader/AI-Trader/.venv/bin/python run_scheduled.py -c configs/astock_config.json -f daily
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable ai-trader-daily
sudo systemctl start ai-trader-daily
sudo systemctl status ai-trader-daily
```

---

## 八、执行流程图

```
09:35 定时触发
     │
     ▼
┌─────────────────────────────────────┐
│ 1. 获取实时价格 (akshare/tushare)   │
│    - 调用 A 股实时行情 API          │
│    - 获取所有持仓股票的最新价格      │
└─────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────┐
│ 2. 追加到 merged.jsonl              │
│    - 格式与历史数据完全一致          │
│    - 保证 AI 读取数据的接口不变      │
└─────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────┐
│ 3. 执行交易会话                      │
│    - 调用 agent.run_trading_session │
│    - AI 决策逻辑完全不变            │
│    - 更新 position.jsonl            │
└─────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────┐
│ 4. 等待下一次触发                    │
│    - 日频：次日 09:35               │
│    - 小时频：下一个整点              │
└─────────────────────────────────────┘
```

---

## 九、与现有代码的兼容性

| 模块 | 改动 | 说明 |
|------|------|------|
| `agent/*.py` | **无改动** | Agent 类完全复用 |
| `tools/*.py` | **无改动** | 工具函数完全复用 |
| `agent_tools/*.py` | **无改动** | MCP 工具完全复用 |
| `prompts/*.py` | **无改动** | 提示词完全复用 |
| `main.py` | **新增函数** | 新增 `run_single_model()` |
| `data/merged.jsonl` | **追加数据** | 实时价格追加到现有文件 |

**核心原则**：通过在数据层面保持兼容（追加到 merged.jsonl），使整个 AI 决策流程无需任何改动。

---

## 十、注意事项

1. **交易日过滤**：需要判断当天是否为交易日
   ```python
   # 简单方案：尝试获取价格，无数据则跳过
   # 完整方案：接入 TuShare 交易日历 API
   ```

2. **价格数据时效**：确保在触发时间时价格数据已可用
   - 日频：09:35 触发（开盘后5分钟）
   - 小时频：整点后5分钟触发

3. **重复执行保护**：检查当前时间点是否已有记录
   ```python
   # 在执行前检查 position.jsonl 最后一条记录的时间
   ```

4. **异常处理**：API 调用失败时的重试机制

5. **日志记录**：记录每次执行的详细日志便于排查问题

---

## 十一、实现状态（已完成）

> 更新日期：2026-01-13

### 已创建的文件

| 文件路径 | 状态 | 说明 |
|---------|------|------|
| `scheduler/__init__.py` | ✅ 已完成 | 模块初始化文件 |
| `scheduler/live_scheduler.py` | ✅ 已完成 | APScheduler 定时调度器实现 |
| `data/fetch_realtime.py` | ✅ 已完成 | akshare 实时数据获取实现 |
| `run_scheduled.py` | ✅ 已完成 | 定时执行入口脚本 |
| `requirements.txt` | ✅ 已更新 | 添加 apscheduler, pytz, akshare 依赖 |

### 实现特点

1. **自动推断配置**：市场类型和交易频率可从配置文件中的 `agent_type` 自动推断
   - `BaseAgentAStock` → `cn` + `daily`
   - `BaseAgentAStock_Hour` → `cn` + `hourly`

2. **数据层兼容**：实时价格追加到现有 `merged.jsonl`，AI 决策逻辑无需改动

3. **重复执行保护**：自动检查当前时间点数据是否已存在

4. **完整的命令行接口**：支持 `--run-now` 立即测试执行

### 快速使用

```bash
# 安装依赖
pip install apscheduler pytz akshare

# 立即测试（不启动定时器）
python run_scheduled.py -c configs/astock_config.json --run-now

# 启动日频定时任务
python run_scheduled.py -c configs/astock_config.json

# 启动小时频定时任务
python run_scheduled.py -c configs/astock_hour_config.json

# 后台运行
nohup python run_scheduled.py -c configs/astock_config.json > logs/scheduled.log 2>&1 &
```

### 调度时间表

| 频率 | 市场 | 触发时间 | 说明 |
|------|------|---------|------|
| 日频 | A股 | 09:35 | 开盘后5分钟 |
| 小时频 | A股 | 10:35, 11:35, 14:05, 15:05 | 每个交易时段结束后5分钟 |
| 日频 | 美股 | 21:35 (北京时间) | 美股开盘后5分钟 |

### 关键代码路径

```
run_scheduled.py
    ↓
scheduler/live_scheduler.py::LiveTradingScheduler
    ↓
├── _run_trading_session()
│   ├── data/fetch_realtime.py::update_realtime_prices()  # 获取实时价格
│   │   ├── fetch_astock_realtime()  # akshare 获取 A 股行情
│   │   └── append_prices_to_merged()  # 追加到 merged.jsonl
│   │
│   └── _run_single_model()  # 执行交易
│       ├── main.py::get_agent_class()  # 获取 Agent 类
│       ├── agent.initialize()  # 初始化 MCP 连接
│       └── agent.run_trading_session()  # 执行交易会话（原有逻辑）
```

### 后续优化建议

1. **交易日历集成**：接入 TuShare 交易日历 API，跳过节假日
2. **多数据源支持**：添加 tushare、新浪财经等备用数据源
3. **告警通知**：添加微信/邮件告警功能
4. **Web 监控界面**：添加简单的状态监控页面
