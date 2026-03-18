# AI-Trader 项目文档

> 自主 AI 交易代理系统 — 多模型竞赛式模拟交易平台

---

## 目录

1. [项目概述](#1-项目概述)
2. [技术架构设计](#2-技术架构设计)
3. [业务架构设计](#3-业务架构设计)
4. [业务功能模块](#4-业务功能模块)
5. [核心模块实现细节](#5-核心模块实现细节)
6. [数据模型](#6-数据模型)
7. [部署与运维](#7-部署与运维)

---

## 1. 项目概述

### 1.1 定位

AI-Trader 是一个自主 AI 交易代理系统，让多个大语言模型（DeepSeek、Gemini、GPT 等）在上证 50 成分股上进行竞赛式模拟交易。系统采用 MCP（Model Context Protocol）协议连接 AI 模型与交易工具，实现零人工干预的端到端自主交易。

### 1.2 核心特性

- **多模型竞赛**：多个 LLM 使用相同初始资金和规则，独立决策、互相比较
- **双频交易**：支持日频（每日 1 次）和小时频（每日 4 次）两种交易频率
- **双模式运行**：回测模式（历史数据遍历）+ 模拟交易模式（实时调度执行）
- **技能系统**：可组合的交易策略、分析工具、风控规则，按模型独立配置
- **记忆系统**：三层记忆架构（复盘→经验→策略），跨交易会话累积经验
- **投资大师点评**：巴菲特、林奇、索罗斯、利弗莫尔四种风格的 AI 点评
- **Web 仪表盘**：实时净值曲线、排行榜、交易记录、Agent 推理过程展示

### 1.3 技术栈

| 层次 | 技术 |
|------|------|
| AI 框架 | LangChain (ReAct Agent) + MCP (FastMCP) |
| 后端 | FastAPI + Uvicorn |
| 数据存储 | DuckDB（主）+ JSONL（备） |
| 数据源 | Tushare + AKShare（行情/新闻） |
| 调度 | APScheduler（模拟交易定时触发） |
| 前端 | 原生 HTML/JS/CSS + Chart.js |
| 配置验证 | Pydantic V2 |

---

## 2. 技术架构设计

### 2.1 系统架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                        Web Dashboard (port 8080)                 │
│    index.html (净值对比)    portfolio.html (持仓分析)              │
│    [Chart.js]  [Skills Panel]  [Memory Panel]  [Master Panel]    │
└──────────────────────────┬──────────────────────────────────────┘
                           │ HTTP API
┌──────────────────────────▼──────────────────────────────────────┐
│                   FastAPI Unified Backend (port 8888)            │
│                                                                  │
│  ┌─────────────────────────┐  ┌─────────────────────────────┐   │
│  │     REST API (13 路由)    │  │   MCP Services (挂载)        │   │
│  │  /api/dashboard          │  │  /mcp/math/mcp              │   │
│  │  /api/agents             │  │  /mcp/trade/mcp             │   │
│  │  /api/prices             │  │  /mcp/search/mcp            │   │
│  │  /api/config             │  │  /mcp/price/mcp             │   │
│  │  /api/live-trading       │  │  /mcp/skill_ta/mcp (技能)   │   │
│  │  /api/skills             │  │  /mcp/skill_flow/mcp (技能) │   │
│  │  /api/memory             │  └─────────────────────────────┘   │
│  │  /api/master-commentary  │                                    │
│  └─────────────────────────┘                                    │
│                                                                  │
│  ┌─────────────────────────┐  ┌─────────────────────────────┐   │
│  │   APScheduler 调度器     │  │      DuckDB 数据库           │   │
│  │  日频: 09:35 (周一至五)   │  │  positions / prices         │   │
│  │  时频: 10:35/11:35/      │  │  agent_memory / agent_skills│   │
│  │        14:05/15:05       │  │  trade_comments / sessions  │   │
│  └─────────────────────────┘  └─────────────────────────────┘   │
└──────────────────────────┬──────────────────────────────────────┘
                           │ MCP over HTTP
┌──────────────────────────▼──────────────────────────────────────┐
│                      Trading Agent (进程内)                      │
│                                                                  │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐  │
│  │ BaseAgent    │  │ System Prompt│  │  LLM (DeepSeek/Gemini) │  │
│  │ AStock       │  │ 组装器       │  │  via OpenAI-compatible  │  │
│  │ (日频/时频)   │  │ (identity +  │  │  API                   │  │
│  │              │  │  skills +    │  └────────────────────────┘  │
│  │ initialize() │  │  portfolio + │                              │
│  │ run_session()│  │  memory)     │                              │
│  └─────────────┘  └──────────────┘                              │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 请求流转

```
用户操作 (浏览器)
    │
    ├─→ 页面数据: GET /api/dashboard/cn → AgentService → DuckDB/JSONL → 净值计算
    │
    ├─→ 大师点评: POST /api/master-commentary/stream → LLM SSE 流式输出
    │
    ├─→ 技能管理: PUT /api/skills/agent/{name} → DuckDB agent_skills 表
    │
    └─→ 记忆查看: GET /api/memory/active → DuckDB agent_memory 表

调度器触发 (APScheduler cron)
    │
    ├─→ Step 1: 更新价格 (ts.get_realtime_quotes → merged.jsonl)
    ├─→ Step 2: 确定交易时间 (日频日期 / 小时频时点)
    ├─→ Step 3: 逐模型执行
    │       ├─→ 加载技能 (DuckDB agent_skills)
    │       ├─→ 创建 Agent (BaseAgentAStock + MCP 工具 + LLM)
    │       ├─→ 构建 System Prompt (identity + skills + portfolio + memory)
    │       ├─→ ReAct 循环 (LLM ↔ MCP Tools, 最多30步)
    │       ├─→ 写入持仓 (DuckDB + JSONL 双写)
    │       └─→ 生成 L1 Reflection (异步)
    └─→ Step 4: 同步价格到 DuckDB + 更新 SSE50 指数
```

### 2.3 技术设计原则

| 原则 | 实现 |
|------|------|
| **双写容错** | 持仓数据同时写入 DuckDB 和 JSONL，读取时 DuckDB 优先、JSONL 降级 |
| **数据源分层** | 实时行情: `get_realtime_quotes` → `rt_min` → `daily`，逐级降级 |
| **无状态 Agent** | 每个交易日重建 Agent 和 System Prompt，不依赖跨日会话 |
| **配置驱动** | 模型/频率/技能均通过 config.json + DuckDB 管理，无需改代码 |
| **Backtest/Live 隔离** | 签名后缀区分（`-live`），数据目录独立，Dashboard 自动合并 |

---

## 3. 业务架构设计

### 3.1 业务全景

```
┌─────────────────────────────────────────────────────────────┐
│                    AI-Trader 业务全景                         │
├──────────┬──────────┬──────────┬──────────┬────────────────┤
│  数据层   │  策略层   │  执行层   │  分析层   │   管理层      │
│          │          │          │          │               │
│ 行情获取  │ Skills   │ 交易执行  │ 净值计算  │ 模型配置      │
│ 新闻检索  │ 策略引导  │ 持仓管理  │ 排行榜   │ 技能分配      │
│ 指数同步  │ 风控规则  │ T+1校验  │ 交易回放  │ 调度管理      │
│ 价格缓存  │ 记忆注入  │ 订单日志  │ 大师点评  │ 数据同步      │
└──────────┴──────────┴──────────┴──────────┴────────────────┘
```

### 3.2 交易生命周期

```
准备阶段                执行阶段                收尾阶段
┌──────────┐    ┌───────────────────┐    ┌──────────────┐
│ 获取实时价格│ → │ 构建 System Prompt  │ → │ 写入持仓记录  │
│ 加载 Skills│    │  ├ 身份定义         │    │ 生成 Reflection│
│ 读取记忆   │    │  ├ 技能策略指引      │    │ 触发 Consolidation│
│ 读取持仓   │    │  ├ 市场规则         │    │ 同步价格到DB  │
│ 计算收益   │    │  ├ 当日行情+持仓     │    │ 更新指数     │
└──────────┘    │  └ 历史经验记忆      │    └──────────────┘
                │                     │
                │ ReAct Agent 循环     │
                │  ├ 查询价格 (MCP)    │
                │  ├ 搜索新闻 (MCP)    │
                │  ├ 计算指标 (MCP)    │
                │  ├ 决策推理          │
                │  ├ 执行买卖 (MCP)    │
                │  └ 发出 FINISH 信号  │
                └───────────────────┘
```

### 3.3 多模型竞赛机制

- **统一起点**：所有模型使用相同初始资金（10万元）和股票池（上证50成分股）
- **独立决策**：每个模型有独立的持仓、交易记录、日志和记忆
- **公平比较**：Dashboard 实时展示净值曲线、收益率排名、基准（上证50指数）对比
- **差异化能力**：通过 Skills 系统为不同模型配备不同策略和工具

---

## 4. 业务功能模块

### 4.1 模拟交易模块

#### 4.1.1 回测模式

- **入口**：`python start.py` 或 `python main.py`
- **流程**：遍历历史日期范围，逐日执行交易会话
- **日期范围**：自动计算（从最后持仓日到最新价格日）
- **数据路径**：`data/agent_data_astock/{model_name}/position/position.jsonl`

#### 4.1.2 模拟交易模式

- **入口**：`python start.py --live` 或 `./start_all.sh`
- **调度**：APScheduler cron 触发，日频 09:35 / 小时频 10:35/11:35/14:05/15:05
- **价格更新**：交易前自动获取实时价格写入 JSONL
- **数据路径**：`data/agent_data_astock/{model_name}-live/position/position.jsonl`

#### 4.1.3 A股交易规则

| 规则 | 值 |
|------|-----|
| 最小交易单位 | 100 股（1 手） |
| 结算制度 | T+1（当日买入次日才可卖出） |
| 涨跌停限制 | ±10%（科创板 ±20%） |
| 交易时段 | 09:30-11:30, 13:00-15:00 |
| 初始资金 | 100,000 元 (CNY) |
| 股票池 | 上证 50 成分股（54 只） |

### 4.2 Skills 技能系统

#### 4.2.1 架构

```
skills/
├── __init__.py          # 自动发现注册表 (SKILL_REGISTRY)
├── builtin/             # 内置核心技能（不可取消）
│   ├── trade_execution  # 交易执行 MCP
│   ├── price_query      # 价格查询 MCP
│   ├── market_news      # 市场资讯 MCP
│   └── math_calc        # 数学计算 MCP
├── strategies/          # 交易策略（Prompt 注入）
│   ├── value_investing  # 价值投资
│   ├── trend_following  # 趋势跟踪
│   ├── mean_reversion   # 均值回归
│   └── momentum         # 动量策略
├── analysis/            # 分析工具（Prompt + MCP）
│   ├── technical_indicators  # MA/MACD/RSI 计算 [MCP]
│   ├── kline_patterns        # K 线形态识别
│   ├── capital_flow          # 资金流向分析 [MCP]
│   └── sector_rotation       # 行业轮动
├── risk/                # 风控管理（Prompt 注入）
│   ├── stop_loss_take_profit # 止损止盈
│   ├── position_sizing       # 仓位管理
│   ├── risk_budgeting        # 风险预算
│   └── correlation_analysis  # 相关性分析
└── tools/               # 技能专用 MCP 工具
    ├── tool_technical_indicators.py  # FastMCP: calculate_ma/macd/rsi
    └── tool_capital_flow.py          # FastMCP: get_capital_flow
```

#### 4.2.2 技能生效机制

1. **注册**：启动时自动扫描 `skills/` 子目录，发现 `SKILL_CONFIG` + `PROMPT` 的模块
2. **分配**：前端 Skills 面板 toggle → `PUT /api/skills/agent/{name}` → DuckDB `agent_skills` 表
3. **加载**：调度器执行前查询 `agent_skills` 表，将 `skill_ids` 传入 Agent
4. **注入**：`build_system_prompt()` 在 `tool_guide` 和 `market_rules` 之间插入技能 Prompt
5. **工具**：有 `tools_module` 的技能自动挂载 MCP 服务，Agent 动态连接

### 4.3 记忆系统

#### 4.3.1 三层架构

```
L3 Strategy (策略备忘)     ← 由 L2 压缩生成，Agent 的核心交易哲学
    ↑ consolidate_l2_to_l3()
L2 Lesson (交易经验)       ← 由 L1 压缩生成，可复用的交易规则
    ↑ consolidate_l1_to_l2()   (阈值: 5 条 active L1)
L1 Reflection (交易复盘)   ← 每次交易后自动生成，短期记忆
```

#### 4.3.2 生命周期

- **L1 生成**：`_generate_reflection()` 在每次交易会话结束后调用，使用 Agent 自身的 LLM 生成
- **L1→L2 压缩**：当 active L1 数量 ≥ 5 时，LLM 从中提炼 1-3 条可复用经验
- **L2→L3 更新**：当 active L2 数量 ≥ 1 时，LLM 整合经验更新核心交易哲学
- **注入 Prompt**：下次交易时，`get_active_memories()` 返回 L3 + 最近 4 条 L2 + 最近 2 条 L1

### 4.4 投资大师点评

#### 4.4.1 大师列表

| ID | 大师 | 风格 |
|----|------|------|
| buffett | 沃伦·巴菲特 | 价值投资、内在价值、安全边际 |
| lynch | 彼得·林奇 | 成长投资、PEG、十倍股 |
| soros | 乔治·索罗斯 | 反身性理论、宏观对冲 |
| livermore | 杰西·利弗莫尔 | 趋势投机、关键点位、资金管理 |

#### 4.4.2 工作流程

1. 前端选择大师 + 模型 + Agent → POST `/api/master-commentary/stream`
2. 后端 `gather_context()` 收集 Agent 最近持仓、交易历史、资产曲线
3. 用大师的 Persona Prompt 作为 System Prompt，交易上下文作为 User Prompt
4. SSE 流式返回 LLM 生成的点评内容

### 4.5 Web 仪表盘

#### 4.5.1 主页面 (index.html)

- **净值曲线图**：Chart.js 折线图，展示各模型 + 上证50基准的总资产变化
- **排行榜**：按收益率排名，显示模型图标、净值、涨跌额
- **交易记录**：分模型展示最近交易操作和 Agent 推理过程

#### 4.5.2 持仓分析 (portfolio.html)

- 个股持仓明细、买卖价格、盈亏计算
- 行业分布可视化

#### 4.5.3 浮动面板

| 面板 | 入口 | 功能 |
|------|------|------|
| Skills 面板 | ⚡ FAB | 按类别展示 16 个技能，toggle 开关即时生效 |
| Memory 面板 | 🧠 FAB | 展示 L1/L2/L3 记忆，手动触发 consolidation |
| Master 面板 | 💬 FAB | 选择大师/模型/Agent，流式生成点评 |

---

## 5. 核心模块实现细节

### 5.1 交易 Agent (`agent/base_agent_astock/base_agent_astock.py`)

#### 5.1.1 初始化流程

```python
agent = BaseAgentAStock(
    signature="deepseek-chat-v3.2-live",
    basemodel="deepseek-chat",
    skill_ids=["value_investing", "technical_indicators"],
    ...
)
await agent.initialize()
# 1. MultiServerMCPClient 连接 4 核心 + N 技能 MCP 服务
# 2. create_llm() 创建 LLM 实例（自动检测 DeepSeek 用专用 wrapper）
# 3. 加载 MCP 工具列表
```

#### 5.1.2 交易会话循环

```python
await agent.run_trading_session("2026-03-18")
# 1. 构建 System Prompt = identity + tool_guide + [skills] + market_rules + portfolio + memory
# 2. 初始消息: "请分析并更新今日（2026-03-18）的持仓。"
# 3. ReAct 循环 (最多30步):
#    a. write_config_value("SIGNATURE", ...) 并发安全
#    b. agent.ainvoke(messages) → LLM 推理 + 工具调用
#    c. 检查 <FINISH_SIGNAL> → break
#    d. 提取工具调用结果 → 追加到消息历史
# 4. _handle_trading_result() → 写入 no_trade 或确认交易
# 5. _generate_reflection() → L1 记忆生成 + consolidation 检查
```

#### 5.1.3 MCP 工具调用（以 buy 为例）

```python
# agent_tools/tool_trade.py
@mcp.tool()
def buy(symbol: str, amount: int) -> str:
    # 1. 读取 SIGNATURE、TODAY_DATE
    # 2. 验证 lot_size (必须为100的倍数)
    # 3. 获取最新持仓 (DuckDB → JSONL fallback)
    # 4. 查询当日开盘价
    # 5. 检查现金是否充足
    # 6. 更新持仓字典
    # 7. 双写: position.jsonl + DuckDB positions 表
    # 8. 设置 IF_TRADE = True
    # 9. 返回新持仓 JSON
```

### 5.2 System Prompt 组装 (`prompts/agent_prompt_astock.py`)

```python
def build_system_prompt(market, skill_ids, **kwargs):
    # 组装顺序：
    # 1. IDENTITY_CN      — "你是一位A股基本面分析交易助手..."
    # 2. TOOL_GUIDE_CN    — "必须实际调用 buy()/sell() 工具..."
    # 3. [SKILLS PROMPTS]  — 按 skill_ids 注入策略/分析/风控指引
    # 4. MARKET_RULES      — "A股交易规则：T+1, 100股整数倍..."
    # 5. PORTFOLIO         — 当日持仓、价格、昨日收益
    # 6. USER_COMMENTS     — 人工注入的交易批注
    # 7. FINISH_TEMPLATE   — "分析完成后输出 <FINISH_SIGNAL>"

def get_agent_system_prompt_astock(today_date, signature, skill_ids):
    # 1. 获取昨日开盘/收盘价
    # 2. 获取今日买入价
    # 3. 获取当前持仓
    # 4. 计算昨日收益
    # 5. 加载用户评论
    # 6. 调用 build_system_prompt()
    # 7. 注入 Memory (L3 策略 + L2 经验 + L1 复盘)
```

### 5.3 数据源切换 (`data/fetch_realtime.py`)

```python
async def fetch_astock_realtime(symbols):
    # 三级降级策略：
    # 方案1: ts.get_realtime_quotes()  — 无配额限制，盘中实时
    #   ↓ 失败
    # 方案2: pro.rt_min(freq='60MIN')  — 每小时10次限制
    #   ↓ 失败
    # 方案3: pro.daily(trade_date=today) — 仅收盘后有数据
```

### 5.4 价格同步 (`data/sync_prices_db.py`)

| 函数 | 数据源 | 目标 |
|------|--------|------|
| `sync_daily_prices()` | Tushare `daily()` + `rt_min` fallback | DuckDB `stock_daily_prices` |
| `sync_hourly_prices()` | Tushare `rt_min()` + JSONL fallback | DuckDB `stock_hourly_prices` |
| `update_sse50_index()` | Tushare `index_daily()` | `index_daily_sse_50.json` |
| `update_sse50_hourly_index()` | `ts.get_realtime_quotes('sh000016')` | `index_hourly_sse_50.json` |

### 5.5 LLM 工厂 (`tools/llm_factory.py`)

```python
def create_llm(model_name, base_url, api_key, llm_class=None):
    # 三级匹配策略：
    # 1. 显式指定 llm_class → 直接查 LLM_REGISTRY
    # 2. 模型名子串匹配 → "deepseek" → DeepSeekChatOpenAI
    # 3. 默认 → ChatOpenAI

# DeepSeekChatOpenAI 解决 tool_call args 序列化问题
# (DeepSeek 返回 JSON 字符串而非 dict，需要额外反序列化)
```

### 5.6 调度器 (`api/services/scheduler_service.py`)

```python
class SchedulerService:
    # A股交易时间表
    ASTOCK_DAILY_TIME = (9, 35)           # 开盘后5分钟
    ASTOCK_HOURLY_TIMES = [
        (10, 35), (11, 35), (14, 5), (15, 5)  # 每根K线后5分钟
    ]

    async def _run_live_trading_session(frequency_override):
        # 1. 更新价格数据 (fetch_realtime)
        # 2. 确定交易时间 (日频: "2026-03-18" / 小时频: "2026-03-18 10:30:00")
        # 3. 遍历 enabled 模型:
        #    a. 加载 skills (DuckDB → config.json fallback)
        #    b. 动态导入 Agent 类
        #    c. 创建实例 + initialize + run_trading_session
        # 4. 同步价格到 DuckDB + 更新 SSE50 指数
```

---

## 6. 数据模型

### 6.1 DuckDB 表结构

#### 价格数据

```sql
-- 日频价格
CREATE TABLE stock_daily_prices (
    ts_code VARCHAR, trade_date VARCHAR,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    volume BIGINT, market VARCHAR DEFAULT 'cn'
);

-- 小时频价格
CREATE TABLE stock_hourly_prices (
    ts_code VARCHAR, trade_time VARCHAR,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    volume BIGINT, market VARCHAR DEFAULT 'cn'
);
```

#### 持仓记录

```sql
CREATE TABLE positions (
    id INTEGER, agent_name VARCHAR, market VARCHAR,
    trade_date VARCHAR, step_id INTEGER,
    action VARCHAR, symbol VARCHAR, amount INTEGER,
    positions JSON,  -- {"600519.SH": 100, "CASH": 50000}
    created_at TIMESTAMP
);
```

#### 记忆系统

```sql
CREATE TABLE agent_memory (
    id INTEGER PRIMARY KEY,
    agent_name VARCHAR, market VARCHAR,
    level VARCHAR,       -- 'reflection' | 'lesson' | 'strategy'
    content TEXT,
    source_dates VARCHAR, tags VARCHAR,
    status VARCHAR DEFAULT 'active',  -- 'active' | 'archived'
    created_at TIMESTAMP, expires_at TIMESTAMP,
    source_session_id INTEGER, parent_ids VARCHAR
);
```

#### 技能分配

```sql
CREATE TABLE agent_skills (
    agent_name VARCHAR, market VARCHAR, skill_id VARCHAR,
    enabled BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP,
    PRIMARY KEY (agent_name, market, skill_id)
);
```

### 6.2 JSONL 文件格式

#### 持仓记录 (position.jsonl)

```json
{
  "date": "2026-03-18",
  "id": 5,
  "this_action": {"action": "buy", "symbol": "600519.SH", "amount": 100},
  "positions": {"600519.SH": 100, "601318.SH": 200, "CASH": 85000.0}
}
```

#### 价格数据 (merged.jsonl)

```json
{
  "Meta Data": {"2. Symbol": "600519.SH"},
  "Time Series (Daily)": {
    "2026-03-18": {
      "1. buy price": "1468.80",
      "2. high": "1498.07",
      "3. low": "1461.19",
      "4. sell price": "1484.26",
      "5. volume": "2777384"
    }
  }
}
```

---

## 7. 部署与运维

### 7.1 一键启动

```bash
# 安装依赖
poetry install

# 一键启动（后端 + 前端 + 模拟交易调度器）
./start_all.sh          # 生产模式
./start_all.sh --debug  # 调试模式（热重载）
```

### 7.2 环境变量

```bash
# .env 文件
DEEPSEEK_API_KEY=sk-xxx
DEEPSEEK_API_BASE=https://api.deepseek.com
OPENAI_API_KEY=xxx
OPENAI_API_BASE=https://generativelanguage.googleapis.com/v1beta/openai/
TUSHARE_TOKEN=xxx
UNIFIED_MCP_MODE=true
AI_TRADER_MODE=live
AI_TRADER_FREQUENCY=daily+hourly
```

### 7.3 数据补录

```bash
# 补录指定日期的模拟交易
python scripts/replay_trading.py -d 2026-03-18 -f all

# 补录记忆数据
python scripts/backfill_memories.py

# 同步价格到 DuckDB
curl -X POST "http://localhost:8888/api/live-trading/sync-prices?trade_date=2026-03-18"
```

### 7.4 API 端点速查

| 分组 | 端点 | 用途 |
|------|------|------|
| 仪表盘 | `GET /api/dashboard/{market}` | 获取完整仪表盘数据 |
| 模拟交易 | `GET /api/live-trading/status` | 调度器状态 |
| | `POST /api/live-trading/trigger` | 手动触发交易 |
| | `POST /api/live-trading/sync-prices` | 手动同步价格 |
| 技能 | `GET /api/skills` | 列出所有技能 |
| | `PUT /api/skills/agent/{name}` | 设置 Agent 技能 |
| 记忆 | `GET /api/memory/active` | 获取活跃记忆 |
| | `POST /api/memory/consolidate` | 触发记忆压缩 |
| 点评 | `POST /api/master-commentary/stream` | 流式大师点评 |
| 健康 | `GET /api/health` | 服务健康检查 |

### 7.5 已知限制

- Tushare `daily()` 仅收盘后出数据，盘中需用 `get_realtime_quotes` 降级
- DuckDB 单进程锁，回测脚本和后端不能同时写入
- `get_realtime_quotes` 依赖网络直连，代理环境需确保 `NO_PROXY=localhost`
- 前端使用 Python `http.server` 静态服务，JSONL 文件需 cache-busting

---

*文档生成时间: 2026-03-18*
*项目版本: v2.4*
