# AI-Trader 数据库设计文档

## 概述

AI-Trader 使用 **DuckDB** 作为本地数据库，用于存储和管理股票行情、指数权重、持仓和交易日志等数据。

### 为什么选择 DuckDB

1. **Python 原生支持**: 与 Pandas DataFrame 无缝集成，无需额外的数据转换
2. **高性能**: 列式存储，适合金融数据的时间序列分析
3. **零配置**: 单文件数据库，无需安装服务端
4. **SQL 兼容**: 支持标准 SQL 语法
5. **轻量级**: 数据库文件小，便于备份和迁移

## 目录结构

```
data/
├── database/                    # 数据库模块
│   ├── __init__.py             # 模块入口
│   ├── connection.py           # 连接管理
│   ├── models.py               # 表定义
│   └── ai_trader.duckdb        # 数据库文件（运行后生成）
│
└── scripts/                     # 数据导入脚本
    ├── __init__.py             # 模块入口
    ├── init_database.py        # 数据库初始化
    ├── import_daily_prices.py  # 导入日线数据
    ├── import_hourly_prices.py # 导入小时线数据
    ├── import_index_weights.py # 导入指数权重
    └── import_all.py           # 一键导入所有数据
```

## 表结构设计

### 1. stock_daily_prices (日线行情表)

存储股票日线行情数据。

| 列名 | 类型 | 说明 |
|------|------|------|
| ts_code | VARCHAR(20) | 股票代码 (如 600519.SH) |
| trade_date | DATE | 交易日期 |
| open | DECIMAL(10,4) | 开盘价 |
| high | DECIMAL(10,4) | 最高价 |
| low | DECIMAL(10,4) | 最低价 |
| close | DECIMAL(10,4) | 收盘价 |
| volume | BIGINT | 成交量 |
| amount | DECIMAL(20,4) | 成交额 |
| market | VARCHAR(10) | 市场标识 (cn/us) |
| created_at | TIMESTAMP | 创建时间 |

**主键**: (ts_code, trade_date)

### 2. stock_hourly_prices (小时线行情表)

存储股票小时级行情数据。

| 列名 | 类型 | 说明 |
|------|------|------|
| ts_code | VARCHAR(20) | 股票代码 |
| trade_time | TIMESTAMP | 交易时间 |
| open | DECIMAL(10,4) | 开盘价 |
| high | DECIMAL(10,4) | 最高价 |
| low | DECIMAL(10,4) | 最低价 |
| close | DECIMAL(10,4) | 收盘价 |
| volume | BIGINT | 成交量 |
| market | VARCHAR(10) | 市场标识 |
| created_at | TIMESTAMP | 创建时间 |

**主键**: (ts_code, trade_time)

### 3. index_weights (指数成分股权重表)

存储指数成分股及其权重。

| 列名 | 类型 | 说明 |
|------|------|------|
| index_code | VARCHAR(20) | 指数代码 (如 000016.SH) |
| con_code | VARCHAR(20) | 成分股代码 |
| stock_name | VARCHAR(50) | 股票名称 |
| weight | DECIMAL(10,4) | 权重 (%) |
| trade_date | DATE | 日期 |
| created_at | TIMESTAMP | 创建时间 |

**主键**: (index_code, con_code, trade_date)

### 4. positions (持仓表)

记录 Agent 的持仓变化。

| 列名 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 自增主键 |
| agent_name | VARCHAR(50) | Agent 名称 |
| market | VARCHAR(10) | 市场 |
| trade_date | DATE | 交易日期 |
| step_id | INTEGER | 步骤 ID |
| ts_code | VARCHAR(20) | 股票代码 |
| quantity | INTEGER | 持仓数量 |
| cash | DECIMAL(20,4) | 现金余额 |
| action | VARCHAR(10) | 操作类型 (buy/sell/hold) |
| action_amount | INTEGER | 操作数量 |
| price | DECIMAL(10,4) | 成交价格 |
| total_value | DECIMAL(20,4) | 总资产价值 |
| created_at | TIMESTAMP | 创建时间 |

### 5. trade_logs (交易日志表)

记录 Agent 的交易日志。

| 列名 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 自增主键 |
| agent_name | VARCHAR(50) | Agent 名称 |
| market | VARCHAR(10) | 市场 |
| trade_date | DATE | 交易日期 |
| log_type | VARCHAR(20) | 日志类型 |
| log_content | TEXT | 日志内容 |
| created_at | TIMESTAMP | 创建时间 |

## 操作指引

### 安装依赖

```bash
pip install duckdb pandas
```

### 初始化数据库

```bash
# 创建所有表
python data/scripts/init_database.py

# 重置数据库（删除所有表并重建）
python data/scripts/init_database.py --reset

# 查看表结构
python data/scripts/init_database.py --show-schema
```

### 导入数据

#### 一键导入所有数据

```bash
# 追加模式（默认）
python data/scripts/import_all.py

# 替换模式（清空表后导入）
python data/scripts/import_all.py --mode replace

# 重置数据库并导入
python data/scripts/import_all.py --reset
```

#### 单独导入

```bash
# 导入日线数据
python data/scripts/import_daily_prices.py
python data/scripts/import_daily_prices.py --file /path/to/your/data.csv

# 导入小时线数据
python data/scripts/import_hourly_prices.py

# 导入指数权重数据
python data/scripts/import_index_weights.py
```

### Python 中使用

```python
from data.database import DatabaseManager, query

# 方式一：使用上下文管理器
with DatabaseManager() as db:
    # 查询数据
    df = db.query("SELECT * FROM stock_daily_prices WHERE ts_code = '600519.SH' LIMIT 10")

    # 执行 SQL
    db.execute("DELETE FROM positions WHERE agent_name = 'test'")

    # 插入 DataFrame
    db.insert_df("stock_daily_prices", df, if_exists="append")

    # 检查表是否存在
    if db.table_exists("stock_daily_prices"):
        count = db.get_table_count("stock_daily_prices")
        print(f"共 {count} 条记录")

# 方式二：使用便捷函数
df = query("SELECT * FROM stock_daily_prices LIMIT 10")
```

### 常用查询示例

```sql
-- 查询某只股票的最近 30 日行情
SELECT * FROM stock_daily_prices
WHERE ts_code = '600519.SH'
ORDER BY trade_date DESC
LIMIT 30;

-- 查询上证 50 成分股
SELECT * FROM index_weights
WHERE index_code = '000016.SH'
ORDER BY weight DESC;

-- 查询某日所有股票涨跌幅
SELECT ts_code,
       close,
       (close - open) / open * 100 as change_pct
FROM stock_daily_prices
WHERE trade_date = '2025-01-20'
ORDER BY change_pct DESC;

-- 统计日线数据日期范围
SELECT ts_code,
       MIN(trade_date) as start_date,
       MAX(trade_date) as end_date,
       COUNT(*) as days
FROM stock_daily_prices
GROUP BY ts_code;
```

### DuckDB 特性

DuckDB 支持直接查询 CSV/Parquet 文件，无需导入：

```python
import duckdb

# 直接查询 CSV 文件
df = duckdb.query("""
    SELECT * FROM 'data/A_stock/A_stock_data/daily_prices_sse_50.csv'
    WHERE ts_code = '600519.SH'
""").df()

# 联合查询数据库和文件
df = duckdb.query("""
    SELECT a.*, b.weight
    FROM stock_daily_prices a
    JOIN 'data/A_stock/A_stock_data/sse_50_weight.csv' b
    ON a.ts_code = b.con_code
""").df()
```

## 数据清洗规则

### 日线数据清洗
- 日期格式转换: YYYYMMDD → DATE
- 列名映射: vol → volume
- 添加市场标识: market = 'cn'
- 去重: 按 (ts_code, trade_date) 去重
- 空值处理: 删除 ts_code、trade_date、close 为空的行

### 小时线数据清洗
- 时间格式转换: 'YYYY-MM-DD HH:MM' → TIMESTAMP
- 列名映射: stock_code → ts_code, trade_date → trade_time
- 添加市场标识: market = 'cn'
- 去重: 按 (ts_code, trade_time) 去重

### 指数权重清洗
- 日期格式转换: YYYYMMDD → DATE
- 去重: 按 (index_code, con_code, trade_date) 去重

## 维护指南

### 备份数据库

```bash
# 数据库文件位于 data/database/ai_trader.duckdb
cp data/database/ai_trader.duckdb backup/ai_trader_$(date +%Y%m%d).duckdb
```

### 导出数据

```python
from data.database import query

# 导出为 CSV
df = query("SELECT * FROM stock_daily_prices")
df.to_csv("export/daily_prices.csv", index=False)

# 导出为 Parquet（推荐，文件更小）
df.to_parquet("export/daily_prices.parquet")
```

### 数据库迁移

由于 DuckDB 是单文件数据库，迁移非常简单：

```bash
# 复制数据库文件即可
cp data/database/ai_trader.duckdb /new/location/
```
