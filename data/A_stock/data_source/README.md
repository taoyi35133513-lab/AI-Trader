# A 股数据源模块

本模块提供统一的 A 股行情数据获取接口，支持 AKShare 和 Tushare 两种数据源，可通过配置灵活切换。

## 模块结构

```
data/A_stock/data_source/
├── __init__.py           # 工厂函数和模块导出
├── base.py               # 抽象基类定义
├── akshare_source.py     # AKShare 数据源实现
├── tushare_source.py     # Tushare 数据源实现
└── README.md             # 本文档
```

## 快速开始

### 1. 创建数据源

```python
from data.A_stock.data_source import create_data_source

# 使用 AKShare（推荐，免费无需 token）
source = create_data_source("akshare")

# 使用 Tushare（需要 token）
source = create_data_source("tushare")
```

### 2. 获取数据

```python
# 获取指数成分股
df_cons = source.get_index_constituents("000016.SH")  # 上证50

# 获取个股日线数据
df_daily = source.get_stock_daily(
    stock_codes=["600519.SH", "600036.SH"],
    start_date="20250101",
    end_date="20250117"
)

# 获取指数日线数据
df_index = source.get_index_daily(
    index_code="000016.SH",
    start_date="20250101",
    end_date="20250117"
)
```

## API 参考

### 工厂函数

```python
create_data_source(source_type: str = "akshare", **kwargs) -> AStockDataSource
```

| 参数 | 类型 | 说明 |
|------|------|------|
| source_type | str | 数据源类型："akshare" 或 "tushare" |
| **kwargs | dict | 传递给数据源构造函数的参数 |

### 数据源接口

所有数据源实现以下统一接口：

#### `get_index_constituents(index_code, start_date, end_date)`

获取指数成分股列表。

| 参数 | 类型 | 说明 |
|------|------|------|
| index_code | str | 指数代码，如 "000016.SH" |
| start_date | str | 开始日期 YYYYMMDD（可选） |
| end_date | str | 结束日期 YYYYMMDD（可选） |

**返回值**：DataFrame，包含列：
- `con_code`: 成分股代码（如 600519.SH）
- `con_name`: 成分股名称
- `weight`: 权重（百分比）
- `trade_date`: 日期

#### `get_stock_daily(stock_codes, start_date, end_date)`

获取个股日线数据。

| 参数 | 类型 | 说明 |
|------|------|------|
| stock_codes | List[str] | 股票代码列表，如 ["600519.SH"] |
| start_date | str | 开始日期 YYYYMMDD |
| end_date | str | 结束日期 YYYYMMDD |

**返回值**：DataFrame，包含列：
- `ts_code`: 股票代码
- `trade_date`: 交易日期
- `open`: 开盘价
- `high`: 最高价
- `low`: 最低价
- `close`: 收盘价
- `vol`: 成交量（手）
- `amount`: 成交额

#### `get_index_daily(index_code, start_date, end_date)`

获取指数日线数据。

| 参数 | 类型 | 说明 |
|------|------|------|
| index_code | str | 指数代码，如 "000016.SH" |
| start_date | str | 开始日期 YYYYMMDD |
| end_date | str | 结束日期 YYYYMMDD |

**返回值**：DataFrame，格式同 `get_stock_daily()`

## 数据源对比

| 特性 | AKShare | Tushare |
|------|---------|---------|
| 是否需要 Token | 否 | 是 |
| 批量获取 | 逐个获取 | 支持批量 |
| 请求限制 | 较宽松 | 6000条/次 |
| 数据延迟 | 实时 | 实时 |
| 推荐场景 | 日常使用 | 大批量获取 |

## API 映射关系

| 功能 | Tushare API | AKShare API |
|------|-------------|-------------|
| 指数成分股 | `pro.index_weight()` | `ak.index_stock_cons_weight_csindex()` |
| 个股日线 | `pro.daily()` | `ak.stock_zh_a_hist()` |
| 指数日线 | `pro.index_daily()` | `ak.index_zh_a_hist()` |

## 数据格式转换

### 股票代码格式

| 格式 | 示例 | 说明 |
|------|------|------|
| 标准格式 | 600519.SH | 本模块统一使用 |
| 纯数字格式 | 600519 | AKShare 使用 |

转换方法：
```python
from data.A_stock.data_source.base import AStockDataSource

# 转为标准格式
code = AStockDataSource.convert_code_to_standard("600519")  # -> "600519.SH"

# 转为纯数字格式
code = AStockDataSource.convert_code_to_plain("600519.SH")  # -> "600519"
```

### 日期格式

统一使用 `YYYYMMDD` 格式（如 "20250117"）。

### 成交量单位

统一使用**手**（1手=100股）。AKShare 返回的是股数，内部自动转换。

## 配置文件

### configs/data_source_config.json

```json
{
  "default_source": "akshare",
  "akshare": {
    "max_retries": 3,
    "retry_delay": 1.0,
    "request_interval": 0.5
  },
  "tushare": {
    "api_url": "http://tushare.xyz:5000",
    "max_retries": 3,
    "retry_delay": 5,
    "timeout": 120
  }
}
```

### configs/astock_config.json

在配置文件中指定数据源：

```json
{
  "agent_type": "BaseAgentAStock",
  "market": "cn",
  "data_source": "akshare",
  ...
}
```

## 入口脚本

### 使用 AKShare 获取数据

```bash
cd data/A_stock
python get_daily_price_akshare.py
```

### 使用 Tushare 获取数据

```bash
cd data/A_stock
python get_daily_price_tushare.py
```

## 支持的指数

| 指数代码 | 名称 | 别名 |
|---------|------|------|
| 000016.SH | 上证50 | sse_50 |
| 000300.SH | 沪深300 | csi_300 |
| 000905.SH | 中证500 | csi_500 |

## 错误处理

所有数据源实现都包含重试机制：

- **AKShare**: 默认重试 3 次，间隔 1 秒
- **Tushare**: 默认重试 3 次，间隔 5 秒（指数退避）

获取失败时返回空 DataFrame，不会抛出异常。

## 扩展新数据源

1. 创建新文件 `xxx_source.py`
2. 继承 `AStockDataSource` 基类
3. 实现三个抽象方法
4. 在 `__init__.py` 的 `create_data_source()` 中注册

```python
from .base import AStockDataSource

class XXXDataSource(AStockDataSource):
    def get_index_constituents(self, index_code, start_date=None, end_date=None):
        # 实现逻辑
        pass

    def get_stock_daily(self, stock_codes, start_date, end_date):
        # 实现逻辑
        pass

    def get_index_daily(self, index_code, start_date, end_date):
        # 实现逻辑
        pass
```

## 依赖

```
akshare>=1.10.0
tushare>=1.2.0  # 可选，仅使用 Tushare 时需要
pandas>=1.0.0
python-dotenv>=0.19.0
```
