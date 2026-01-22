# Configuration Files

This directory contains configuration files for the AI-Trader Bench. These JSON configuration files define the parameters and settings used by the trading agents during execution.

## Files

This directory contains configuration files for A-share (China market) trading:

### Available Configurations

| Configuration File | Market | Trading Frequency | Description |
|-------------------|--------|-------------------|-------------|
| `astock_config.json` | CN (SSE 50) | Daily | A-share daily trading configuration |
| `astock_hour_config.json` | CN (SSE 50) | Hourly | A-share hourly trading configuration (10:30/11:30/14:00/15:00) |

### `astock_config.json`

The main configuration file for A-share daily trading. This file is loaded by `main.py` and contains the following sections:

#### Agent Configuration
- **`agent_type`**: Specifies which agent class to use (BaseAgentAStock or BaseAgentAStock_Hour)
- **`agent_config`**: Agent-specific parameters
  - `max_steps`: Maximum number of reasoning steps per trading decision (default: 30)
  - `max_retries`: Maximum retry attempts for failed operations (default: 3)
  - `base_delay`: Base delay between operations in seconds (default: 1.0)
  - `initial_cash`: Starting cash amount for trading (default: ¥100,000)

#### Date Range
- **`date_range`**: Trading period configuration
  - `init_date`: Start date for trading simulation (format: YYYY-MM-DD)
  - `end_date`: End date for trading simulation (format: YYYY-MM-DD)

#### Model Configuration
- **`models`**: List of AI models to use for trading decisions
  - Each model entry contains:
    - `name`: Display name for the model
    - `basemodel`: Full model identifier/path
    - `signature`: Model signature for API calls
    - `enabled`: Boolean flag to enable/disable the model

#### Logging Configuration
- **`log_config`**: Logging parameters
  - `log_path`: Directory path where agent data and logs are stored

## Usage

### Quick Start with Scripts

The easiest way to run the system:

```bash
# A-Share Market (SSE 50) - uses astock_config.json
bash scripts/main_a_stock_step1.sh  # Prepare A-share data
bash scripts/main_a_stock_step2.sh  # Start MCP services
bash scripts/main_a_stock_step3.sh  # Run with astock_config.json
```

### Using the Unified Start Script

```bash
python start.py                    # A-stock daily trading (default)
python start.py -f hourly          # A-stock hourly trading
python start.py --skip-data        # Skip data preparation
python start.py --only-mcp         # Only start MCP services
python start.py --only-agent       # Only start agent
```

### Manual Configuration

#### Default Configuration
The system automatically loads `astock_config.json` when no specific configuration file is provided:

```bash
python main.py
```

#### Custom Configuration
You can specify a custom configuration file:

```bash
python main.py configs/astock_config.json
python main.py configs/astock_hour_config.json
```

### Environment Variable Overrides
Certain configuration values can be overridden using environment variables:
- `INIT_DATE`: Overrides the initial trading date
- `END_DATE`: Overrides the end trading date

## Configuration Examples

### A-Share Daily Configuration (BaseAgentAStock)
```json
{
  "agent_type": "BaseAgentAStock",
  "market": "cn",
  "date_range": {
    "init_date": "2025-10-09",
    "end_date": "2025-10-31"
  },
  "models": [
    {
      "name": "claude-3.7-sonnet",
      "basemodel": "anthropic/claude-3.7-sonnet",
      "signature": "claude-3.7-sonnet",
      "enabled": true
    }
  ],
  "agent_config": {
    "max_steps": 30,
    "initial_cash": 100000.0
  },
  "log_config": {
    "log_path": "./data/agent_data_astock"
  }
}
```

### A-Share Hourly Configuration (BaseAgentAStock_Hour)
```json
{
  "agent_type": "BaseAgentAStock_Hour",
  "market": "cn",
  "date_range": {
    "init_date": "2025-10-09 10:30:00",
    "end_date": "2025-10-31 15:00:00"
  },
  "models": [
    {
      "name": "claude-3.7-sonnet",
      "basemodel": "anthropic/claude-3.7-sonnet",
      "signature": "claude-3.7-sonnet-astock-hour",
      "enabled": true
    }
  ],
  "agent_config": {
    "max_steps": 30,
    "initial_cash": 100000.0
  },
  "log_config": {
    "log_path": "./data/agent_data_astock_hour"
  }
}
```

> 💡 **Tip**: A-share hourly trading time points: 10:30, 11:30, 14:00, 15:00 (4 time points per day)

### Multi-Model Configuration
```json
{
  "agent_type": "BaseAgentAStock",
  "market": "cn",
  "date_range": {
    "init_date": "2025-01-01",
    "end_date": "2025-01-31"
  },
  "models": [
    {
      "name": "claude-3.7-sonnet",
      "basemodel": "anthropic/claude-3.7-sonnet",
      "signature": "claude-3.7-sonnet",
      "enabled": true
    },
    {
      "name": "gpt-4o",
      "basemodel": "openai/gpt-4o-2024-11-20",
      "signature": "gpt-4o-2024-11-20",
      "enabled": true
    },
    {
      "name": "qwen3-max",
      "basemodel": "qwen/qwen3-max",
      "signature": "qwen3-max",
      "enabled": false
    }
  ],
  "agent_config": {
    "max_steps": 50,
    "max_retries": 5,
    "base_delay": 2.0,
    "initial_cash": 100000.0
  },
  "log_config": {
    "log_path": "./data/agent_data_astock"
  }
}
```

## Agent Types

### BaseAgentAStock (A-Shares Daily)
- **Market Support**: A-share market only
- **Trading Frequency**: Daily
- **Use Case**: Specialized A-share daily trading with built-in Chinese market rules
- **Stock Pool**: SSE 50 by default
- **Trading Rules**: T+1 settlement, 100-share lot size, CNY pricing

### BaseAgentAStock_Hour (A-Shares Hourly)
- **Market Support**: A-share market only
- **Trading Frequency**: Hourly (10:30/11:30/14:00/15:00)
- **Use Case**: A-share hourly trading with 4 intraday time points
- **Stock Pool**: SSE 50 by default
- **Trading Rules**: T+1 settlement, 100-share lot size, CNY pricing
- **Data Source**: merged_hourly.jsonl

## Notes

- Configuration files must be valid JSON format
- The system validates date ranges and ensures `init_date` is not greater than `end_date`
- Only models with `enabled: true` will be used for trading simulations
- Configuration errors will cause the system to exit with appropriate error messages
- The configuration system supports dynamic agent class loading through the `AGENT_REGISTRY` mapping
- Initial cash should be ¥100,000 for A-shares (default)
