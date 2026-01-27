"""
Trading Logger - 结构化交易日志系统

提供回测和交易过程中的结构化日志输出，包括：
- 回测进度追踪
- 交易指令记录
- 持仓变动记录
- 性能指标记录
"""

import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

# 配置日志格式
LOG_FORMAT = "%(asctime)s | %(levelname)-5s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class TradingLogger:
    """交易日志记录器"""

    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if TradingLogger._initialized:
            return
        TradingLogger._initialized = True

        # 创建主日志记录器
        self.logger = logging.getLogger("trading")
        self.logger.setLevel(logging.DEBUG)

        # 清除现有处理器
        self.logger.handlers.clear()

        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
        self.logger.addHandler(console_handler)

        # 当前上下文
        self.current_agent: Optional[str] = None
        self.current_date: Optional[str] = None
        self.total_dates: int = 0
        self.processed_dates: int = 0

    def set_context(
        self,
        agent: str,
        date: str,
        total_dates: int = 0,
        processed_dates: int = 0,
    ):
        """设置当前日志上下文"""
        self.current_agent = agent
        self.current_date = date
        self.total_dates = total_dates
        self.processed_dates = processed_dates

    def _format_prefix(self) -> str:
        """格式化日志前缀"""
        parts = []
        if self.current_agent:
            parts.append(f"[{self.current_agent}]")
        if self.current_date:
            parts.append(f"[{self.current_date}]")
        if self.total_dates > 0:
            progress = f"{self.processed_dates}/{self.total_dates}"
            pct = (self.processed_dates / self.total_dates) * 100
            parts.append(f"[{progress} {pct:.0f}%]")
        return " ".join(parts)

    def _log(self, level: int, msg: str, *args, **kwargs):
        """内部日志方法"""
        prefix = self._format_prefix()
        if prefix:
            msg = f"{prefix} {msg}"
        self.logger.log(level, msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        """记录信息日志"""
        self._log(logging.INFO, msg, *args, **kwargs)

    def debug(self, msg: str, *args, **kwargs):
        """记录调试日志"""
        self._log(logging.DEBUG, msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        """记录警告日志"""
        self._log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        """记录错误日志"""
        self._log(logging.ERROR, msg, *args, **kwargs)

    # ===== 回测相关日志 =====

    def log_backtest_start(
        self,
        agent: str,
        start_date: str,
        end_date: str,
        trading_dates: List[str],
    ):
        """记录回测开始"""
        self.current_agent = agent
        self.total_dates = len(trading_dates)
        self.processed_dates = 0

        self.info("=" * 60)
        self.info(f"回测开始: {agent}")
        self.info(f"日期范围: {start_date} -> {end_date}")
        self.info(f"交易日数: {self.total_dates}")
        self.info("=" * 60)

    def log_trading_day_start(self, date: str):
        """记录交易日开始"""
        self.current_date = date
        self.processed_dates += 1
        self.info(f">>> 开始交易日: {date}")

    def log_trading_day_end(self, date: str, result: str = "completed"):
        """记录交易日结束"""
        self.info(f"<<< 结束交易日: {date} ({result})")

    def log_backtest_end(self, summary: Dict[str, Any] = None):
        """记录回测结束"""
        self.info("=" * 60)
        self.info("回测完成!")
        if summary:
            self.info(f"总交易日: {summary.get('total_days', self.total_dates)}")
            self.info(f"最终现金: {summary.get('final_cash', 'N/A')}")
            self.info(f"总资产: {summary.get('total_value', 'N/A')}")
        self.info("=" * 60)

    # ===== 交易相关日志 =====

    def log_trade(
        self,
        action: str,
        symbol: str,
        amount: int,
        price: float,
        cost: float,
        cash_before: float,
        cash_after: float,
    ):
        """记录交易指令"""
        action_emoji = "🟢 买入" if action == "buy" else "🔴 卖出"
        self.info(
            f"{action_emoji} {symbol} x {amount}股 @ ¥{price:.2f} = ¥{cost:.2f} | "
            f"现金: ¥{cash_before:.2f} -> ¥{cash_after:.2f}"
        )

    def log_no_trade(self, reason: str = "维持持仓"):
        """记录不交易"""
        self.info(f"⏸️  无交易: {reason}")

    def log_trade_error(self, action: str, symbol: str, error: str):
        """记录交易错误"""
        self.error(f"❌ {action} {symbol} 失败: {error}")

    # ===== 持仓相关日志 =====

    def log_position_summary(self, positions: Dict[str, Any], prices: Dict[str, float] = None):
        """记录持仓摘要"""
        cash = positions.get("CASH", 0)
        stock_count = sum(1 for k, v in positions.items() if k != "CASH" and v > 0)

        self.info(f"📊 持仓摘要: 现金 ¥{cash:,.2f} | 持股 {stock_count} 只")

        # 如果有价格信息，计算总资产
        if prices:
            total_value = cash
            for symbol, qty in positions.items():
                if symbol != "CASH" and qty > 0:
                    price_key = f"{symbol}_price"
                    if price_key in prices and prices[price_key]:
                        total_value += prices[price_key] * qty
            self.info(f"📈 总资产: ¥{total_value:,.2f}")

    # ===== Agent 步骤日志 =====

    def log_agent_step(self, step: int, max_steps: int, action: str = None):
        """记录 Agent 推理步骤"""
        self.info(f"🔄 步骤 {step}/{max_steps}" + (f" - {action}" if action else ""))

    def log_agent_thinking(self, content: str, max_length: int = 100):
        """记录 Agent 思考过程（简化版）"""
        if len(content) > max_length:
            content = content[:max_length] + "..."
        self.debug(f"💭 思考: {content}")


# 全局单例
_logger = None


def get_trading_logger() -> TradingLogger:
    """获取交易日志记录器单例"""
    global _logger
    if _logger is None:
        _logger = TradingLogger()
    return _logger
