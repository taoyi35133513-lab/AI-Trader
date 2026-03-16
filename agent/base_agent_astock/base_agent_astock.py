"""
BaseAgentAStock class - A股专用交易Agent基类
Chinese A-shares specific trading agent base class
Encapsulates core functionality for A-shares trading including MCP tool management, AI agent creation, and trading execution
"""

import asyncio
import json
import logging
import os
# Import project tools
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from langchain.agents import create_agent
from langchain_core.messages import AIMessage
from langchain_core.utils.function_calling import convert_to_openai_tool
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_openai import ChatOpenAI

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from tools.llm_factory import create_llm

logger = logging.getLogger(__name__)


from prompts.agent_prompt_astock import (STOP_SIGNAL,
                                         get_agent_system_prompt_astock)
from tools.general_tools import (extract_conversation, extract_tool_messages,
                                 get_config_value, write_config_value)
from tools.price_tools import add_no_trade_record
from tools.trading_logger import get_trading_logger

# Load environment variables
load_dotenv()

# DuckDB 服务（延迟导入避免循环依赖）
_conversation_service = None
_db_conn = None


def _get_conversation_service():
    """获取 ConversationService 单例（延迟初始化）

    注意：连接在进程生命周期内保持打开，在 _cleanup_db_connection() 中关闭
    """
    global _conversation_service, _db_conn
    if _conversation_service is None:
        try:
            from data.database.connection import get_connection
            from api.services.conversation_service import ConversationService
            _db_conn = get_connection(read_only=False)
            _conversation_service = ConversationService(_db_conn)
        except Exception as e:
            logger.warning("Failed to initialize DuckDB conversation service: %s", e)
            return None
    return _conversation_service


def _cleanup_db_connection():
    """清理数据库连接（在 Agent 结束时调用）"""
    global _conversation_service, _db_conn
    if _db_conn is not None:
        try:
            _db_conn.close()
        except Exception:
            pass
        _db_conn = None
    _conversation_service = None


class BaseAgentAStock:
    """
    A股专用交易Agent基类
    Chinese A-shares specific trading agent base class

    Main functionalities:
    1. MCP tool management and connection
    2. AI agent creation and configuration
    3. Trading execution and decision loops (with A-shares specific rules)
    4. Logging and management
    5. Position and configuration management
    """

    # Default SSE 50 stock symbols (A-shares only)
    DEFAULT_SSE50_SYMBOLS = [
        "600519.SH",
        "601318.SH",
        "600036.SH",
        "601899.SH",
        "600900.SH",
        "601166.SH",
        "600276.SH",
        "600030.SH",
        "603259.SH",
        "688981.SH",
        "688256.SH",
        "601398.SH",
        "688041.SH",
        "601211.SH",
        "601288.SH",
        "601328.SH",
        "688008.SH",
        "600887.SH",
        "600150.SH",
        "601816.SH",
        "601127.SH",
        "600031.SH",
        "688012.SH",
        "603501.SH",
        "601088.SH",
        "600309.SH",
        "601601.SH",
        "601668.SH",
        "603993.SH",
        "601012.SH",
        "601728.SH",
        "600690.SH",
        "600809.SH",
        "600941.SH",
        "600406.SH",
        "601857.SH",
        "601766.SH",
        "601919.SH",
        "600050.SH",
        "600760.SH",
        "601225.SH",
        "600028.SH",
        "601988.SH",
        "688111.SH",
        "601985.SH",
        "601888.SH",
        "601628.SH",
        "601600.SH",
        "601658.SH",
        "600048.SH",
    ]

    def __init__(
        self,
        signature: str,
        basemodel: str,
        stock_symbols: Optional[List[str]] = None,
        mcp_config: Optional[Dict[str, Dict[str, Any]]] = None,
        log_path: Optional[str] = None,
        max_steps: int = 10,
        max_retries: int = 3,
        base_delay: float = 0.5,
        openai_base_url: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        initial_cash: float = 100000.0,  # 默认10万人民币
        init_date: str = "2025-10-09",
        market: str = "cn",  # 接受但忽略此参数，始终使用"cn"
    ):
        """
        Initialize BaseAgentAStock

        Args:
            signature: Agent signature/name
            basemodel: Base model name
            stock_symbols: List of stock symbols, defaults to SSE 50
            mcp_config: MCP tool configuration, including port and URL information
            log_path: Log path, defaults to ./data/agent_data_astock
            max_steps: Maximum reasoning steps
            max_retries: Maximum retry attempts
            base_delay: Base delay time for retries
            openai_base_url: OpenAI API base URL
            openai_api_key: OpenAI API key
            initial_cash: Initial cash amount (default: 100000.0 RMB)
            init_date: Initialization date
            market: Market type (accepted for compatibility, but always uses "cn")
        """
        self.signature = signature
        self.basemodel = basemodel
        self.market = "cn"  # 硬编码为A股市场

        # 默认使用上证50成分股
        if stock_symbols is None:
            self.stock_symbols = self.DEFAULT_SSE50_SYMBOLS
        else:
            self.stock_symbols = stock_symbols

        self.max_steps = max_steps
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.initial_cash = initial_cash
        self.init_date = init_date

        # Set MCP configuration
        self.mcp_config = mcp_config or self._get_default_mcp_config()

        # Set log path - A股专用路径
        self.base_log_path = log_path or "./data/agent_data_astock"

        # Set OpenAI configuration
        if openai_base_url == None:
            self.openai_base_url = os.getenv("OPENAI_API_BASE")
        else:
            self.openai_base_url = openai_base_url
        if openai_api_key == None:
            self.openai_api_key = os.getenv("OPENAI_API_KEY")
        else:
            self.openai_api_key = openai_api_key

        # Initialize components
        self.client: Optional[MultiServerMCPClient] = None
        self.tools: Optional[List] = None
        self.model: Optional[ChatOpenAI] = None
        self.agent: Optional[Any] = None

        # Data paths
        self.data_path = os.path.join(self.base_log_path, self.signature)
        self.position_file = os.path.join(self.data_path, "position", "position.jsonl")

        # DuckDB session tracking
        self._current_session_id: Optional[int] = None
        self._current_session_timestamp: Optional[datetime] = None

    def _get_default_mcp_config(self) -> Dict[str, Dict[str, Any]]:
        """Get default MCP configuration

        Supports two modes:
        1. Unified mode (default): All MCP services via FastAPI backend at single port
           - Set by UNIFIED_BACKEND_URL env var (default: http://localhost:8888)
        2. Legacy mode: Separate MCP services on different ports
           - Set individual port env vars (MATH_HTTP_PORT, etc.)
           - Or set UNIFIED_MCP_MODE=false
        """
        # Check for unified backend mode (default if UNIFIED_BACKEND_URL is set or not explicitly disabled)
        unified_url = os.getenv("UNIFIED_BACKEND_URL", "http://localhost:8888")
        unified_mode = os.getenv("UNIFIED_MCP_MODE", "true").lower() == "true"

        if unified_mode:
            # Unified mode: All services through single FastAPI backend
            return {
                "math": {
                    "transport": "streamable_http",
                    "url": f"{unified_url}/mcp/math/mcp",
                },
                "stock_local": {
                    "transport": "streamable_http",
                    "url": f"{unified_url}/mcp/price/mcp",
                },
                "search": {
                    "transport": "streamable_http",
                    "url": f"{unified_url}/mcp/search/mcp",
                },
                "trade": {
                    "transport": "streamable_http",
                    "url": f"{unified_url}/mcp/trade/mcp",
                },
            }
        else:
            # Legacy mode: Separate MCP services on different ports
            return {
                "math": {
                    "transport": "streamable_http",
                    "url": f"http://localhost:{os.getenv('MATH_HTTP_PORT', '8000')}/mcp",
                },
                "stock_local": {
                    "transport": "streamable_http",
                    "url": f"http://localhost:{os.getenv('GETPRICE_HTTP_PORT', '8003')}/mcp",
                },
                "search": {
                    "transport": "streamable_http",
                    "url": f"http://localhost:{os.getenv('SEARCH_HTTP_PORT', '8001')}/mcp",
                },
                "trade": {
                    "transport": "streamable_http",
                    "url": f"http://localhost:{os.getenv('TRADE_HTTP_PORT', '8002')}/mcp",
                },
            }

    async def initialize(self) -> None:
        """Initialize MCP client and AI model"""
        logger.info("Initializing A-shares agent: %s", self.signature)

        # Validate OpenAI configuration
        if not self.openai_api_key:
            raise ValueError(
                "❌ OpenAI API key not set. Please configure OPENAI_API_KEY in environment or config file."
            )
        if not self.openai_base_url:
            logger.warning("OpenAI base URL not set, using default")

        try:
            # Create MCP client
            self.client = MultiServerMCPClient(self.mcp_config)

            # Get tools
            self.tools = await self.client.get_tools()
            if not self.tools:
                raise RuntimeError(
                    "No MCP tools loaded. Cannot run trading session without tools. "
                    f"MCP services may not be running. Config: {self.mcp_config}"
                )
            else:
                logger.info("Loaded %d MCP tools", len(self.tools))
        except Exception as e:
            unified_mode = os.getenv("UNIFIED_MCP_MODE", "true").lower() == "true"
            if unified_mode:
                unified_url = os.getenv("UNIFIED_BACKEND_URL", "http://localhost:8888")
                raise RuntimeError(
                    f"❌ Failed to initialize MCP client: {e}\n"
                    f"   Please ensure the unified backend is running at {unified_url}\n"
                    f"   Run: python start.py --only-backend\n"
                    f"   Or use: python start.py (to start everything)"
                )
            else:
                raise RuntimeError(
                    f"❌ Failed to initialize MCP client: {e}\n"
                    f"   Please ensure MCP services are running at the configured ports.\n"
                    f"   Run: python agent_tools/start_mcp_services.py"
                )

        try:
            # Create AI model via factory (auto-detects DeepSeek, etc.)
            self.model = create_llm(
                self.basemodel,
                base_url=self.openai_base_url,
                api_key=self.openai_api_key,
            )
        except Exception as e:
            raise RuntimeError(f"❌ Failed to initialize AI model: {e}")

        # Note: agent will be created in run_trading_session() based on specific date
        # because system_prompt needs the current date and price information

        logger.info("A-shares agent %s initialization completed", self.signature)

    def _setup_logging(self, today_date: str) -> str:
        """Set up log file path, clearing any stale log from previous runs"""
        # Sanitize date for Windows compatibility (replace : with -)
        safe_date = today_date.replace(":", "-")
        log_path = os.path.join(self.base_log_path, self.signature, "log", safe_date)
        if not os.path.exists(log_path):
            os.makedirs(log_path)
        log_file = os.path.join(log_path, "log.jsonl")
        # Truncate existing log file to avoid accumulating entries from multiple runs
        if os.path.exists(log_file):
            open(log_file, "w").close()
        return log_file

    def _log_message(self, log_file: str, new_messages: List[Dict[str, str]], session_timestamp: Optional[datetime] = None) -> None:
        """Log messages to DuckDB (primary) and JSONL file (backup)

        Args:
            log_file: JSONL log file path (for backup)
            new_messages: List of messages [{"role": "user", "content": "..."}]
            session_timestamp: Session timestamp (for DuckDB session creation)
        """
        timestamp = session_timestamp or datetime.now()

        # Normalize new_messages to list
        if isinstance(new_messages, dict):
            new_messages = [new_messages]

        # Write to DuckDB (primary)
        try:
            conv_service = _get_conversation_service()
            if conv_service:
                # Create session if not exists
                if self._current_session_id is None:
                    self._current_session_id = conv_service.create_session(
                        agent_name=self.signature,
                        market=self.market,
                        session_timestamp=timestamp,
                        system_prompt=getattr(self, '_current_system_prompt', None),
                    )
                    self._current_session_timestamp = timestamp

                # Add messages to session
                conv_service.add_messages(
                    session_id=self._current_session_id,
                    messages=new_messages,
                    base_timestamp=timestamp,
                )
        except Exception as e:
            logger.warning("DuckDB log write failed: %s", e)

        # Write to JSONL file (backup) - 保留以便迁移期间的向后兼容
        try:
            log_entry = {"timestamp": timestamp.isoformat(), "signature": self.signature, "new_messages": new_messages}
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning("JSONL log write failed: %s", e)

    async def _ainvoke_with_retry(self, message: List[Dict[str, str]]) -> Any:
        """Agent invocation with retry"""
        for attempt in range(1, self.max_retries + 1):
            try:
                return await self.agent.ainvoke({"messages": message}, {"recursion_limit": 100})
            except Exception as e:
                if attempt == self.max_retries:
                    raise e
                logger.warning("Attempt %d failed, retrying after %.1f seconds... Error: %s", attempt, self.base_delay * attempt, e)
                await asyncio.sleep(self.base_delay * attempt)

    async def run_trading_session(self, today_date: str) -> None:
        """
        Run single day trading session (A-shares specific)

        Args:
            today_date: Trading date
        """
        # 获取日志记录器
        trading_logger = get_trading_logger()
        trading_logger.set_context(agent=self.signature, date=today_date)
        trading_logger.log_trading_day_start(today_date)

        # Reset session tracking for new trading day
        self._current_session_id = None
        self._current_session_timestamp = None

        # Parse session timestamp from today_date
        try:
            if " " in today_date:
                session_timestamp = datetime.strptime(today_date, "%Y-%m-%d %H:%M:%S")
            else:
                session_timestamp = datetime.strptime(today_date, "%Y-%m-%d")
        except ValueError:
            session_timestamp = datetime.now()

        # Set up logging
        log_file = self._setup_logging(today_date)

        # Update system prompt - 使用A股专用提示词
        self._current_system_prompt = get_agent_system_prompt_astock(today_date, self.signature, self.stock_symbols)
        self.agent = create_agent(
            self.model,
            tools=self.tools,
            system_prompt=self._current_system_prompt,
        )

        # Initial user query
        user_query = [{"role": "user", "content": f"请分析并更新今日（{today_date}）的持仓。"}]
        message = user_query.copy()

        # Log initial message
        self._log_message(log_file, user_query, session_timestamp)

        # Trading loop
        current_step = 0
        while current_step < self.max_steps:
            current_step += 1
            trading_logger.log_agent_step(current_step, self.max_steps)

            try:
                # Re-assert runtime config before each agent call to prevent
                # race conditions when backtest and live run concurrently.
                write_config_value("SIGNATURE", self.signature)
                write_config_value("TODAY_DATE", today_date)

                # Call agent
                response = await self._ainvoke_with_retry(message)

                # Extract agent response
                agent_response = extract_conversation(response, "final")

                # Check stop signal
                if STOP_SIGNAL in agent_response:
                    trading_logger.info("收到停止信号，交易会话结束")
                    self._log_message(log_file, [{"role": "assistant", "content": agent_response}])
                    break

                # Extract tool messages
                tool_msgs = extract_tool_messages(response)
                tool_response = "\n".join([
                    msg.content if isinstance(msg.content, str) else str(msg.content)
                    for msg in tool_msgs if msg.content is not None
                ])

                # Prepare new messages
                new_messages = [
                    {"role": "assistant", "content": agent_response},
                    {"role": "user", "content": f"Tool results: {tool_response}"},
                ]

                # Add new messages
                message.extend(new_messages)

                # Log messages
                self._log_message(log_file, new_messages[0])
                self._log_message(log_file, new_messages[1])

            except Exception as e:
                trading_logger.error(f"交易会话错误: {str(e)}")
                raise

        # Handle trading results
        await self._handle_trading_result(today_date)

        # Generate L1 reflection after trading session
        await self._generate_reflection(today_date, message)

        trading_logger.log_trading_day_end(today_date)

    async def _handle_trading_result(self, today_date: str) -> None:
        """Handle trading results"""
        trading_logger = get_trading_logger()
        if_trade = get_config_value("IF_TRADE")
        if if_trade:
            write_config_value("IF_TRADE", False)
            trading_logger.info("交易完成")
        else:
            trading_logger.log_no_trade("维持当前持仓")
            try:
                add_no_trade_record(today_date, self.signature)
            except NameError as e:
                trading_logger.error(f"添加无交易记录失败: {e}")
                raise
            write_config_value("IF_TRADE", False)

    async def _generate_reflection(self, date_str: str, messages: list):
        """Generate a L1 reflection after trading session."""
        try:
            import duckdb
            from api.config import get_database_path
            from api.services.memory_service import MemoryService
            from api.services.memory_consolidation import consolidate_l1_to_l2, consolidate_l2_to_l3
            from prompts.components.memory import REFLECTION_GENERATE_PROMPT

            # Extract actions and reasoning from messages
            actions = []
            reasoning_parts = []
            for msg in messages:
                if isinstance(msg, dict):
                    content = msg.get("content", "")
                    role = msg.get("role", "")
                    if role == "assistant" and content and len(content) > 20:
                        reasoning_parts.append(content[:300])
                elif hasattr(msg, 'content') and msg.content:
                    content = msg.content if isinstance(msg.content, str) else str(msg.content)
                    if len(content) > 20:
                        reasoning_parts.append(content[:300])
                # Check for tool calls (trade actions)
                if hasattr(msg, 'tool_calls'):
                    for tc in (msg.tool_calls or []):
                        if isinstance(tc, dict):
                            actions.append(f"{tc.get('name', '')}: {tc.get('args', '')}")

            if not actions:
                actions = ["无交易操作"]

            actions_text = "; ".join(actions[:5])
            reasoning_text = "\n".join(reasoning_parts[:3]) if reasoning_parts else "无推理记录"

            # Use the agent's own LLM config for reflection generation
            from openai import AsyncOpenAI

            api_key = self.openai_api_key or os.environ.get("OPENAI_API_KEY", "")
            base_url = self.openai_base_url or os.environ.get("OPENAI_API_BASE", "")
            model_name = self.basemodel or "deepseek-chat"

            if not api_key:
                return

            client = AsyncOpenAI(api_key=api_key, base_url=base_url or None)

            prompt = REFLECTION_GENERATE_PROMPT.format(
                date=date_str,
                actions=actions_text,
                pnl="见持仓变化",
                reasoning_summary=reasoning_text[:800],
            )

            resp = await client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": "你是一个交易复盘助手，用中文简洁地总结交易经验。"},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=300,
                temperature=0.3,
            )

            reflection = resp.choices[0].message.content.strip()
            if not reflection:
                return

            # Save to DB
            db_path = get_database_path()
            conn = duckdb.connect(str(db_path), read_only=False)
            try:
                svc = MemoryService(conn)
                market = "cn_hour" if "-astock-hour" in self.signature else "cn"
                svc.add_reflection(
                    agent_name=self.signature,
                    market=market,
                    content=reflection,
                    source_date=date_str,
                    session_id=self._current_session_id,
                )

                # Check if consolidation needed
                if svc.should_consolidate_l1(self.signature, market):
                    await consolidate_l1_to_l2(svc, self.signature, market)
                if svc.should_consolidate_l2(self.signature, market):
                    await consolidate_l2_to_l3(svc, self.signature, market)

                logger.info("[%s] Reflection saved for %s", self.signature, date_str)
            finally:
                conn.close()

        except Exception as e:
            logger.debug("[%s] Reflection generation skipped: %s", self.signature, e)

    def register_agent(self) -> None:
        """Register new agent, create initial positions"""
        # Check if position.jsonl file already exists
        if os.path.exists(self.position_file):
            logger.info("Position file %s already exists, skipping registration", self.position_file)
            return

        # Ensure directory structure exists
        position_dir = os.path.join(self.data_path, "position")
        if not os.path.exists(position_dir):
            os.makedirs(position_dir)
            logger.info("Created position directory: %s", position_dir)

        # Create initial positions
        init_position = {symbol: 0 for symbol in self.stock_symbols}
        init_position["CASH"] = self.initial_cash
        # Normalize init_date to zero-padded HH if time exists
        init_date_str = self.init_date
        if " " in init_date_str:
            try:
                # If already proper format, keep it
                datetime.strptime(init_date_str, "%Y-%m-%d %H:%M:%S")
            except Exception:
                try:
                    date_part, time_part = init_date_str.split(" ", 1)
                    hh, mm, ss = time_part.split(":")
                    init_date_str = f"{date_part} {hh.zfill(2)}:{mm}:{ss}"
                except Exception:
                    # Fallback: keep original if unexpected
                    pass

        # Write initial position to both JSONL and DuckDB
        from tools.data_access import PositionDataAccess
        pos_access = PositionDataAccess()
        init_action = {"action": "init", "symbol": "", "amount": 0}
        pos_access.add_position_record(init_date_str, self.signature, init_action, init_position)

        logger.info("A-shares agent %s registered: position=%s, cash=%.2f, stocks=%d",
                    self.signature, self.position_file, self.initial_cash, len(self.stock_symbols))

    def get_trading_dates(self, init_date: str, end_date: str) -> List[str]:
        """
        Get trading date list, filtered by actual trading days in A-shares market

        Args:
            init_date: Start date
            end_date: End date

        Returns:
            List of trading dates (excluding weekends and holidays)
        """
        from tools.price_tools import is_trading_day

        dates = []
        max_date = None

        if not os.path.exists(self.position_file):
            self.register_agent()
            max_date = init_date
        else:
            # Read existing position file, find latest date
            with open(self.position_file, "r") as f:
                for line in f:
                    doc = json.loads(line)
                    current_date = doc["date"]
                    if max_date is None:
                        max_date = current_date
                    else:
                        current_date_obj = datetime.strptime(current_date, "%Y-%m-%d")
                        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
                        if current_date_obj > max_date_obj:
                            max_date = current_date

        # Check if new dates need to be processed
        max_date_obj = datetime.strptime(max_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")

        if end_date_obj <= max_date_obj:
            return []

        # Generate trading date list, filtered by actual trading days (A-shares market)
        trading_dates = []
        current_date = max_date_obj + timedelta(days=1)

        while current_date <= end_date_obj:
            date_str = current_date.strftime("%Y-%m-%d")
            # Check if this is an actual trading day in A-shares market
            if is_trading_day(date_str, market="cn"):
                trading_dates.append(date_str)
            current_date += timedelta(days=1)

        return trading_dates

    async def run_with_retry(self, today_date: str) -> None:
        """Run method with retry"""
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info("Attempting to run %s - %s (Attempt %d)", self.signature, today_date, attempt)
                await self.run_trading_session(today_date)
                logger.info("%s - %s run successful", self.signature, today_date)
                return
            except Exception as e:
                logger.error("Attempt %d failed: %s", attempt, e)
                if attempt == self.max_retries:
                    logger.error("%s - %s all retries failed", self.signature, today_date)
                    raise
                else:
                    wait_time = self.base_delay * attempt
                    logger.info("Waiting %.1f seconds before retry...", wait_time)
                    await asyncio.sleep(wait_time)

    async def run_date_range(self, init_date: str, end_date: str) -> None:
        """
        Run all trading days in date range

        Args:
            init_date: Start date
            end_date: End date
        """
        # 获取日志记录器
        trading_logger = get_trading_logger()

        # Get trading date list
        trading_dates = self.get_trading_dates(init_date, end_date)

        if not trading_dates:
            trading_logger.info(f"[{self.signature}] 无需处理的交易日")
            return

        # 记录回测开始
        trading_logger.log_backtest_start(
            agent=self.signature,
            start_date=init_date,
            end_date=end_date,
            trading_dates=trading_dates,
        )

        # Process each trading day
        for i, date in enumerate(trading_dates):
            trading_logger.set_context(
                agent=self.signature,
                date=date,
                total_dates=len(trading_dates),
                processed_dates=i,
            )

            # Set configuration
            write_config_value("TODAY_DATE", date)
            write_config_value("SIGNATURE", self.signature)

            try:
                await self.run_with_retry(date)
            except Exception as e:
                trading_logger.error(f"处理日期 {date} 时出错: {e}")
                raise

        # 记录回测结束
        summary = self.get_position_summary()
        trading_logger.log_backtest_end({
            "total_days": len(trading_dates),
            "final_cash": summary.get("positions", {}).get("CASH", "N/A"),
        })

        # 清理数据库连接
        _cleanup_db_connection()

    def get_position_summary(self) -> Dict[str, Any]:
        """Get position summary"""
        if not os.path.exists(self.position_file):
            return {"error": "Position file does not exist"}

        positions = []
        with open(self.position_file, "r") as f:
            for line in f:
                positions.append(json.loads(line))

        if not positions:
            return {"error": "No position records"}

        latest_position = positions[-1]
        return {
            "signature": self.signature,
            "latest_date": latest_position.get("date"),
            "positions": latest_position.get("positions", {}),
            "total_records": len(positions),
        }

    def __str__(self) -> str:
        return (
            f"BaseAgentAStock(signature='{self.signature}', basemodel='{self.basemodel}', "
            f"market='cn', stocks={len(self.stock_symbols)})"
        )

    def __repr__(self) -> str:
        return self.__str__()
