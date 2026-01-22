"""
AKShare 新闻工具

使用 akshare 获取 A 股新闻，作为 Alpha Vantage/Finnhub 的免费替代方案。
数据来源：东方财富

优势：
- 免费无限制，无需 API Key
- A 股专属，中文新闻内容
- 数据来源权威（东方财富）
"""

import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastmcp import FastMCP

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.general_tools import get_config_value

logger = logging.getLogger(__name__)


def parse_publish_time(time_str: str) -> Optional[datetime]:
    """解析发布时间字符串

    Args:
        time_str: 发布时间字符串，格式如 "2025-01-22 10:30:00" 或 "01月22日 10:30"

    Returns:
        datetime 对象，解析失败返回 None
    """
    if not time_str:
        return None

    # 尝试多种格式
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%m月%d日 %H:%M",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(time_str, fmt)
            # 如果只有月日，补充年份
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now().year)
            return dt
        except ValueError:
            continue

    return None


class AKShareNewsTool:
    """AKShare 新闻工具类

    使用 akshare 获取 A 股新闻数据。
    """

    def __init__(self):
        # 延迟导入 akshare
        try:
            import akshare as ak

            self.ak = ak
        except ImportError:
            raise ImportError("请安装 akshare: pip install akshare")

    def get_stock_news(self, symbol: str, limit: int = 20) -> List[Dict[str, Any]]:
        """获取个股新闻

        Args:
            symbol: 股票代码（支持 600519 或 600519.SH 格式）
            limit: 返回新闻数量限制

        Returns:
            新闻列表
        """
        # 转换代码格式：600519.SH -> 600519
        clean_symbol = symbol.split(".")[0] if "." in symbol else symbol

        try:
            df = self.ak.stock_news_em(symbol=clean_symbol)

            if df is None or df.empty:
                logger.warning(f"未获取到 {symbol} 的新闻")
                return []

            # 转换为字典列表
            news_list = []
            for _, row in df.iterrows():
                news_item = {
                    "symbol": symbol,
                    "title": row.get("新闻标题", ""),
                    "content": row.get("新闻内容", ""),
                    "publish_time": row.get("发布时间", ""),
                    "source": row.get("文章来源", ""),
                    "url": row.get("新闻链接", ""),
                }
                news_list.append(news_item)

            return news_list[:limit]

        except Exception as e:
            logger.error(f"获取 {symbol} 新闻失败: {e}")
            return []

    def get_general_news(self, limit: int = 20) -> List[Dict[str, Any]]:
        """获取市场通用新闻

        使用百度经济新闻作为通用新闻源

        Args:
            limit: 返回新闻数量限制

        Returns:
            新闻列表
        """
        try:
            df = self.ak.news_economic_baidu()

            if df is None or df.empty:
                logger.warning("未获取到市场新闻")
                return []

            # 转换为字典列表
            news_list = []
            for _, row in df.iterrows():
                news_item = {
                    "symbol": "MARKET",
                    "title": row.get("title", ""),
                    "content": row.get("content", ""),
                    "publish_time": row.get("date", ""),
                    "source": "百度财经",
                    "url": row.get("url", ""),
                }
                news_list.append(news_item)

            return news_list[:limit]

        except Exception as e:
            logger.error(f"获取市场新闻失败: {e}")
            return []

    def __call__(
        self,
        query: str,
        tickers: Optional[str] = None,
        topics: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """搜索新闻（兼容 Alpha Vantage/Finnhub 接口）

        Args:
            query: 搜索关键词（用于日志）
            tickers: 股票代码，多个用逗号分隔（如 "600519.SH,000001.SZ"）
            topics: 新闻主题（akshare 不支持，忽略）

        Returns:
            新闻列表
        """
        print(f"Searching AKShare news: query={query}, tickers={tickers}, topics={topics}")

        # 获取日期过滤配置
        today_date = get_config_value("TODAY_DATE")
        filter_date = None
        if today_date:
            try:
                if " " in today_date:
                    filter_date = datetime.strptime(today_date, "%Y-%m-%d %H:%M:%S")
                else:
                    filter_date = datetime.strptime(today_date, "%Y-%m-%d")
                print(f"Date filter: news before {filter_date}")
            except Exception as e:
                logger.error(f"Failed to parse TODAY_DATE: {e}")

        all_news = []

        # 如果指定了 tickers，获取个股新闻
        if tickers:
            ticker_list = [t.strip() for t in tickers.split(",")]
            for ticker in ticker_list[:5]:  # 限制最多 5 个 ticker
                news = self.get_stock_news(ticker, limit=10)
                all_news.extend(news)
                print(f"Found {len(news)} articles for {ticker}")

        # 如果没有 tickers 或需要通用新闻，获取市场新闻
        if not tickers or topics:
            general_news = self.get_general_news(limit=10)
            all_news.extend(general_news)
            print(f"Found {len(general_news)} general market articles")

        # 日期过滤
        if filter_date:
            filtered_news = []
            for news in all_news:
                pub_time = parse_publish_time(news.get("publish_time", ""))
                if pub_time is None or pub_time <= filter_date:
                    filtered_news.append(news)
            all_news = filtered_news
            print(f"After date filtering: {len(all_news)} articles")

        # 去重（按标题）
        seen_titles = set()
        unique_news = []
        for news in all_news:
            title = news.get("title", "")
            if title and title not in seen_titles:
                seen_titles.add(title)
                unique_news.append(news)

        print(f"Total unique articles: {len(unique_news)}")
        return unique_news[:20]  # 最多返回 20 条


# ==================== MCP 工具定义 ====================

mcp = FastMCP("Search")


@mcp.tool()
def get_market_news(
    query: str,
    tickers: Optional[str] = None,
    topics: Optional[str] = None,
) -> str:
    """
    使用 AKShare 获取 A 股新闻（数据来源：东方财富）。
    仅返回 TODAY_DATE 之前发布的新闻。

    注意：此工具专为 A 股设计，美股代码会被忽略。

    Args:
        query: 搜索查询描述（用于日志）
        tickers: 可选。股票代码，支持 A 股格式。
                示例: "600519.SH" 或 "600519.SH,000001.SZ"
        topics: 可选。新闻主题（当前版本不支持主题筛选）

    Returns:
        包含结构化新闻的字符串：
        - 标题: 新闻标题
        - 摘要: 新闻内容摘要
        - 来源: 新闻来源
        - 时间: 发布时间
    """
    try:
        tool = AKShareNewsTool()
        results = tool(query=query, tickers=tickers, topics=topics)

        if not results:
            return f"未找到符合条件的新闻 '{query}' (tickers={tickers}, topics={topics})。"

        # 格式化输出
        formatted_results = []
        for article in results:
            title = article.get("title", "N/A")
            content = article.get("content", "N/A")
            source = article.get("source", "N/A")
            publish_time = article.get("publish_time", "unknown")
            symbol = article.get("symbol", "")

            # 截断过长的内容
            if content and len(content) > 500:
                content = content[:500] + "..."

            formatted_result = f"""标题: {title}
摘要: {content}
来源: {source} | 时间: {publish_time} | 股票: {symbol}
--------------------------------"""
            formatted_results.append(formatted_result)

        if not formatted_results:
            return f"过滤后未找到符合条件的新闻 '{query}'。"

        return "\n".join(formatted_results)

    except Exception as e:
        logger.error(f"AKShare 新闻工具执行失败: {str(e)}")
        return f"AKShare 新闻工具执行失败: {str(e)}"


@mcp.tool()
def get_stock_news_detail(symbol: str) -> str:
    """
    获取指定 A 股的详细新闻列表。

    Args:
        symbol: A 股代码，如 "600519" 或 "600519.SH"

    Returns:
        该股票的最新新闻列表
    """
    try:
        tool = AKShareNewsTool()
        news_list = tool.get_stock_news(symbol, limit=10)

        if not news_list:
            return f"未找到 {symbol} 的相关新闻。"

        formatted_results = []
        for article in news_list:
            title = article.get("title", "N/A")
            content = article.get("content", "N/A")
            source = article.get("source", "N/A")
            publish_time = article.get("publish_time", "unknown")

            if content and len(content) > 300:
                content = content[:300] + "..."

            formatted_result = f"""【{title}】
{content}
—— {source} ({publish_time})
"""
            formatted_results.append(formatted_result)

        header = f"=== {symbol} 最新新闻 ({len(news_list)} 条) ===\n"
        return header + "\n".join(formatted_results)

    except Exception as e:
        logger.error(f"获取 {symbol} 新闻失败: {str(e)}")
        return f"获取 {symbol} 新闻失败: {str(e)}"


if __name__ == "__main__":
    print("Running AKShare News Tool as search tool")
    port = int(os.getenv("SEARCH_HTTP_PORT", "8001"))
    mcp.run(transport="streamable-http", port=port)
