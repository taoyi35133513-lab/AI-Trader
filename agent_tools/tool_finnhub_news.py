"""
Finnhub 新闻工具

使用 Finnhub API 获取金融新闻，作为 Alpha Vantage 的备选方案。
Finnhub 免费层：60 次/分钟，适合开发和轻量使用。

API 文档: https://finnhub.io/docs/api/company-news
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from fastmcp import FastMCP

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.general_tools import get_config_value

logger = logging.getLogger(__name__)


def parse_unix_timestamp(timestamp: int) -> str:
    """将 Unix 时间戳转换为标准日期格式

    Args:
        timestamp: Unix 时间戳（秒）

    Returns:
        标准格式日期字符串 "YYYY-MM-DD HH:MM:SS"
    """
    try:
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return "unknown"


class FinnhubNewsTool:
    """Finnhub 新闻工具类

    支持两种新闻 API：
    1. Company News - 获取特定公司的新闻
    2. General News - 获取市场通用新闻
    """

    def __init__(self):
        self.api_key = os.environ.get("FINNHUB_API_KEY")
        if not self.api_key:
            raise ValueError(
                "Finnhub API key not provided! Please set FINNHUB_API_KEY environment variable. "
                "Get free API key at: https://finnhub.io/"
            )
        self.base_url = "https://finnhub.io/api/v1"

    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Any:
        """发送 API 请求

        Args:
            endpoint: API 端点
            params: 请求参数

        Returns:
            JSON 响应数据
        """
        params["token"] = self.api_key
        url = f"{self.base_url}/{endpoint}"

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            # 检查 API 错误
            if isinstance(data, dict) and "error" in data:
                raise Exception(f"Finnhub API error: {data['error']}")

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Finnhub API request failed: {e}")
            raise Exception(f"Finnhub API request failed: {e}")

    def get_company_news(
        self,
        symbol: str,
        from_date: str,
        to_date: str,
    ) -> List[Dict[str, Any]]:
        """获取公司新闻

        Args:
            symbol: 股票代码（如 "AAPL"）
            from_date: 开始日期 "YYYY-MM-DD"
            to_date: 结束日期 "YYYY-MM-DD"

        Returns:
            新闻列表
        """
        params = {
            "symbol": symbol.upper(),
            "from": from_date,
            "to": to_date,
        }

        news = self._make_request("company-news", params)
        return news if isinstance(news, list) else []

    def get_general_news(self, category: str = "general") -> List[Dict[str, Any]]:
        """获取市场通用新闻

        Args:
            category: 新闻类别
                - "general": 通用新闻
                - "forex": 外汇新闻
                - "crypto": 加密货币新闻
                - "merger": 并购新闻

        Returns:
            新闻列表
        """
        params = {"category": category}

        news = self._make_request("news", params)
        return news if isinstance(news, list) else []

    def get_market_sentiment(self, symbol: str) -> Dict[str, Any]:
        """获取社交媒体情感分析（Reddit/Twitter）

        Args:
            symbol: 股票代码

        Returns:
            情感分析数据
        """
        params = {"symbol": symbol.upper()}

        try:
            sentiment = self._make_request("stock/social-sentiment", params)
            return sentiment if isinstance(sentiment, dict) else {}
        except Exception as e:
            logger.warning(f"Failed to get sentiment for {symbol}: {e}")
            return {}

    def __call__(
        self,
        query: str,
        tickers: Optional[str] = None,
        topics: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """搜索新闻（兼容 Alpha Vantage 接口）

        Args:
            query: 搜索关键词（用于日志）
            tickers: 股票代码，多个用逗号分隔（如 "AAPL,MSFT"）
            topics: 新闻主题（映射到 Finnhub 的 category）

        Returns:
            新闻列表
        """
        print(f"Searching Finnhub news: query={query}, tickers={tickers}, topics={topics}")

        # 获取日期范围
        today_date = get_config_value("TODAY_DATE")
        if today_date:
            try:
                if " " in today_date:
                    today_dt = datetime.strptime(today_date, "%Y-%m-%d %H:%M:%S")
                else:
                    today_dt = datetime.strptime(today_date, "%Y-%m-%d")
                to_date = today_dt.strftime("%Y-%m-%d")
                from_date = (today_dt - timedelta(days=30)).strftime("%Y-%m-%d")
                print(f"Date range: {from_date} to {to_date}")
            except Exception as e:
                logger.error(f"Failed to parse TODAY_DATE: {e}")
                to_date = datetime.now().strftime("%Y-%m-%d")
                from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        else:
            to_date = datetime.now().strftime("%Y-%m-%d")
            from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        all_news = []

        # 如果指定了 tickers，获取公司新闻
        if tickers:
            ticker_list = [t.strip().upper() for t in tickers.split(",")]
            for ticker in ticker_list[:5]:  # 限制最多 5 个 ticker
                # 移除 A 股后缀（Finnhub 不支持 A 股）
                clean_ticker = ticker.split(".")[0] if "." in ticker else ticker
                # 跳过明显的 A 股代码
                if clean_ticker.isdigit() and len(clean_ticker) == 6:
                    print(f"Skipping A-share ticker: {ticker}")
                    continue

                try:
                    news = self.get_company_news(clean_ticker, from_date, to_date)
                    for item in news:
                        item["_ticker"] = clean_ticker  # 标记来源 ticker
                    all_news.extend(news[:10])  # 每个 ticker 最多 10 条
                    print(f"Found {len(news)} articles for {clean_ticker}")
                except Exception as e:
                    logger.warning(f"Failed to get news for {clean_ticker}: {e}")

        # 如果指定了 topics 或没有 tickers，获取通用新闻
        if topics or not tickers:
            # 将 Alpha Vantage topics 映射到 Finnhub category
            category_map = {
                "technology": "general",
                "blockchain": "crypto",
                "earnings": "general",
                "ipo": "general",
                "mergers_and_acquisitions": "merger",
                "financial_markets": "general",
                "economy_fiscal": "general",
                "economy_monetary": "general",
                "economy_macro": "general",
                "forex": "forex",
                "crypto": "crypto",
            }

            categories = set()
            if topics:
                for topic in topics.split(","):
                    topic = topic.strip().lower()
                    category = category_map.get(topic, "general")
                    categories.add(category)
            else:
                categories.add("general")

            for category in categories:
                try:
                    news = self.get_general_news(category)
                    # 按日期过滤
                    filtered_news = []
                    for item in news:
                        news_date = parse_unix_timestamp(item.get("datetime", 0))
                        if news_date <= to_date + " 23:59:59":
                            filtered_news.append(item)
                    all_news.extend(filtered_news[:10])
                    print(f"Found {len(filtered_news)} articles for category '{category}'")
                except Exception as e:
                    logger.warning(f"Failed to get general news for {category}: {e}")

        # 去重（按 URL）
        seen_urls = set()
        unique_news = []
        for item in all_news:
            url = item.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_news.append(item)

        # 按时间排序（最新优先）
        unique_news.sort(key=lambda x: x.get("datetime", 0), reverse=True)

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
    Use Finnhub API to retrieve market news articles with date filtering.
    Only returns articles published before TODAY_DATE (as configured in runtime config).

    Note: Finnhub primarily supports US stocks. A-share tickers (e.g., 600519.SH) will be skipped.

    Args:
        query: Search query description (used for logging purposes)
        tickers: Optional. Stock symbols to filter by (US stocks only).
                Examples: "AAPL" or "AAPL,MSFT,GOOGL"
        topics: Optional. News topics to filter by.
                Supported topics: technology, blockchain, earnings, ipo,
                mergers_and_acquisitions, financial_markets, economy_fiscal,
                economy_monetary, economy_macro, forex, crypto

    Returns:
        A string containing structured news articles with:
        - Title: Article title
        - Source: News source
        - Summary: Article summary
        - Published: Publication time
    """
    try:
        tool = FinnhubNewsTool()
        results = tool(query=query, tickers=tickers, topics=topics)

        if not results:
            return f"No news articles found matching criteria '{query}' (tickers={tickers}, topics={topics})."

        # 格式化输出
        formatted_results = []
        for article in results:
            title = article.get("headline", "N/A")
            source = article.get("source", "N/A")
            summary = article.get("summary", "N/A")
            url = article.get("url", "N/A")
            timestamp = article.get("datetime", 0)
            published = parse_unix_timestamp(timestamp) if timestamp else "unknown"

            # 情感标签（Finnhub 不直接提供，但可以根据关键词简单判断）
            category = article.get("category", "general")

            formatted_result = f"""Title: {title}
Summary: {summary[:800] if summary else 'N/A'}
Source: {source} | Published: {published}
--------------------------------"""
            formatted_results.append(formatted_result)

        if not formatted_results:
            return f"No news articles found matching criteria '{query}' after filtering."

        return "\n".join(formatted_results)

    except Exception as e:
        logger.error(f"Finnhub news tool execution failed: {str(e)}")
        return f"Finnhub news tool execution failed: {str(e)}"


@mcp.tool()
def get_social_sentiment(symbol: str) -> str:
    """
    Get social media sentiment analysis for a stock from Reddit and Twitter.

    Args:
        symbol: Stock symbol (US stocks only, e.g., "AAPL")

    Returns:
        Social sentiment summary including mention counts and sentiment scores
    """
    try:
        tool = FinnhubNewsTool()
        sentiment = tool.get_market_sentiment(symbol)

        if not sentiment:
            return f"No sentiment data available for {symbol}"

        # 格式化 Reddit 数据
        reddit = sentiment.get("reddit", [])
        reddit_summary = "No Reddit data"
        if reddit:
            total_mentions = sum(item.get("mention", 0) for item in reddit)
            avg_score = sum(item.get("score", 0) for item in reddit) / len(reddit) if reddit else 0
            reddit_summary = f"Mentions: {total_mentions}, Avg Score: {avg_score:.2f}"

        # 格式化 Twitter 数据
        twitter = sentiment.get("twitter", [])
        twitter_summary = "No Twitter data"
        if twitter:
            total_mentions = sum(item.get("mention", 0) for item in twitter)
            avg_score = sum(item.get("score", 0) for item in twitter) / len(twitter) if twitter else 0
            twitter_summary = f"Mentions: {total_mentions}, Avg Score: {avg_score:.2f}"

        return f"""Social Sentiment for {symbol.upper()}:
Reddit: {reddit_summary}
Twitter: {twitter_summary}
"""

    except Exception as e:
        logger.error(f"Finnhub sentiment tool failed: {str(e)}")
        return f"Failed to get sentiment for {symbol}: {str(e)}"


if __name__ == "__main__":
    print("Running Finnhub News Tool as search tool")
    port = int(os.getenv("SEARCH_HTTP_PORT", "8001"))
    mcp.run(transport="streamable-http", port=port)
