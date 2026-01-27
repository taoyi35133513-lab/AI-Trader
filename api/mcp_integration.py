"""
MCP Services Integration Module

This module integrates all MCP services (Math, Trade, Search, Price) into the FastAPI application
using FastMCP's http_app() method for ASGI mounting.

Architecture:
- Each MCP service is mounted at /mcp/{service_name}/
- Services share a combined lifespan with the main FastAPI app
- Client agents connect to unified backend URL instead of separate ports
"""

import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Optional

# Add project root to path for imports
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import MCP instances from tool modules
from agent_tools.tool_math import mcp as math_mcp
from agent_tools.tool_trade import mcp as trade_mcp
from agent_tools.tool_akshare_news import mcp as search_mcp
from agent_tools.tool_get_price_local import mcp as price_mcp


# Cached MCP apps - created once and reused
_mcp_apps: Optional[Dict[str, Any]] = None


def get_mcp_apps() -> Dict[str, Any]:
    """
    Get ASGI apps for all MCP services (cached singleton).

    Returns:
        Dictionary mapping service names to their ASGI apps.
        Each app should be mounted at /mcp/{name}/ in FastAPI.
    """
    global _mcp_apps

    if _mcp_apps is None:
        _mcp_apps = {
            "math": math_mcp.http_app(path="/mcp"),
            "search": search_mcp.http_app(path="/mcp"),
            "trade": trade_mcp.http_app(path="/mcp"),
            "price": price_mcp.http_app(path="/mcp"),
        }

    return _mcp_apps


def combine_lifespans(*lifespans: Callable):
    """
    Combine multiple lifespan context managers into one.

    This is necessary because FastAPI only accepts one lifespan,
    but we need to manage lifespans from multiple MCP apps plus
    the main FastAPI app.

    Args:
        *lifespans: Lifespan callables from various ASGI apps

    Returns:
        Combined lifespan context manager
    """
    @asynccontextmanager
    async def combined_lifespan(app):
        # Filter out None lifespans
        valid_lifespans = [ls for ls in lifespans if ls is not None]

        if not valid_lifespans:
            yield
            return

        # Enter all lifespans
        cms = []
        for lifespan in valid_lifespans:
            cm = lifespan(app)
            await cm.__aenter__()
            cms.append(cm)

        try:
            yield
        finally:
            # Exit all lifespans in reverse order
            for cm in reversed(cms):
                await cm.__aexit__(None, None, None)

    return combined_lifespan


def get_combined_lifespan():
    """
    Get a combined lifespan that includes all MCP service lifespans.

    IMPORTANT: This must use the SAME cached apps that will be mounted,
    otherwise the mounted apps won't have their lifespans initialized.

    Returns:
        Combined lifespan context manager for FastAPI
    """
    mcp_apps = get_mcp_apps()  # Uses cached apps
    lifespans = []

    for name, app in mcp_apps.items():
        if hasattr(app, 'lifespan') and app.lifespan is not None:
            lifespans.append(app.lifespan)

    return combine_lifespans(*lifespans)


# MCP service URL mappings for agent configuration
# When running in unified mode, agents connect to these paths instead of separate ports
MCP_SERVICE_PATHS = {
    "math": "/mcp/math/mcp",
    "search": "/mcp/search/mcp",
    "trade": "/mcp/trade/mcp",
    "price": "/mcp/price/mcp",
}


def get_mcp_client_urls(base_url: str = "http://localhost:8888") -> Dict[str, str]:
    """
    Get full URLs for MCP client connections.

    Args:
        base_url: The base URL of the unified FastAPI server

    Returns:
        Dictionary mapping service names to full URLs
    """
    return {
        name: f"{base_url}{path}"
        for name, path in MCP_SERVICE_PATHS.items()
    }
