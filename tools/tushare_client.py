"""
Tushare API 客户端单例

提供统一的 tushare pro_api 实例，避免重复初始化。
使用自定义 API 端点以确保国内网络可用。
"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

_pro_instance = None

TUSHARE_TOKEN = os.environ.get(
    "TUSHARE_TOKEN",
    "3d5b042fdf594f80b46afd4f82c64d2398e6b2d6d04a4c1bba32468623d0",
)
TUSHARE_API_URL = os.environ.get(
    "TUSHARE_API_URL",
    "http://lianghua.nanyangqiankun.top",
)


def get_tushare_pro():
    """获取 tushare pro_api 单例"""
    global _pro_instance
    if _pro_instance is None:
        import tushare as ts

        _pro_instance = ts.pro_api(TUSHARE_TOKEN)
        _pro_instance._DataApi__token = TUSHARE_TOKEN
        _pro_instance._DataApi__http_url = TUSHARE_API_URL
        logger.info("Tushare pro_api initialized (url=%s)", TUSHARE_API_URL)
    return _pro_instance
