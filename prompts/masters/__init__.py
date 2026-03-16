"""
投资大师 Persona 注册表

每位大师有独立的 system prompt、名称和头像。
"""

from prompts.masters.buffett import BUFFETT_PROMPT
from prompts.masters.lynch import LYNCH_PROMPT
from prompts.masters.soros import SOROS_PROMPT
from prompts.masters.livermore import LIVERMORE_PROMPT

MASTER_REGISTRY: dict[str, dict] = {
    "buffett": {
        "id": "buffett",
        "name": "沃伦·巴菲特",
        "name_en": "Warren Buffett",
        "avatar": "figs/buffett.svg",
        "description": "价值投资之父，关注企业内在价值与安全边际",
        "prompt": BUFFETT_PROMPT,
    },
    "lynch": {
        "id": "lynch",
        "name": "彼得·林奇",
        "name_en": "Peter Lynch",
        "avatar": "figs/lynch.svg",
        "description": "成长股猎手，PEG估值与十倍股发现者",
        "prompt": LYNCH_PROMPT,
    },
    "soros": {
        "id": "soros",
        "name": "乔治·索罗斯",
        "name_en": "George Soros",
        "avatar": "figs/soros.svg",
        "description": "宏观对冲大师，反身性理论创立者",
        "prompt": SOROS_PROMPT,
    },
    "livermore": {
        "id": "livermore",
        "name": "杰西·利弗莫尔",
        "name_en": "Jesse Livermore",
        "avatar": "figs/livermore.svg",
        "description": "华尔街传奇投机客，趋势交易与资金管理先驱",
        "prompt": LIVERMORE_PROMPT,
    },
}
