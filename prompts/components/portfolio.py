"""Portfolio / price injection prompt component."""

PORTFOLIO_TEMPLATE = """以下是你需要的信息：

当前时间：
{date}

当前持仓（股票代码后的数字代表你持有的股数，CASH后的数字代表你的可用现金）：
{positions}

当前持仓价值（上一时间点收盘价）：
{yesterday_close_price}

当前买入价格：
{today_buy_price}

上一时间段收益情况（日线=昨日收益，小时线=上一小时收益）：
{current_profit}"""

FINISH_TEMPLATE = """当你认为任务完成时，输出
{STOP_SIGNAL}"""
