"""检查持仓数据与价格数据的匹配情况"""
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def check_hourly_data():
    """检查小时线数据"""
    logger.info('=== 完整数据验证 (小时线) ===')

    # Load position data
    position_file = Path('../agent_data_astock_hour/gemini-2.5-flash-astock-hour/position/position.jsonl')
    positions_by_date = {}
    with open(position_file, 'r') as f:
        for line in f:
            data = json.loads(line)
            date = data.get('date')
            positions = data.get('positions', {})
            positions_by_date[date] = positions

    # Load price data
    stock_data = {}
    with open('merged_hourly.jsonl', 'r') as f:
        for line in f:
            data = json.loads(line)
            symbol = data.get('Meta Data', {}).get('2. Symbol')
            ts = data.get('Time Series (60min)', {})
            stock_data[symbol] = set(ts.keys())

    # Check each position date
    missing_data = []
    for pos_date, positions in positions_by_date.items():
        holdings = {k: v for k, v in positions.items() if k != 'CASH' and v > 0}
        for stock in holdings:
            if stock not in stock_data:
                missing_data.append((pos_date, stock, 'stock_not_found'))
            else:
                # Check if the date format matches
                if pos_date not in stock_data[stock]:
                    # Try to find similar dates
                    similar = [d for d in stock_data[stock] if d.startswith(pos_date[:10])]
                    missing_data.append((pos_date, stock, f'date_mismatch, similar: {similar[:3]}'))

    if missing_data:
        logger.warning('发现 %d 条数据缺失/不匹配:', len(missing_data))
        for date, stock, reason in missing_data[:50]:
            logger.warning('  %s | %s | %s', date, stock, reason)
    else:
        logger.info('所有持仓数据与价格数据匹配正常')

    # Show sample date formats
    sample_stock = list(stock_data.keys())[0]
    logger.info('持仓日期格式: %s', list(positions_by_date.keys())[:3])
    logger.info('价格日期格式 (%s): %s', sample_stock, sorted(list(stock_data[sample_stock]))[:3])


def check_daily_data():
    """检查日线数据"""
    logger.info('=== 完整数据验证 (日线) ===')

    # Load position data
    position_file = Path('../agent_data_astock/gemini-2.5-flash/position/position.jsonl')
    positions_by_date = {}
    with open(position_file, 'r') as f:
        for line in f:
            data = json.loads(line)
            date = data.get('date')
            positions = data.get('positions', {})
            positions_by_date[date] = positions

    # Load price data
    stock_data = {}
    with open('merged.jsonl', 'r') as f:
        for line in f:
            data = json.loads(line)
            symbol = data.get('Meta Data', {}).get('2. Symbol')
            ts = data.get('Time Series (Daily)', {})
            stock_data[symbol] = set(ts.keys())

    # Check each position date
    missing_data = []
    for pos_date, positions in positions_by_date.items():
        holdings = {k: v for k, v in positions.items() if k != 'CASH' and v > 0}
        for stock in holdings:
            if stock not in stock_data:
                missing_data.append((pos_date, stock, 'stock_not_found'))
            else:
                # Check if the date format matches
                if pos_date not in stock_data[stock]:
                    # Try to find similar dates
                    similar = [d for d in stock_data[stock] if d.startswith(pos_date[:7])]
                    missing_data.append((pos_date, stock, f'date_mismatch, similar: {similar[:3]}'))

    if missing_data:
        logger.warning('发现 %d 条数据缺失/不匹配:', len(missing_data))
        for date, stock, reason in missing_data[:50]:
            logger.warning('  %s | %s | %s', date, stock, reason)
    else:
        logger.info('所有持仓数据与价格数据匹配正常')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    check_hourly_data()
    check_daily_data()
