"""按成交 ID 补查现货聚合成交；估计时间与真实接收时间分开保存。

此模块不调度后台任务，也不替代整个采集器共享的请求额度管理。
"""

import json
from pathlib import Path
import ssl
import sys
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen


class BackfillError(Exception):
    """请求或响应异常；partial_result 保存此前可用结果及未解决范围。"""

    def __init__(self, message, *, partial_result=None, status=None, retry_after=None):
        super().__init__(message)
        self.partial_result = partial_result
        self.status = status
        self.retry_after = retry_after


def fetch_aggtrade_page(from_id, limit, *, symbol='BTCUSDT', timeout=10):
    """请求一页公开行情；429/418 交给上层按 Retry-After 等待，绝不立即重试。"""
    query = urlencode({'symbol': symbol, 'fromId': from_id, 'limit': limit})
    ca = Path('/etc/ssl/cert.pem')
    context = ssl.create_default_context(cafile=str(ca) if sys.platform == 'darwin' and ca.is_file() else None)
    try:
        with urlopen('https://api.binance.com/api/v3/aggTrades?' + query,
                     timeout=timeout, context=context) as response:
            return json.load(response)
    except HTTPError as exc:
        retry_after = exc.headers.get('Retry-After')
        raise BackfillError(f'成交补查 HTTP {exc.code}; Retry-After={retry_after}',
                            status=exc.code, retry_after=retry_after) from exc


def backfill_range(start_id, end_id, fetch_page, *, typical_delay_ms=None,
                   clock_offset_ms=None, page_size=1000, max_pages=10):
    """补查闭区间 [start_id, end_id]，返回补回记录及仍缺的闭区间。

    fetch_page(from_id, limit) 是可注入的查询函数。若同时提供典型延迟和
    本机减交易所时钟偏差，估计本机接收时间；这不是实际实时接收时间。
    不完整的页面/页数上限均不伪称补齐。请求异常保留已查到的结果。
    """
    if (type(start_id) is not int or type(end_id) is not int or start_id < 0
            or end_id < start_id or type(page_size) is not int or not 1 <= page_size <= 1000
            or type(max_pages) is not int or max_pages < 1):
        raise ValueError('成交编号范围、每页数量或页数上限无效')
    for name, value in [('typical_delay_ms', typical_delay_ms), ('clock_offset_ms', clock_offset_ms)]:
        if value is not None and type(value) is not int:
            raise ValueError(f'{name} 必须是整数毫秒或 None')
    if typical_delay_ms is not None and typical_delay_ms < 0:
        raise ValueError('典型延迟不能为负')

    records = []
    missing = []
    cursor = start_id

    def partial():
        remaining = missing.copy()
        if cursor <= end_id:
            remaining.append((cursor, end_id))
        return {'records': records.copy(), 'missing_ranges': remaining, 'complete': not remaining}

    for _ in range(max_pages):
        if cursor > end_id:
            break
        limit = min(page_size, end_id - cursor + 1)
        try:
            page = fetch_page(cursor, limit)
        except Exception as exc:
            if isinstance(exc, BackfillError):
                exc.partial_result = partial()
                raise
            raise BackfillError(f'成交补查请求失败：{exc}', partial_result=partial(),
                                status=getattr(exc, 'status', getattr(exc, 'code', None)),
                                retry_after=getattr(exc, 'retry_after', getattr(exc, 'remaining', None))) from exc
        if not isinstance(page, list) or len(page) > limit:
            raise BackfillError('成交补查响应不是合法页面', partial_result=partial())
        if not page:
            break

        for row in page:
            if (not isinstance(row, dict) or type(row.get('a')) is not int
                    or type(row.get('T')) is not int or not isinstance(row.get('p'), str)
                    or not isinstance(row.get('q'), str) or type(row.get('m')) is not bool
                    or type(row.get('f')) is not int or type(row.get('l')) is not int):
                raise BackfillError('成交补查响应缺少或含有无效字段', partial_result=partial())
            trade_id = row['a']
            if trade_id < cursor:
                raise BackfillError('成交补查页面编号重复或倒序', partial_result=partial())
            if trade_id > end_id:
                break
            if trade_id > cursor:
                missing.append((cursor, trade_id - 1))
            estimate = None
            if typical_delay_ms is not None and clock_offset_ms is not None:
                estimate = (row['T'] + typical_delay_ms + clock_offset_ms) * 1_000_000
            records.append({'data': row, 'source': 'rest_backfill', 'received_ns': None,
                            'estimated_received_ns': estimate,
                            'estimate_typical_delay_ms': typical_delay_ms,
                            'estimate_clock_offset_ms': clock_offset_ms})
            cursor = trade_id + 1
        else:
            continue
        break
    return partial()
