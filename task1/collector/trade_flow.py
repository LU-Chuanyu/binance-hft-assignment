"""协调实时聚合成交 ID 检查与缺口补查；调用者负责调度和持久化。

observe_live 不做网络请求。repair_missing 会执行阻塞式 REST 查询，必须在
独立的处理线程/任务中运行，不可在 WebSocket 接收线程中调用。
"""

import threading

from aggtrade_backfill import BackfillError, backfill_range
from aggtrade_check import AggTradeTracker
from api_client import RateLimitWait


class TradeFlow:
    def __init__(self, fetch_page):
        self.fetch_page = fetch_page
        self.tracker = AggTradeTracker()
        self.lock = threading.Lock()
        self.repair_lock = threading.Lock()
        self.older_seen = set()

    def observe_live(self, event, received_ns):
        """处理线程按接收顺序调用；返回判断、记录及当前缺失范围。"""
        with self.lock:
            status = self.tracker.observe(event)
            if status == 'older_than_baseline':
                include = event['a'] not in self.older_seen
                self.older_seen.add(event['a'])
            else:
                include = status != 'duplicate'
            return {'status': status, 'include': include,
                    'record': {'data': event, 'source': 'live', 'received_ns': received_ns,
                               'estimated_received_ns': None},
                    'missing_ranges': self.tracker.missing_ranges.copy()}

    def repair_missing(self, *, typical_delay_ms=None, clock_offset_ms=None,
                       page_size=1000, max_pages=10):
        """同步补查一轮；不持锁等待网络。未解决范围和错误显式返回。"""
        with self.repair_lock:
            queried = self.query_missing(typical_delay_ms=typical_delay_ms,
                clock_offset_ms=clock_offset_ms, page_size=page_size, max_pages=max_pages)
            return self.apply_repair(queried)

    def query_missing(self, *, typical_delay_ms=None, clock_offset_ms=None,
                      page_size=1000, max_pages=10):
        """后台查询当前缺口，只返回数据；由采集线程按日志顺序合并。"""
        with self.lock:
            ranges = self.tracker.missing_ranges.copy()
        records = []
        errors = []
        for start, end in ranges:
            try:
                result = backfill_range(start, end, self.fetch_page,
                    typical_delay_ms=typical_delay_ms, clock_offset_ms=clock_offset_ms,
                    page_size=page_size, max_pages=max_pages)
            except BackfillError as exc:
                result = exc.partial_result or {'records': []}
                errors.append({'message': str(exc), 'status': exc.status,
                               'retry_after': exc.retry_after,
                               'local_limit_wait': isinstance(exc.__cause__, RateLimitWait)})
            records.extend(result['records'])
            if errors:
                break
        return {'records': records, 'errors': errors}

    def apply_repair(self, queried):
        """仅接纳仍缺失的成交，实时先到或重复的结果不再写一次。"""
        with self.lock:
            recovered = [record for record in queried['records']
                         if self.tracker.observe(record['data']) == 'late']
            return {'records': recovered, 'missing_ranges': self.tracker.missing_ranges.copy(),
                    'errors': queried['errors']}
