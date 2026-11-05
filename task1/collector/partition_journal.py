"""将 JSONL 日志写入后台分片存储，保留原有顺序和记录内容。"""

import json
import time

from partition_store import PartitionStore


class PartitionJournal:
    """日志写入适配器：write 每次接收一条完整 JSON 记录。"""

    def __init__(self, root, *, store=None, batch_size=500, flush_interval=10, queue_size=10000,
                 file_format='csv'):
        self.store = store if store is not None else PartitionStore(
            root, batch_size=batch_size, flush_interval=flush_interval, queue_size=queue_size,
            file_format=file_format)
        self.expected = 1

    def write(self, text):
        row = json.loads(text)
        if not isinstance(row, dict) or row.get('journal_seq') != self.expected:
            raise ValueError('日志顺序编号不连续')
        kind = row['type']
        stamp_ns = row.get('received_ns', row.get('observed_ns', row.get('checkpoint_ns')))
        if stamp_ns is None:
            stamp_ns = time.time_ns()
        if type(stamp_ns) is not int:
            raise ValueError('日志时间必须是整数纳秒')
        sequence = self.store.submit(kind, stamp_ns // 1_000_000, row)
        if sequence != self.expected:
            raise ValueError('日志编号与存储编号不一致')
        self.expected += 1
        return len(text)

    def close(self):
        self.store.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False
