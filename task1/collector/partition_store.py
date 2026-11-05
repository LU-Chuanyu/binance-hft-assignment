"""后台分片存储，按记录类型和 UTC 日期、小时分区。"""

import csv
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import queue
import re
import tempfile
import threading
import time
import uuid


_COLUMNS = ('sequence', 'timestamp_ms', 'body_json')


class StorageError(RuntimeError):
    """存储初始化、入队或后台写入失败。"""


def _load_parquet():
    try:
        import pyarrow as arrow
        import pyarrow.parquet as parquet
    except Exception as exc:
        # 动态库或依赖损坏时，导入也可能抛出 ImportError 以外的异常。
        raise StorageError(f'无法加载 pyarrow Parquet 支持：{exc}') from exc
    return arrow, parquet


def _parquet_schema(arrow):
    return arrow.schema(zip(_COLUMNS, (arrow.int64(), arrow.int64(), arrow.string())))


class PartitionStore:
    """后台分批写入 gzip CSV 或 Parquet。auto 在启动时选择一次格式。"""

    _STOP = object()

    def __init__(self, root, *, batch_size=500, flush_interval=5, queue_size=10000, file_format='csv'):
        if batch_size < 1 or flush_interval <= 0 or queue_size < 1:
            raise ValueError('批大小、间隔和队列容量必须为正')
        if file_format not in ('auto', 'csv', 'parquet'):
            raise ValueError('file_format 必须是 auto、csv 或 parquet')
        self._parquet = None
        if file_format != 'csv':
            try:
                arrow, parquet = _load_parquet()
                if not arrow.Codec.is_available('zstd'):
                    raise StorageError('pyarrow 缺少 Parquet 所需的 ZSTD 编码支持')
            except StorageError:
                if file_format == 'parquet':
                    raise
            else:
                self._parquet = (arrow, parquet)
        self.root = Path(root)
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.queue = queue.Queue(maxsize=queue_size)
        self._lock = threading.Lock()
        self._closed = False
        self._error = None
        self._sequence = 0
        self._run_id = uuid.uuid4().hex
        self._thread = threading.Thread(target=self._run, name='partition-writer', daemon=True)
        self._thread.start()

    @property
    def file_format(self):
        return 'parquet' if self._parquet is not None else 'csv'

    def submit(self, kind, timestamp_ms, body):
        """非阻塞入队；满或后台失败时抛 StorageError，不能当作已保存。"""
        if not isinstance(kind, str) or not re.fullmatch(r'[A-Za-z0-9_-]+', kind):
            raise ValueError('记录类型只能包含英文字母、数字、下划线或横线')
        if type(timestamp_ms) is not int:
            raise ValueError('timestamp_ms 必须是整数')
        # 此处固定快照，调用方之后修改 body 不会改变已提交的内容。
        body_json = json.dumps(body, ensure_ascii=False, allow_nan=False, separators=(',', ':'))
        with self._lock:
            if self._closed:
                raise StorageError('存储已关闭')
            if self._error is not None:
                raise StorageError(f'后台写盘失败：{self._error}') from self._error
            sequence = self._sequence + 1
            try:
                self.queue.put_nowait((sequence, kind, timestamp_ms, body_json))
            except queue.Full as exc:
                raise StorageError('后台写盘队列已满') from exc
            self._sequence = sequence
            return sequence

    def _run(self):
        batch = []
        first_at = None
        try:
            while True:
                timeout = None if first_at is None else max(0, first_at + self.flush_interval - time.monotonic())
                try:
                    item = self.queue.get(timeout=timeout)
                except queue.Empty:
                    self._write_batch(batch)
                    batch = []
                    first_at = None
                    continue
                if item is self._STOP:
                    if batch:
                        self._write_batch(batch)
                    return
                if not batch:
                    first_at = time.monotonic()
                batch.append(item)
                if len(batch) >= self.batch_size:
                    self._write_batch(batch)
                    batch = []
                    first_at = None
        except Exception as exc:
            self._error = exc

    def _write_batch(self, batch):
        groups = {}
        for sequence, kind, timestamp_ms, body_json in batch:
            instant = datetime.fromtimestamp(timestamp_ms // 1000, tz=timezone.utc)
            key = (kind, instant.strftime('%Y-%m-%d'), instant.strftime('%H'))
            groups.setdefault(key, []).append((sequence, timestamp_ms, body_json))
        for (kind, day, hour), rows in groups.items():
            directory = self.root / kind / day / hour
            directory.mkdir(parents=True, exist_ok=True)
            suffix = '.parquet' if self._parquet is not None else '.csv.gz'
            target = directory / f'{self._run_id}-{rows[0][0]:020d}-{uuid.uuid4().hex}{suffix}'
            fd, name = tempfile.mkstemp(prefix='.part-', suffix='.tmp', dir=directory)
            os.close(fd)
            try:
                if self._parquet is None:
                    with gzip.open(name, 'wt', encoding='utf-8', newline='') as stream:
                        writer = csv.writer(stream)
                        writer.writerow(_COLUMNS)
                        writer.writerows(rows)
                else:
                    arrow, parquet = self._parquet
                    schema = _parquet_schema(arrow)
                    column_values = zip(*rows)
                    columns = [arrow.array(values, type=field.type) for values, field in zip(column_values, schema)]
                    parquet.write_table(arrow.Table.from_arrays(columns, schema=schema), name, compression='zstd')
                # 硬链接创建目标是原子的，并会在目标已存在时失败，不覆盖历史文件。
                # 读者只查看正式后缀，临时文件始终与目标位于同一目录。
                os.link(name, target)
            finally:
                os.unlink(name)

    def close(self):
        """停止接收新记录、写完待处理数据并等待后台线程。"""
        with self._lock:
            already_closed = self._closed
            self._closed = True
        # 队列满时允许写线程先消化；若线程失败则立即结束等待。
        while not already_closed and self._thread.is_alive():
            try:
                self.queue.put(self._STOP, timeout=0.1)
                break
            except queue.Full:
                continue
        self._thread.join()
        if self._error is not None:
            raise StorageError(f'后台写盘失败：{self._error}') from self._error
