"""按一次写入的全局顺序恢复分片日志，供现有 journal_replay 使用。"""

import argparse
import csv
from datetime import datetime, timezone
import gzip
import heapq
import json
from pathlib import Path
import re
import sys

from journal_replay import replay
from partition_store import _COLUMNS, _load_parquet, _parquet_schema, StorageError


_NAME = re.compile(r'([0-9a-f]{32})-([0-9]{20})-([0-9a-f]{32})(?:\.csv\.gz|\.parquet)\Z')


def _shards(root):
    root = Path(root)
    result = []
    for path in root.rglob('*'):
        if not path.name.endswith(('.csv.gz', '.parquet')):
            continue
        match = _NAME.fullmatch(path.name)
        parts = path.relative_to(root).parts
        if match is None or len(parts) != 4:
            raise ValueError(f'分片路径或文件名不合法：{path}')
        run_id, first, _ = match.groups()
        result.append((run_id, int(first), path, parts[0], parts[1], parts[2]))
    return result


def list_runs(root):
    """发现已经发布的分片所属 run；未发布的 .tmp 不在结果中。"""
    return sorted({item[0] for item in _shards(root)})


def _ordered_run_shards(shards, run_id):
    return sorted((shard for shard in shards if shard[0] == run_id), key=lambda shard: (shard[1], str(shard[2])))


def shard_paths(root, run_id):
    """按首编号列出指定 run 的已发布分片。"""
    return [shard[2] for shard in _ordered_run_shards(_shards(root), run_id)]


def _read_rows(path):
    if path.suffix == '.parquet':
        arrow, parquet = _load_parquet()
        try:
            with parquet.ParquetFile(path) as file:
                if not file.schema_arrow.equals(_parquet_schema(arrow)):
                    raise ValueError('列名或类型不符合 Parquet 分片格式')
                for batch in file.iter_batches(batch_size=128, use_threads=False):
                    for row in batch.to_pylist():
                        yield row['sequence'], row['timestamp_ms'], row['body_json']
        except arrow.ArrowException as exc:
            raise ValueError(str(exc)) from exc
        return

    # 完整快照或在线对账记录可以超过 CSV 默认的 128 KiB 单字段上限。
    # 文件仍逐行读取，不在这里按该上限预分配内存。
    csv.field_size_limit(sys.maxsize)
    with gzip.open(path, 'rt', encoding='utf-8', newline='') as file:
        reader = csv.DictReader(file)
        if reader.fieldnames != list(_COLUMNS):
            raise ValueError('列名不符合分片格式')
        for row in reader:
            if None in row:
                raise ValueError('记录列数不符合分片格式')
            yield int(row['sequence']), int(row['timestamp_ms']), row['body_json']


def _read_shard(shard, *, market=False):
    _, first, path, kind, day, hour = shard
    previous = None
    kind_field = 'kind' if market else 'type'
    try:
        for sequence, timestamp_ms, body_json in _read_rows(path):
            body = json.loads(body_json)
            if sequence < 1 or (previous is not None and sequence <= previous):
                raise ValueError('分片内编号未递增')
            if previous is None and sequence != first:
                raise ValueError('文件名的起始编号与内容不一致')
            if not isinstance(body, dict) or body.get(kind_field) != kind:
                raise ValueError('记录类型与分片不一致')
            if not market and (type(body.get('journal_seq')) is not int or body['journal_seq'] != sequence):
                raise ValueError('日志编号与分片不一致')
            stamp = datetime.fromtimestamp(timestamp_ms // 1000, tz=timezone.utc)
            if stamp.strftime('%Y-%m-%d') != day or stamp.strftime('%H') != hour:
                raise ValueError('记录时间与 UTC 日期小时分区不一致')
            previous = sequence
            yield sequence, timestamp_ms, body
        if previous is None:
            raise ValueError('分片为空')
    except (OSError, EOFError, UnicodeError, ValueError, TypeError, KeyError, OverflowError,
            csv.Error, StorageError) as exc:
        scope = 'market ' if market else ''
        raise ValueError(f'读取{scope}分片 {path} 失败：{exc}') from exc


def iter_journal(root, *, run_id=None):
    """重组一个已完成 run 的 JSONL 行，检查编号、分区、结束标记。

    流按文件名首编号排序：先放入一片，再输出编号小于下一片首编号的行。
    临时文件不会被读取；没有结束标记的 run 一律不能视为完整。
    """
    shards = _shards(root)
    runs = sorted({item[0] for item in shards})
    if run_id is None:
        if not runs:
            raise ValueError('找不到已发布分片')
        if len(runs) != 1:
            raise ValueError('目录中存在多个 run，请指定 run_id')
        run_id = runs[0]
    if run_id not in runs:
        raise ValueError(f'找不到指定 run：{run_id}')
    shards = _ordered_run_shards(shards, run_id)
    heap = []
    expected = 1
    ended = False

    def take():
        nonlocal expected, ended
        sequence, _, body = heapq.heappop(heap)
        if sequence != expected:
            raise ValueError(f'全局编号不连续、缺失或重复：期望 {expected}，实际 {sequence}')
        if ended:
            raise ValueError('结束记录之后还有数据')
        expected += 1
        ended = body['type'] == 'end'
        return json.dumps(body, ensure_ascii=False) + '\n'

    tie_breaker = 0
    for shard in shards:
        while heap and heap[0][0] < shard[1]:
            yield take()
        for sequence, _, body in _read_shard(shard):
            heapq.heappush(heap, (sequence, tie_breaker, body))
            tie_breaker += 1
    while heap:
        yield take()
    if not ended:
        raise ValueError('缺少结束记录，不能确认尾部分片完整')


def main():
    parser = argparse.ArgumentParser(description='读取一次分片日志并进行离线盘口回放')
    parser.add_argument('root', help='PartitionStore 根目录')
    parser.add_argument('--run-id', help='根目录有多次采集时指定 run_id')
    args = parser.parse_args()
    result = replay(iter_journal(args.root, run_id=args.run_id))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
