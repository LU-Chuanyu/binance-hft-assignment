"""核对同次采集的 journal 与 market 分片；不证明交易所上游没有漏数。

这是离线检查器，使用多重集合保存期望与实际记录，长时间采集可能消耗
较多内存。历史日志未记录补回任务的连接代次时，该字段无法完整核验。
"""

import argparse
from collections import Counter
import heapq
import json
from pathlib import Path

from journal_replay import replay
from market_projection import project_live, project_backfill
from market_stream import RawMessage, decode_message
from partition_replay import _ordered_run_shards, _read_shard, _shards, iter_journal


def _iter_market(root, run_id=None):
    shards = _shards(root)
    runs = sorted({row[0] for row in shards})
    if not runs and run_id is None:
        return
    if run_id is None:
        if len(runs) != 1:
            raise ValueError('market 目录存在多个 run，请指定 market_run_id')
        run_id = runs[0]
    if run_id not in runs:
        raise ValueError('找不到指定的 market run')
    shards = _ordered_run_shards(shards, run_id)
    heap = []
    expected, tie = 1, 0
    for shard in shards:
        while heap and heap[0][0] < shard[1]:
            seq, _, timestamp, body = heapq.heappop(heap)
            if seq != expected:
                raise ValueError(f'market 编号不连续：期望 {expected}，实际 {seq}')
            expected += 1
            yield timestamp, body
        for seq, timestamp, body in _read_shard(shard, market=True):
            heapq.heappush(heap, (seq, tie, timestamp, body))
            tie += 1
    while heap:
        seq, _, timestamp, body = heapq.heappop(heap)
        if seq != expected:
            raise ValueError(f'market 编号不连续：期望 {expected}，实际 {seq}')
        expected += 1
        yield timestamp, body


def _canonical(timestamp, body, unverified_ids):
    body = dict(body)
    if body.get('source') == 'rest_backfill' and body.get('a') in unverified_ids:
        body.pop('generation', None)
    return json.dumps({'timestamp_ms': timestamp, 'body': body}, ensure_ascii=False,
                      sort_keys=True, separators=(',', ':'), allow_nan=False)


def reconcile(root, *, journal_run_id=None, market_run_id=None):
    """核对一个完整日志及对应 market；恢复重连代次和实时成交去重动作。"""
    root = Path(root)
    if not (root / 'journal').is_dir():
        raise ValueError('采集目录缺少 journal 子目录')
    replay(iter_journal(root / 'journal', run_id=journal_run_id))
    expected_rows = []
    pending_trade = {}
    unverified_ids = set()
    for line in iter_journal(root / 'journal', run_id=journal_run_id):
        row = json.loads(line)
        if row['type'] == 'raw':
            generation = row.get('generation')
            if type(generation) is not int or generation < 1:
                raise ValueError('历史 raw 缺少连接代次，无法完整对账')
            message = RawMessage(row['receive_seq'], row['received_ns'], row['monotonic_ns'], row['raw_text'])
            event = decode_message(message)
            if event.kind == 'aggTrade':
                pending_trade[message.sequence] = (message, generation)
            else:
                expected_rows.extend((message.received_ns // 1_000_000, item)
                                     for item in project_live(message, generation=generation))
        elif row['type'] == 'trade_live':
            entry = pending_trade.pop(row['receive_seq'], None)
            if entry is None:
                raise ValueError('成交处理动作缺少原始消息')
            message, generation = entry
            if row['include']:
                expected_rows.extend((message.received_ns // 1_000_000, item)
                                     for item in project_live(message, generation=generation))
        elif row['type'] == 'trade_backfill':
            record = row['record']
            generation = row.get('origin_generation')
            if generation is None:
                unverified_ids.add(record['data']['a'])
                generation = 1  # 仅用于生成其他字段；校验时不比较此值。
            for item in project_backfill(record, symbol='BTCUSDT', generation=generation):
                item['partition_basis'] = 'exchange_trade_time_ms'
                expected_rows.append((item['T'], item))
    if pending_trade:
        raise ValueError('原始成交缺少对应处理动作，无法完整对账')

    expected = Counter(_canonical(ts, body, unverified_ids) for ts, body in expected_rows)
    actual = Counter(_canonical(ts, body, unverified_ids)
                     for ts, body in _iter_market(root / 'market', run_id=market_run_id))
    missing = expected - actual
    extra = actual - expected
    missing_count, extra_count = sum(missing.values()), sum(extra.values())
    status = 'mismatch' if missing_count or extra_count else 'partial' if unverified_ids else 'matched'
    return {'status': status, 'expected_count': sum(expected.values()), 'actual_count': sum(actual.values()),
            'missing_count': missing_count, 'extra_count': extra_count,
            'missing_examples': [json.loads(item) for item in list(missing)[:3]],
            'extra_examples': [json.loads(item) for item in list(extra)[:3]],
            'unverified_backfill_generation_count': len(unverified_ids),
            'scope': '仅核对日志可重建数据与 market 文件，不证明交易所上游完整'}


def main():
    parser = argparse.ArgumentParser(description='核对同次采集的 journal 与 market 分片')
    parser.add_argument('root', help='含 journal/ 和 market/ 的采集目录')
    parser.add_argument('--journal-run-id')
    parser.add_argument('--market-run-id')
    args = parser.parse_args()
    result = reconcile(args.root, journal_run_id=args.journal_run_id, market_run_id=args.market_run_id)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result['status'] != 'matched':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
