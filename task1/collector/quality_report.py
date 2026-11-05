"""对已完成的教学 JSONL 记录做基础统计；不替代作业完整质量报告。"""

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import argparse
import json
from pathlib import Path

from clock_monitor import estimate_delay_ms, summarize_delays
from clock_samples import ClockSamples
from journal_replay import replay
from partition_replay import iter_journal, list_runs, shard_paths
from runtime_stats import SECOND_NS, STREAMS


@dataclass(frozen=True)
class ClockWindow:
    """调用方确认适用区间的校时结果；RTT 门槛须显式指定，仍有测量误差。"""
    start_ns: int
    end_ns: int
    offset_ns: int  # 本机时钟减交易所时钟
    rtt_ns: int
    max_rtt_ns: int

    def __post_init__(self):
        values = (self.start_ns, self.end_ns, self.offset_ns, self.rtt_ns, self.max_rtt_ns)
        if any(type(value) is not int for value in values):
            raise ValueError('校时窗口和 RTT 须为整数')
        if self.start_ns > self.end_ns or self.rtt_ns < 0 or self.max_rtt_ns < 0:
            raise ValueError('校时窗口或 RTT 不合法')
        if self.rtt_ns > self.max_rtt_ns:
            raise ValueError('校时往返时间超过调用方指定的门槛')


def iter_source_lines(path, *, run_id=None):
    """重读 JSONL 或压缩分片日志，不把所有记录一次性加载到内存。"""
    path = Path(path)
    if path.is_dir():
        journal_root = path / 'journal' if (path / 'journal').is_dir() else path
        yield from iter_journal(journal_root, run_id=run_id)
    else:
        with path.open(encoding='utf-8') as file:
            yield from file


def _disk_usage(files):
    stats = [file.stat() for file in files]
    return (sum(stat.st_size for stat in stats),
            sum(stat.st_blocks * 512 for stat in stats)
            if all(hasattr(stat, 'st_blocks') for stat in stats) else None)


def summarize_file(path, *, clock=None, run_id=None, include_samples=False):
    """校验已结束日志再统计；clock 未提供时不计算事件时间差。"""
    path = Path(path)
    if clock is not None and not isinstance(clock, ClockWindow):
        raise TypeError('clock 须为 ClockWindow 或 None')
    # 先验证日志动作和结束检查点。该检查不证明交易所没有漏发或采集未漏收。
    replayed = replay(iter_source_lines(path, run_id=run_id))

    minute_counts = Counter()
    minute_by_kind = defaultdict(Counter)
    deltas = {'depth': [], 'aggTrade': [], 'bookTicker': []}
    stream_kind = {'btcusdt@depth@100ms': 'depth', 'btcusdt@aggTrade': 'aggTrade',
                   'btcusdt@bookTicker': 'bookTicker'}
    first_monotonic = last_monotonic = None
    types = Counter()
    calibration = ClockSamples()
    clock_rtt_ms = []
    clock_connection_counts = Counter()
    checkpoint_gap_reasons = Counter()
    recorded_duration = None
    runtime_windows = Counter()
    runtime_elapsed_ns = 0
    runtime_counts = dict.fromkeys(STREAMS, 0)
    runtime_delay_counts = dict.fromkeys(STREAMS, 0)
    backfill_estimated = 0
    for line in iter_source_lines(path, run_id=run_id):
        row = json.loads(line)
        types[row['type']] += 1
        if row['type'] == 'end':
            start, stop = row.get('started_monotonic_ns'), row.get('stopped_monotonic_ns')
            if type(start) is int and type(stop) is int and start <= stop:
                recorded_duration = (stop - start) / 1_000_000_000
        if row['type'] == 'checkpoint_gap':
            checkpoint_gap_reasons[row['reason']] += row['missed_count']
        if row['type'] == 'clock_sample':
            calibration.observe(row)
            if row.get('status') == 'ok' and type(row.get('rtt_ns')) is int:
                clock_rtt_ms.append(row['rtt_ns'] / 1_000_000)
                reused = row.get('connection_reused')
                if type(reused) is bool:
                    clock_connection_counts['reused' if reused else 'fresh'] += 1
        if row['type'] == 'runtime_stats':
            runtime_windows['partial' if row['partial'] else 'full'] += 1
            runtime_elapsed_ns += row['window_end_monotonic_ns'] - row['window_start_monotonic_ns']
            for kind, stream in row['streams'].items():
                runtime_counts[kind] += stream['count']
                runtime_delay_counts[kind] += stream['delay']['count']
        if row['type'] == 'trade_backfill' and type(row['record'].get('estimated_received_ns')) is int:
            backfill_estimated += 1
        if row['type'] != 'raw':
            continue
        message = json.loads(row['raw_text'])
        kind = stream_kind[message['stream']]
        received = row['received_ns']
        minute = datetime.fromtimestamp(received // 1_000_000_000, timezone.utc).strftime('%Y-%m-%dT%H:%MZ')
        minute_counts[minute] += 1
        minute_by_kind[minute][kind] += 1
        if first_monotonic is None:
            first_monotonic = row['monotonic_ns']
        last_monotonic = row['monotonic_ns']
        event_ms = message['data'].get('E')
        if kind == 'bookTicker':
            event_ms = None
        if clock and clock.start_ns <= received <= clock.end_ns and type(event_ms) is int:
            deltas[kind].append(estimate_delay_ms(received, event_ms, clock.offset_ns))
        elif clock is None:
            delay = calibration.delay(received, event_ms, row['monotonic_ns'])
            if delay is not None:
                deltas[kind].append(delay)

    elapsed_ns = None if first_monotonic is None else last_monotonic - first_monotonic
    if elapsed_ns is not None and elapsed_ns < 0:
        raise ValueError('接收单调时钟倒退')
    market_bytes = market_allocated = None
    if path.is_dir():
        journal_root = path / 'journal' if (path / 'journal').is_dir() else path
        runs = list_runs(journal_root)
        chosen = run_id if run_id is not None else runs[0]
        journal_paths = shard_paths(journal_root, chosen)
        file_bytes, allocated = _disk_usage(journal_paths)
        formats = sorted({'Parquet' if shard.suffix == '.parquet' else 'gzip CSV'
                          for shard in journal_paths})
        source_type = f"压缩分片日志（{' / '.join(formats)}）"
        if journal_root != path and len(runs) == 1 and (path / 'market').is_dir():
            market_root = path / 'market'
            market_runs = list_runs(market_root)
            if len(market_runs) == 1:
                market_bytes, market_allocated = _disk_usage(shard_paths(market_root, market_runs[0]))
    else:
        file_bytes, allocated = _disk_usage([path])
        source_type = 'JSONL 日志'
    result = {
        'file': str(path),
        'file_bytes': file_bytes,
        'allocated_disk_bytes': allocated,
        'market_file_bytes': market_bytes,
        'market_allocated_disk_bytes': market_allocated,
        'recorded_duration_seconds': recorded_duration,
        'source_type': source_type,
        'record_types': dict(types),
        'runtime_stats': {'full_windows': runtime_windows['full'],
                          'partial_windows': runtime_windows['partial'],
                          'elapsed_seconds': runtime_elapsed_ns / SECOND_NS,
                          'counts': runtime_counts, 'delay_sample_counts': runtime_delay_counts}
                         if runtime_windows else None,
        'backfill_estimated_count': backfill_estimated,
        'clock_sample_count': types['clock_sample'],
        'usable_clock_samples': calibration.valid_samples,
        'clock_jumps': calibration.jumps,
        'clock_rtt': summarize_delays(clock_rtt_ms),
        'clock_connection_counts': dict(clock_connection_counts),
        'counts': replayed['counts'],
        'minute_counts_utc': dict(sorted(minute_counts.items())),
        'minute_counts_by_kind_utc': {minute: dict(counts) for minute, counts in sorted(minute_by_kind.items())},
        'observed_elapsed_seconds': None if elapsed_ns is None else elapsed_ns / 1_000_000_000,
        'observed_rate_per_second': ({kind: count * 1_000_000_000 / elapsed_ns
                                      for kind, count in replayed['counts'].items()}
                                     if elapsed_ns else None),
        'gap_count': replayed['gap_count'],
        'reset_count': replayed['reset_count'],
        'applied_depth': replayed['applied_count'],
        'ignored_depth': replayed['ignored_count'],
        'pending_depth': replayed['pending_depth_count'],
        'checkpoints_checked': replayed['checkpoints_checked'],
        'checkpoints_passed': replayed['checkpoints_passed'],
        'checkpoint_gaps': replayed['checkpoint_gaps'],
        'checkpoint_gap_records': types['checkpoint_gap'],
        'checkpoint_gap_reasons': dict(checkpoint_gap_reasons),
        'validation_counts': replayed['validation_counts'],
        'last_update_id': replayed['last_update_id'],
        'latency': {kind: summarize_delays(values) for kind, values in deltas.items()},
        'bookTicker_latency_note': '消息缺少交易所事件时间 E，无法使用此方法估算时间差。',
        'time_difference_note': (('未提供校时窗口，未计算事件时间差。' if clock is None and not types['clock_sample'] else
                                  ('使用日志内 RTT ≤1000ms、最多有效60秒且时钟未跳变的校时样本估算；' if clock is None else
                                   '按调用方提供的校时窗口估算；') +
                                  '仍包含发布、传输和本机处理等影响，不等同纯网络延迟；无有效校时或无 E 的数据未计算。')),
        'scope_note': ('这是教学日志基础统计，不是完整质量报告；尚需逐档校验、'
                       '补查与断线统计、校时覆盖、图表及足够长的采集。'),
    }
    if include_samples:
        result['latency_samples_ms'] = deltas
    return result


def main():
    parser = argparse.ArgumentParser(description='汇总一份已完成的教学日志（UTC 按分钟计数）')
    parser.add_argument('path')
    args = parser.parse_args()
    # CLI 不猜测校时偏差，因此只输出可确定的基础统计。
    print(json.dumps(summarize_file(args.path), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
