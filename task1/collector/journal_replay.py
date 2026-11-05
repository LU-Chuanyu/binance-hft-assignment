"""离线回放教学日志：原文 + 同一轮快照 + 按顺序记录的处理动作。"""

import argparse
from collections import deque
import json
import math

from aggtrade_check import AggTradeTracker
from book_sync import DepthGapError, DepthSynchronizer
from book_provenance import BookProvenance
from clock_estimates import clock_offset_alert
from clock_monitor import estimate_clock
from book_validation import compare_exchange_snapshot, compare_top20
from checkpoint_helpers import make_checkpoint
from market_stream import RawMessage, decode_message
from runtime_stats import validate_runtime_stats


def top_as_text(book):
    """保存/比较前一档时使用可写入 JSON 的字符串。"""
    levels = book.top_levels(1)
    return {side: [[str(price), str(quantity)] for price, quantity in levels[side]]
            for side in ('bids', 'asks')}


def valid_missing_ranges(value):
    """校验按升序排列的不重叠成交缺口闭区间。"""
    if not isinstance(value, list):
        return False
    previous_end = -1
    for interval in value:
        if (not isinstance(interval, list) or len(interval) != 2
                or any(type(number) is not int for number in interval)
                or interval[0] < 0 or interval[0] > interval[1]
                or interval[0] <= previous_end):
            return False
        previous_end = interval[1]
    return True


def replay(file):
    """按 journal_seq 复现盘口；尾部未处理 depth 会单独计数。"""
    expected_journal = 1
    expected_receive = 1
    pending = deque()
    sync = None
    provenance = None
    applied_count = ignored_count = 0
    gap_count = reset_count = 0
    checkpoints_checked = checkpoints_passed = 0
    checkpoint_gaps = 0
    needs_reset = False
    needs_bridge = False
    connection_open = None
    generation = 0
    last_raw_kind = None
    last_raw_data = None
    trade_ids = set()
    trade_tracker = AggTradeTracker()
    legacy_trade_tracker = AggTradeTracker()
    excluded_trade_ids = set()
    trade_raw_count = trade_action_count = 0
    last_trade_action_seq = None
    trade_errors = False
    trade_missing = []
    pending_retries = {}
    retry_failed = False
    validation_ids = set()
    validation_counts = {status: 0 for status in ('matched', 'mismatch', 'unverifiable', 'discarded', 'error')}
    validation_events = []
    counts = {'depth': 0, 'aggTrade': 0, 'bookTicker': 0}
    runtime_counts = dict.fromkeys(counts, 0)
    runtime_start = runtime_end = None
    runtime_partial = False
    last_clock_sample = None
    offset_alert_logged = False
    ended = False

    for line_number, line in enumerate(file, 1):
        try:
            row = json.loads(line)
            if not isinstance(row, dict) or row['journal_seq'] != expected_journal:
                raise ValueError('日志顺序编号不连续')
            if ended:
                raise ValueError('结束记录后还有数据')
            expected_journal += 1
            kind = row['type']

            if kind == 'checkpoint_gap':
                if (type(row.get('missed_count')) is not int or row['missed_count'] < 1
                        or row.get('reason') not in ('processing_delayed', 'unsynchronized')
                        or type(row.get('checkpoint_ns')) is not int):
                    raise ValueError('盘口摘要缺失记录无效')
                checkpoint_gaps += row['missed_count']

            elif kind == 'connection_open':
                if connection_open is True or row['generation'] != generation + 1:
                    raise ValueError('连接代次或状态不正确')
                generation = row['generation']
                connection_open = True

            elif kind == 'connection_lost':
                if connection_open is not True or row['generation'] != generation:
                    raise ValueError('断线事件与连接状态不一致')
                if pending:
                    raise ValueError('断线前还有未处理盘口消息')
                connection_open = False
                if sync is not None:
                    sync.valid = False
                    needs_reset = True

            elif kind == 'trade_backfill':
                record = row['record']
                origin = row.get('origin_generation')
                if origin is not None and (type(origin) is not int or not 1 <= origin <= generation):
                    raise ValueError('补回成交连接代次无效')
                if (record.get('source') != 'rest_backfill'
                        or record.get('received_ns') is not None):
                    raise ValueError('补回成交的来源或接收时间无效')
                if record.get('estimate_method') == 'nearby_live_trade_median':
                    delay_ns, offset_ns = record['estimate_typical_delay_ns'], record['estimate_clock_offset_ns']
                    estimate_ns, sample_ns = record['estimated_received_ns'], record['estimate_clock_sample_ns']
                    if (any(type(value) is not int for value in (delay_ns, offset_ns, estimate_ns, sample_ns))
                            or delay_ns < 0 or estimate_ns < sample_ns
                            or estimate_ns != record['data']['T'] * 1_000_000 + delay_ns + offset_ns
                            or type(record['estimate_sample_count']) is not int or record['estimate_sample_count'] < 1):
                        raise ValueError('补回成交的估计时间与依据不一致')
                trade_id = record['data']['a']
                if type(trade_id) is not int or trade_id in trade_ids:
                    raise ValueError('补回成交编号无效或重复')
                if trade_tracker.observe(record['data']) != 'late':
                    raise ValueError('补回成交没有对应的未解决缺口')
                trade_ids.add(trade_id)

            elif kind == 'trade_repair':
                if not isinstance(row['errors'], list) or not valid_missing_ranges(row['missing_ranges']):
                    raise ValueError('补查结果格式无效')
                if row['missing_ranges'] != [list(interval) for interval in trade_tracker.missing_ranges]:
                    raise ValueError('成交补查剩余缺口与独立重算结果不一致')
                trade_errors = trade_errors or bool(row['errors'])
                trade_missing = row['missing_ranges']

            elif kind == 'rest_retry':
                operation, origin, attempt = row['operation'], row['generation'], row['attempt']
                state, error = row['state'], row['error']
                if (operation not in ('snapshot', 'trade_backfill') or type(origin) is not int
                        or not 1 <= origin <= generation or type(attempt) is not int or attempt < 1
                        or type(row.get('observed_ns')) is not int
                        or state not in ('waiting', 'recovered', 'resolved_live', 'failed')
                        or (error is not None and (not isinstance(error, dict)
                            or not isinstance(error.get('message'), str)))):
                    raise ValueError('REST 重试记录无效')
                key = (operation, origin)
                previous = pending_retries.get(key)
                if state == 'waiting':
                    delay = error.get('retry_after') if error else None
                    if (type(delay) not in (int, float) or not math.isfinite(delay) or delay < 0
                            or attempt != (1 if previous is None else previous + 1)):
                        raise ValueError('REST 重试等待参数无效')
                    pending_retries[key] = attempt
                elif state == 'recovered':
                    if previous is None or attempt != previous + 1:
                        raise ValueError('REST 恢复记录缺少对应等待')
                    del pending_retries[key]
                elif state == 'resolved_live':
                    if operation != 'trade_backfill' or trade_tracker.missing_ranges:
                        raise ValueError('REST 补查仍有缺口')
                    pending_retries.pop(key, None)
                else:
                    pending_retries.pop(key, None)
                    retry_failed = True

            elif kind == 'runtime_stats':
                validate_runtime_stats(row)
                start = row['window_start_monotonic_ns']
                if runtime_partial or (runtime_end is not None and start != runtime_end):
                    raise ValueError('运行统计窗口不连续')
                if runtime_start is None:
                    runtime_start = start
                runtime_end = row['window_end_monotonic_ns']
                runtime_partial = row['partial']
                for stream in counts:
                    runtime_counts[stream] += row['streams'][stream]['count']

            elif kind == 'clock_sample':
                status = row['status']
                if status == 'ok':
                    estimate = estimate_clock(row['sent_wall_ns'], row['received_wall_ns'],
                        row['server_ms'], row['sent_monotonic_ns'], row['received_monotonic_ns'])
                    if (row['offset_ns'] != estimate.offset_ns or row['rtt_ns'] != estimate.rtt_ns
                            or row['sample_ns'] != row['received_wall_ns']
                            or row['valid_from_ns'] != row['sample_ns']
                            or row['valid_until_ns'] < row['valid_from_ns']
                            or type(row['max_rtt_ns']) is not int or row['max_rtt_ns'] < 0
                            or ('connection_reused' in row and type(row['connection_reused']) is not bool)
                            or type(row['usable']) is not bool
                            or row['usable'] != (row['rtt_ns'] <= row['max_rtt_ns'])):
                        raise ValueError('校时样本与估计结果不一致')
                elif status in ('error', 'timeout'):
                    if row['usable'] is not False or row['offset_ns'] is not None:
                        raise ValueError('失效校时样本不能带有效偏差')
                else:
                    raise ValueError('未知校时样本状态')
                last_clock_sample = row
                offset_alert_logged = False

            elif kind == 'clock_offset_alert':
                expected = (clock_offset_alert(last_clock_sample, threshold_ns=row['threshold_ns'])
                            if last_clock_sample is not None else None)
                if (offset_alert_logged or expected is None
                        or any(row.get(key) != value for key, value in expected.items())):
                    raise ValueError('时钟偏差告警与校时样本不一致')
                offset_alert_logged = True

            elif kind == 'trade_live':
                if row['receive_seq'] != expected_receive - 1 or last_raw_kind != 'aggTrade':
                    raise ValueError('实时成交动作没有对应原始消息')
                if row['receive_seq'] == last_trade_action_seq:
                    raise ValueError('同一条实时成交动作重复')
                if type(row['include']) is not bool or not isinstance(row['status'], str):
                    raise ValueError('实时成交动作格式无效')
                if not valid_missing_ranges(row['missing_ranges']):
                    raise ValueError('实时成交缺口格式无效')
                trade_missing = row['missing_ranges']
                trade_id = last_raw_data['a']
                if row['include']:
                    if trade_id in trade_ids:
                        raise ValueError('重复的实时成交被保留')
                    previous_high = trade_tracker.highest_id
                    actual_status = trade_tracker.observe(last_raw_data)
                    if actual_status != row['status']:
                        raise ValueError(f'实时成交状态与独立重算不一致：{actual_status}')
                    if actual_status == 'gap' and not any(start <= previous_high + 1 and end >= trade_id - 1
                                                         for start, end in row['missing_ranges']):
                        raise ValueError('实时成交未记录新发现的编号缺口')
                    trade_ids.add(trade_id)
                else:
                    if row['status'] not in ('duplicate', 'older_than_baseline'):
                        raise ValueError('未保留成交的状态不是重复或基线之前')
                    if type(trade_id) is not int or trade_id < 0:
                        raise ValueError('实时成交编号无效')
                    if trade_tracker.first_id is None:
                        raise ValueError('尚无成交基线，不能丢弃重复成交')
                    if row['status'] == 'duplicate' and trade_id < trade_tracker.first_id:
                        raise ValueError('基线之前的成交不能标为普通重复')
                    if row['status'] == 'older_than_baseline' and (trade_tracker.first_id is None or trade_id >= trade_tracker.first_id):
                        raise ValueError('基线之前的成交编号无效')
                    # 异步补查可能已经更新tracker，但对应 backfill 日志尚未写入。
                    # 排除项在结束时必须能找到已保留的实时或补回副本。
                    excluded_trade_ids.add(trade_id)
                trade_action_count += 1
                last_trade_action_seq = row['receive_seq']

            elif kind == 'raw':
                if connection_open is False:
                    raise ValueError('断线后、重连前不能收到实时消息')
                if connection_open is True and row.get('generation') != generation:
                    raise ValueError('消息连接代次不匹配')
                seq = row['receive_seq']
                if type(seq) is not int or seq != expected_receive:
                    raise ValueError('本地接收编号不连续')
                message = RawMessage(seq, row['received_ns'], row['monotonic_ns'], row['raw_text'])
                event = decode_message(message)
                last_raw_kind = event.kind
                last_raw_data = event.data
                if event.kind == 'aggTrade':
                    legacy_trade_tracker.observe(event.data)
                    trade_raw_count += 1
                expected_receive += 1
                counts[event.kind] += 1
                if event.kind == 'depth':
                    pending.append((seq, event.data))

            elif kind == 'snapshot':
                if sync is not None:
                    raise ValueError('第二份快照必须作为缺口后的 snapshot_reset')
                sync = DepthSynchronizer(row['snapshot'], strict_followup=row.get('strict_followup', False))
                provenance = BookProvenance(row['snapshot'])

            elif kind == 'book_validation':
                status = row['status']
                validation_id = row['validation_id']
                epoch = row['epoch']
                if status not in validation_counts:
                    raise ValueError('在线校验状态无效')
                if type(validation_id) is not int or validation_id < 1 or validation_id in validation_ids:
                    raise ValueError('在线校验编号无效或重复')
                if provenance is None or type(epoch) is not int or epoch < 1:
                    raise ValueError('在线校验缺少本地盘口轮次')
                if status == 'discarded':
                    if epoch >= provenance.epoch:
                        raise ValueError('丢弃结果必须来自较早盘口轮次')
                elif epoch != provenance.epoch:
                    raise ValueError('在线校验结果的盘口轮次不匹配')
                if status in ('matched', 'mismatch'):
                    if needs_reset or needs_bridge or not sync.valid or not applied_count:
                        raise ValueError('盘口尚未同步，不能声称在线校验结果')
                    snapshot = row['snapshot']
                    if row['snapshot_id'] != snapshot['lastUpdateId'] or row['local_id'] != sync.last_update_id:
                        raise ValueError('在线校验快照或本地编号不一致')
                    comparison = compare_exchange_snapshot(snapshot, provenance.known, provenance.last_changes,
                        initial_id=provenance.initial_id, local_id=provenance.current_id, contiguous=True)
                    if (not comparison['eligible'] or comparison['compared'] < 1
                            or comparison['matches'] != (status == 'matched')):
                        raise ValueError('在线校验状态与可比价位结果不一致')
                    for field in ('eligible', 'compared', 'matches', 'differences', 'skipped'):
                        if row[field] != comparison[field]:
                            raise ValueError(f'在线校验 {field} 与重新计算结果不一致')
                    if status == 'mismatch':
                        sync.valid = False
                        needs_reset = True
                elif not isinstance(row.get('reason'), str) or not row['reason']:
                    raise ValueError('不可校验或异常结果必须保留原因')
                validation_ids.add(validation_id)
                validation_counts[status] += 1
                validation_events.append({'validation_id': validation_id, 'epoch': epoch, 'status': status,
                    'reason': row.get('reason', '交易所快照有差异' if status == 'mismatch' else None),
                    'snapshot_id': row.get('snapshot_id'), 'compared': row.get('compared'),
                    'differences': row.get('differences', [])})

            elif kind == 'depth_gap':
                if sync is None or needs_reset:
                    raise ValueError('发现缺口之前没有可用盘口')
                if not pending or row['receive_seq'] != pending[0][0]:
                    raise ValueError('缺口缺少对应的队首原始消息')
                event = pending[0][1]
                if (row['before_id'] != sync.last_update_id or row['U'] != event['U']
                        or row['u'] != event['u']):
                    raise ValueError('缺口信息与原始消息或当前进度不一致')
                try:
                    sync.apply(event)
                except DepthGapError:
                    pass
                else:
                    raise ValueError('编号检查器未发现记录中的缺口')
                # 引发缺口的消息仍留在队列中，待新快照后按顺序处理。
                needs_reset = True
                gap_count += 1

            elif kind == 'snapshot_reset':
                if not needs_reset:
                    raise ValueError('没有缺口，不能重置快照')
                if connection_open is False:
                    raise ValueError('断线时不能建立新盘口')
                sync = DepthSynchronizer(row['snapshot'], strict_followup=row.get('strict_followup', False))
                provenance = provenance.reset(row['snapshot'])
                needs_reset = False
                needs_bridge = True
                reset_count += 1

            elif kind == 'depth_action':
                if sync is None:
                    raise ValueError('处理更新之前缺少快照')
                if needs_reset:
                    raise ValueError('盘口失效，需要先加载新快照')
                if not pending or row['receive_seq'] != pending[0][0]:
                    raise ValueError('处理动作缺少原始消息或未按接收顺序处理')
                _, event = pending.popleft()
                before = sync.last_update_id
                if row['before_id'] != before:
                    raise ValueError('处理前编号与记录不一致')
                actual = sync.apply(event)
                if row['action'] != actual or row['after_id'] != sync.last_update_id:
                    raise ValueError('处理结果或处理后编号与记录不一致')
                if actual == 'applied':
                    provenance.observe_applied(event)
                    applied_count += 1
                    needs_bridge = False
                else:
                    ignored_count += 1

            elif kind == 'book_checkpoint':
                if sync is None or not sync.valid or needs_reset or needs_bridge or not applied_count:
                    raise ValueError('盘口尚未同步，不能校验摘要')
                checkpoint = make_checkpoint(sync)
                checkpoints_checked += 1
                comparison = compare_top20(row['top'], checkpoint['top'])
                if row['last_update_id'] != checkpoint['last_update_id'] or not comparison['matches']:
                    raise ValueError(f"盘口摘要不一致：{comparison['differences']}")
                checkpoints_passed += 1

            elif kind == 'end':
                if row['status'] != 'success' or sync is None or applied_count == 0:
                    raise ValueError('记录未成功完成同步，不能作为成功回放')
                if needs_reset or needs_bridge:
                    raise ValueError('缺口后未重新同步，不能作为成功回放')
                if connection_open is False:
                    raise ValueError('结束时连接未恢复')
                if pending_retries or retry_failed:
                    raise ValueError('REST 请求恢复未完成，不能作为成功回放')
                if trade_errors or trade_missing:
                    raise ValueError('成交补查未完成，不能作为成功回放')
                if trade_action_count:
                    if trade_action_count != trade_raw_count:
                        raise ValueError('原始成交缺少处理动作')
                    if trade_tracker.missing_ranges or not excluded_trade_ids <= trade_ids:
                        raise ValueError('独立重算发现成交缺口或重复项缺少已保留副本')
                else:
                    # 历史教学日志没有 trade_live 动作，仍独立检查 raw 成交序号。
                    if connection_open is not None and trade_raw_count:
                        raise ValueError('原始成交缺少处理动作')
                    if legacy_trade_tracker.missing_ranges:
                        raise ValueError('历史原始成交编号存在缺口')
                if row['received_count'] != expected_receive - 1:
                    raise ValueError('结束记录的接收条数不一致')
                if runtime_start is not None:
                    started, stopped = row['started_monotonic_ns'], row['stopped_monotonic_ns']
                    duration = row['requested_duration_seconds']
                    if (type(started) is not int or type(stopped) is not int
                            or type(duration) not in (int, float) or not math.isfinite(duration) or duration <= 0
                            or runtime_start != started
                            or runtime_end != min(stopped, started + max(1, round(duration * 1_000_000_000)))
                            or any(runtime_counts[stream] > counts[stream] for stream in counts)):
                        raise ValueError('运行统计与采集时间或消息条数不一致')
                if row['processed_depth'] != applied_count + ignored_count:
                    raise ValueError('结束记录的盘口处理条数不一致')
                if row['pending_depth'] != len(pending):
                    raise ValueError('结束记录的未处理盘口条数不一致')
                if row['last_update_id'] != sync.last_update_id or row['top'] != top_as_text(sync.book):
                    raise ValueError('回放结果与结束记录不一致')
                ended = True
            else:
                raise ValueError(f'未知的记录类型：{kind!r}')
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f'日志第 {line_number} 行无法回放：{exc}') from exc

    if not ended:
        raise ValueError('缺少成功的结束记录')
    return {'last_update_id': sync.last_update_id, 'top': top_as_text(sync.book),
            'received_count': expected_receive - 1, 'counts': counts,
            'applied_count': applied_count, 'ignored_count': ignored_count,
            'pending_depth_count': len(pending), 'gap_count': gap_count,
            'reset_count': reset_count,
            'checkpoints_checked': checkpoints_checked, 'checkpoints_passed': checkpoints_passed,
            'checkpoint_gaps': checkpoint_gaps,
            'validation_counts': validation_counts, 'validation_events': validation_events}


def main():
    parser = argparse.ArgumentParser(description='离线回放一次教学盘口日志')
    parser.add_argument('path', help='JSONL 日志文件')
    args = parser.parse_args()
    with open(args.path, encoding='utf-8') as file:
        result = replay(file)
    print(f"回放成功：收到 {result['received_count']} 条，分类 {result['counts']}")
    print(f"盘口更新：应用 {result['applied_count']}，忽略 {result['ignored_count']}，结尾未处理 {result['pending_depth_count']}")
    print(f"本地更新编号：{result['last_update_id']}；前一档：{result['top']}")
    print('记录与回放结果一致，不单独证明交易所数据未缺失。')


if __name__ == '__main__':
    main()
