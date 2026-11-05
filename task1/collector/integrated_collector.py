"""教学短录：三流采集、盘口同步、后台补查、校时和时点对账。

支持后台分区、快照摘要与离线回放；尚需长期运行及作业完整验收。
"""

from concurrent.futures import ThreadPoolExecutor
from collections import deque
import argparse
import math
from pathlib import Path
import queue
import sys
import threading
import time

from api_client import PublicAPIClient, RateLimitError, RateLimitWait
from book_provenance import BookProvenance
from book_sync import DepthGapError, DepthSynchronizer
from checkpoint_helpers import make_checkpoint
from clock_estimates import DEFAULT_OFFSET_ALERT_NS, TradeReceiptEstimator, clock_offset_alert
from clock_monitor import estimate_clock
from clock_samples import ClockSamples
from journal_capture import Journal, top_as_text
from market_stream import decode_message
from market_projection import project_backfill, project_live
from nonblocking_retry import NonblockingRetry
from online_validation import OnlineValidation
from reconnect_stream import ReconnectingStream, StreamClosed, StreamLost, StreamMessage, StreamOpened
from runtime_stats import RuntimeStats
from trade_flow import TradeFlow


def collect(stream, get_snapshot, trade_flow, output, *, duration=3, wait_timeout=5,
            max_resync=3, queue_size=5000, market_store=None, clock_query=None,
            clock_interval=60, clock_timeout=5, clock_validity_ns=60_000_000_000,
            clock_max_rtt_ns=1_000_000_000, validation_query=None,
            validation_interval=30, validation_max_age=15, snapshot_timeout=15,
            rate_retry_timeout=180, clock_offset_alert_ns=DEFAULT_OFFSET_ALERT_NS):
    """短时采集。接收线程只收事件并入队；网络补查由后台线程执行。"""
    events = queue.Queue(maxsize=queue_size)
    stopped = threading.Event()
    errors = []
    journal = Journal(output)
    sync = None
    provenance = None
    connected = False
    generation = 0
    needs_bridge = True
    next_checkpoint = None
    runtime_stats = None
    stats_cutoff_ns = None
    applied = processed = received = 0
    resyncs = 0
    repair_active = False
    repair_generation = None
    repair_wait = None
    repair_waiting = False
    repair_blocked = False
    trade_tail_unknown = False
    trade_last_before_loss = None
    if (clock_interval <= 0 or clock_timeout <= 0 or clock_validity_ns < 0
            or clock_max_rtt_ns < 0 or validation_interval <= 0 or validation_max_age <= 0
            or type(clock_offset_alert_ns) is not int or clock_offset_alert_ns <= 0
            or snapshot_timeout <= 0 or not math.isfinite(rate_retry_timeout) or rate_retry_timeout <= 0
            or isinstance(duration, bool)
            or not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration <= 0):
        raise ValueError('校时间隔、超时或有效条件无效')
    calibration = ClockSamples(max_rtt_ns=clock_max_rtt_ns, max_age_ns=clock_validity_ns)
    receipt_estimator = TradeReceiptEstimator(calibration)
    clock_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='clock-sample') if clock_query else None
    clock_future = None
    clock_due = time.monotonic()
    clock_deadline = None
    clock_expired = False
    validator = OnlineValidation(validation_query, max_age=validation_max_age) if validation_query else None
    snapshot_retry = NonblockingRetry(get_snapshot, max_age=rate_retry_timeout,
                                      attempt_timeout=snapshot_timeout)
    snapshot_active = False
    snapshot_deadline = None
    pending_depth = deque()
    barrier = None
    validation_due = time.monotonic() + validation_interval
    validation_active = False
    validation_id = 0

    def query_repair():
        result = trade_flow.query_missing()
        if result['errors']:
            error = result['errors'][0]
            try:
                delay = float(error['retry_after'])
            except (TypeError, ValueError):
                delay = None
            if delay is not None and (not math.isfinite(delay) or delay < 0):
                delay = None
            if error['status'] in (418, 429):
                rate_error = RateLimitError(error['status'], delay)
            elif error['local_limit_wait']:
                rate_error = RateLimitWait(delay)
            else:
                return result
            rate_error.partial_result = result
            raise rate_error
        return result

    trade_retry = NonblockingRetry(query_repair, max_age=rate_retry_timeout)

    def log_retry(operation, outcome, state):
        journal.add('rest_retry', operation=operation, generation=outcome['token'],
                    attempt=outcome['attempts'], state=state, observed_ns=time.time_ns(),
                    error=outcome.get('error'), reason=outcome.get('reason'))

    def poll_validation(now, *, finishing=False):
        nonlocal validation_due, validation_active, validation_id, needs_bridge, next_checkpoint
        if validator is None:
            return
        if validation_active:
            result = validator.poll(provenance, contiguous=(connected and sync is not None
                                    and sync.valid and not needs_bridge))
            if result is not None and result['status'] != 'pending':
                journal.add('book_validation', validation_id=validation_id,
                            observed_ns=time.time_ns(), **result)
                validation_active = False
                if result['status'] == 'mismatch':
                    sync.valid = False
                    needs_bridge = True
            elif finishing:
                journal.add('book_validation', validation_id=validation_id,
                            observed_ns=time.time_ns(), status='unverifiable',
                            epoch=provenance.epoch, reason='采集结束前在线校验未完成')
                validation_active = False
        if (not finishing and not validation_active and now >= validation_due and connected
                and sync is not None and sync.valid and not needs_bridge):
            validation_id += 1
            validation_due = now + validation_interval
            try:
                validator.submit(epoch=provenance.epoch)
                validation_active = True
            except Exception as exc:
                journal.add('book_validation', validation_id=validation_id,
                            observed_ns=time.time_ns(), status='error', epoch=provenance.epoch,
                            reason=f'{type(exc).__name__}: {exc}')

    def record_clock_sample(**fields):
        journal.add('clock_sample', **fields)
        calibration.observe(fields)
        alert = clock_offset_alert(fields, threshold_ns=clock_offset_alert_ns)
        if alert is not None:
            journal.add('clock_offset_alert', **alert)
            confidence = ('扣除往返时间裕量后仍超阈值' if alert['outside_threshold_with_rtt_margin']
                          else '往返时间带来较大不确定性')
            print(f"时钟偏差告警：估计 {alert['offset_ns'] / 1e6:+.1f} ms，{confidence}",
                  file=sys.stderr, flush=True)

    def write_runtime_windows(windows):
        for window in windows:
            journal.add('runtime_stats', observed_ns=time.time_ns(), **window)

    def poll_clock(now, *, finishing=False):
        nonlocal clock_future, clock_due, clock_deadline, clock_expired
        if clock_executor is None:
            return
        if clock_future is not None and not clock_expired and now >= clock_deadline:
            record_clock_sample(status='timeout', observed_ns=time.time_ns(),
                        error='校时请求超时', usable=False, offset_ns=None, rtt_ns=None)
            clock_expired = True
        if clock_future is not None and clock_future.done():
            if not clock_expired:
                try:
                    sample = clock_future.result()
                    server_ms = sample.data['serverTime']
                    estimate = estimate_clock(sample.sent_wall_ns, sample.received_wall_ns,
                        server_ms, sample.sent_monotonic_ns, sample.received_monotonic_ns)
                    fields = dict(status='ok', sample_ns=sample.received_wall_ns,
                        sent_wall_ns=sample.sent_wall_ns, received_wall_ns=sample.received_wall_ns,
                        sent_monotonic_ns=sample.sent_monotonic_ns,
                        received_monotonic_ns=sample.received_monotonic_ns, server_ms=server_ms,
                        offset_ns=estimate.offset_ns, rtt_ns=estimate.rtt_ns,
                        valid_from_ns=sample.received_wall_ns,
                        valid_until_ns=sample.received_wall_ns + clock_validity_ns,
                        max_rtt_ns=clock_max_rtt_ns, usable=estimate.rtt_ns <= clock_max_rtt_ns)
                    connection_reused = getattr(sample, 'connection_reused', None)
                    if connection_reused is not None:
                        fields['connection_reused'] = connection_reused
                except Exception as exc:
                    fields = dict(status='error', observed_ns=time.time_ns(),
                                  error=f'{type(exc).__name__}: {exc}', usable=False,
                                  offset_ns=None, rtt_ns=None)
                record_clock_sample(**fields)
            clock_future = None
        elif clock_future is not None and not clock_expired and finishing:
            record_clock_sample(status='timeout', observed_ns=time.time_ns(),
                        error='校时请求超时或采集结束前未完成', usable=False, offset_ns=None, rtt_ns=None)
            clock_expired = True
        # 等初始快照先完成，首次校时可以使用快照刚建立的 HTTPS 连接。
        if not finishing and sync is not None and clock_future is None and now >= clock_due:
            clock_future = clock_executor.submit(clock_query)
            clock_deadline = now + clock_timeout
            clock_due = now + clock_interval
            clock_expired = False

    def receive():
        try:
            while not stopped.is_set():
                event = stream.next_event()
                events.put_nowait(event)
        except Exception as exc:
            if not (isinstance(exc, StreamClosed) and stopped.is_set()):
                errors.append(exc)
                stopped.set()

    def poll_checkpoint(now):
        nonlocal next_checkpoint
        if next_checkpoint is None or now < next_checkpoint:
            return
        missed = int(now - next_checkpoint) + 1
        if now - next_checkpoint >= 0.1:
            journal.add('checkpoint_gap', checkpoint_ns=time.time_ns(), missed_count=missed,
                        reason='processing_delayed')
        elif not connected or sync is None or not sync.valid or needs_bridge:
            journal.add('checkpoint_gap', checkpoint_ns=time.time_ns(), missed_count=missed,
                        reason='unsynchronized')
        else:
            journal.add('book_checkpoint', checkpoint_ns=time.time_ns(), **make_checkpoint(sync))
        next_checkpoint += missed

    def write_repair(result, origin_generation):
        nonlocal repair_blocked
        for record in result['records']:
            record = receipt_estimator.estimate(record, now_wall_ns=time.time_ns(),
                                                now_monotonic_ns=time.monotonic_ns())
            journal.add('trade_backfill', record=record, origin_generation=origin_generation)
            if market_store is not None:
                for row in project_backfill(record, symbol='BTCUSDT', generation=origin_generation):
                    # 补回消息没有真实接收时刻；按交易所成交时间归档，并标明依据。
                    row['partition_basis'] = 'exchange_trade_time_ms'
                    market_store.submit('aggTrade', row['T'], row)
        journal.add('trade_repair', missing_ranges=result['missing_ranges'], errors=result['errors'])
        if result['errors']:
            repair_blocked = True

    def start_repair(cutoff=None):
        nonlocal repair_active, repair_generation
        if repair_active or repair_blocked or not trade_flow.tracker.missing_ranges:
            return
        trade_retry.start(token=generation, deadline=cutoff)
        repair_generation = generation
        repair_active = True

    def poll_repair(cutoff=None):
        nonlocal repair_active, repair_wait, repair_waiting
        if not repair_active:
            return
        # 成交 ID 属于交易对，重连不会使旧连接发现的成交缺口失效。
        if repair_waiting and not trade_flow.tracker.missing_ranges:
            trade_retry.poll(token=repair_generation, active=False)
            log_retry('trade_backfill', repair_wait, 'resolved_live')
            repair_wait = None
            repair_active = False
            repair_waiting = False
            return
        outcome = trade_retry.poll(token=repair_generation, deadline=cutoff)
        state = outcome['status']
        if state == 'pending':
            repair_waiting = False
            return
        if state == 'retry_wait':
            if 'cause' in outcome:  # 同一等待期间的轮询不会重复写入部分结果。
                result = trade_flow.apply_repair(outcome['cause'].partial_result)
                write_repair({**result, 'errors': []}, repair_generation)
                log_retry('trade_backfill', outcome, 'waiting')
                repair_wait = outcome
                repair_waiting = True
            return
        repair_active = False
        repair_waiting = False
        if state == 'success':
            result = trade_flow.apply_repair(outcome['result'])
            if repair_wait is not None and result['errors'] and not result['missing_ranges']:
                write_repair({**result, 'errors': []}, repair_generation)
                log_retry('trade_backfill', {**outcome, 'error': result['errors'][0]}, 'resolved_live')
            else:
                write_repair(result, repair_generation)
                if repair_wait is not None:
                    log_retry('trade_backfill', outcome, 'failed' if result['errors'] else 'recovered')
        else:
            partial = getattr(outcome.get('cause'), 'partial_result', None)
            rate_limited = partial is not None or repair_wait is not None
            if partial is None:
                partial = {'records': [], 'errors': [outcome.get('error') or {'message': outcome['reason']}]}
            result = trade_flow.apply_repair(partial)
            if not result['missing_ranges'] and rate_limited:
                write_repair({**result, 'errors': []}, repair_generation)
                log_retry('trade_backfill', outcome, 'resolved_live')
            else:
                write_repair(result, repair_generation)
                log_retry('trade_backfill', outcome, 'failed')
        repair_wait = None

    worker = threading.Thread(target=receive, name='market-receiver', daemon=True)
    worker.start()
    deadline = None
    bridge_deadline = None
    started_wall_ns = started_monotonic_ns = None
    stopped_wall_ns = stopped_monotonic_ns = None
    startup_deadline = time.monotonic() + wait_timeout
    failure = None

    def start_snapshot():
        nonlocal snapshot_active, snapshot_deadline
        if snapshot_active:
            return
        snapshot_retry.start(token=generation)
        snapshot_active = True
        snapshot_deadline = time.monotonic() + rate_retry_timeout

    def process_depth():
        nonlocal sync, provenance, needs_bridge, applied, processed, resyncs, next_checkpoint, deadline, started_wall_ns, started_monotonic_ns
        nonlocal runtime_stats, stats_cutoff_ns
        while pending_depth and not snapshot_active:
            if sync is None or not sync.valid:
                start_snapshot()
                return
            seq, data = pending_depth[0]
            before = sync.last_update_id
            try:
                action = sync.apply(data)
            except DepthGapError as exc:
                journal.add('depth_gap', receive_seq=seq, before_id=before,
                            U=data['U'], u=data['u'], reason=str(exc))
                if resyncs >= max_resync:
                    raise RuntimeError('盘口重同步超过次数限制')
                needs_bridge = True
                resyncs += 1
                start_snapshot()
                return
            journal.add('depth_action', receive_seq=seq, action=action,
                        before_id=before, after_id=sync.last_update_id)
            pending_depth.popleft()
            processed += 1
            if action == 'applied':
                provenance.observe_applied(data)
                applied += 1
                needs_bridge = False
                if deadline is None:
                    started_monotonic_ns = time.monotonic_ns()
                    started_wall_ns = time.time_ns()
                    deadline = started_monotonic_ns / 1_000_000_000 + duration
                    runtime_stats = RuntimeStats(started_monotonic_ns)
                    stats_cutoff_ns = started_monotonic_ns + max(1, round(duration * 1_000_000_000))
                if next_checkpoint is None:
                    next_checkpoint = time.monotonic() + 1

    def poll_snapshot(*, cutoff=None):
        nonlocal snapshot_active, sync, provenance, needs_bridge, bridge_deadline
        if not snapshot_active:
            return
        outcome = snapshot_retry.poll(token=generation, active=connected, deadline=cutoff)
        state = outcome['status']
        if state == 'retry_wait' and 'cause' in outcome:
            log_retry('snapshot', outcome, 'waiting')
        if state in ('pending', 'retry_wait'):
            return
        snapshot_active = False
        if state != 'success':
            log_retry('snapshot', outcome, 'failed')
            if 'cause' in outcome:
                raise outcome['cause']
            raise TimeoutError(f"同步快照请求未完成：{outcome.get('reason', state)}")
        if outcome['attempts'] > 1:
            log_retry('snapshot', outcome, 'recovered')
        snapshot = outcome['result']
        journal.add('snapshot' if sync is None else 'snapshot_reset',
                    snapshot=snapshot, strict_followup=True)
        sync = DepthSynchronizer(snapshot, strict_followup=True)
        provenance = BookProvenance(snapshot) if provenance is None else provenance.reset(snapshot)
        needs_bridge = True
        if deadline is None:
            bridge_deadline = time.monotonic() + wait_timeout
        process_depth()

    def process(event):
        nonlocal sync, connected, generation, needs_bridge, received, trade_tail_unknown, trade_last_before_loss
        if isinstance(event, StreamOpened):
            if connected or event.generation != generation + 1:
                raise RuntimeError('连接事件顺序无效')
            generation = event.generation
            connected = True
            needs_bridge = True
            journal.add('connection_open', generation=generation, observed_ns=event.observed_ns,
                        reconnected=event.reconnected)
            return
        if isinstance(event, StreamLost):
            if not connected or event.generation != generation:
                raise RuntimeError('断线事件顺序无效')
            if not trade_tail_unknown:
                trade_last_before_loss = trade_flow.tracker.highest_id
            trade_tail_unknown = True
            journal.add('connection_lost', generation=generation, observed_ns=event.observed_ns,
                        reason=event.reason, trade_last_id=trade_last_before_loss,
                        trade_tail_unknown=True)
            connected = False
            needs_bridge = True
            if sync is not None:
                sync.valid = False
            return
        if not isinstance(event, StreamMessage) or not connected or event.generation != generation:
            raise RuntimeError('行情消息的连接状态无效')

        message = event.message
        if message.sequence != received + 1:
            raise RuntimeError('接收编号不连续')
        journal.add('raw', generation=generation, receive_seq=message.sequence,
                    received_ns=message.received_ns, monotonic_ns=message.monotonic_ns,
                    raw_text=message.raw_text)
        received += 1
        decoded = decode_message(message)
        if runtime_stats is not None:
            processed_ns = time.monotonic_ns()
            if processed_ns < stats_cutoff_ns:
                delay = (None if decoded.kind == 'bookTicker' else calibration.delay(
                    message.received_ns, decoded.data.get('E'), message.monotonic_ns))
                write_runtime_windows(runtime_stats.observe(decoded.kind, processed_ns, delay))
        if decoded.kind == 'aggTrade':
            result = trade_flow.observe_live(decoded.data, message.received_ns)
            if result['include']:
                receipt_estimator.observe_live(decoded.data, message.received_ns, message.monotonic_ns)
            if (trade_tail_unknown and trade_last_before_loss is not None
                    and decoded.data['a'] > trade_last_before_loss):
                trade_tail_unknown = False
            journal.add('trade_live', receive_seq=message.sequence, status=result['status'],
                        include=result['include'], missing_ranges=result['missing_ranges'])
            if result['include'] and market_store is not None:
                for row in project_live(message, generation=generation):
                    market_store.submit(decoded.kind, message.received_ns // 1_000_000, row)
            if result['status'] == 'gap':
                start_repair()
            return
        if market_store is not None:
            for row in project_live(message, generation=generation):
                market_store.submit(decoded.kind, message.received_ns // 1_000_000, row)
        if decoded.kind != 'depth':
            return
        if len(pending_depth) >= queue_size:
            raise RuntimeError('待同步盘口消息队列已满')
        pending_depth.append((message.sequence, decoded.data))
        process_depth()

    def active_deadline():
        if deadline is not None:
            return deadline
        if snapshot_active:
            return snapshot_deadline
        return bridge_deadline if bridge_deadline is not None else startup_deadline

    try:
        while True:
            if errors:
                raise RuntimeError(f'接收失败：{errors[0]}') from errors[0]
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                break
            poll_snapshot()
            if now >= active_deadline():
                break
            poll_repair()
            poll_clock(now)
            poll_validation(now)
            poll_checkpoint(now)
            if runtime_stats is not None:
                write_runtime_windows(runtime_stats.advance(min(time.monotonic_ns(), stats_cutoff_ns)))
            if barrier is not None:
                if pending_depth or snapshot_active:
                    time.sleep(0.01)
                    continue
                process(barrier)
                barrier = None
            try:
                end = active_deadline()
                wakeup = min(end, next_checkpoint) if next_checkpoint is not None else end
                if clock_executor is not None or validator is not None or snapshot_active or repair_active:
                    wakeup = min(wakeup, time.monotonic() + 0.05)
                event = events.get(timeout=min(wait_timeout, max(0.001, wakeup-time.monotonic())))
            except queue.Empty:
                continue
            if isinstance(event, StreamLost) and (pending_depth or snapshot_active):
                # 先完成本连接已记录的盘口消息，再处理断线；不丢弃原始消息。
                barrier = event
            else:
                process(event)
        if deadline is None:
            raise TimeoutError('启动时未能完成初始快照同步')
        stopped_monotonic_ns = time.monotonic_ns()
        stopped_wall_ns = time.time_ns()
        stopped.set()
        stream.close()
        worker.join(timeout=2)
        if worker.is_alive():
            raise RuntimeError('接收线程未停止')
        if errors:
            raise RuntimeError(f'接收失败：{errors[0]}') from errors[0]
        finish_deadline = time.monotonic() + snapshot_timeout
        while barrier is not None or pending_depth or snapshot_active or not events.empty():
            now = time.monotonic()
            if now >= finish_deadline:
                raise TimeoutError('结束时仍有未完成的盘口同步')
            poll_snapshot(cutoff=finish_deadline)
            poll_repair(cutoff=finish_deadline)
            poll_checkpoint(min(now, deadline))
            if barrier is not None:
                if pending_depth or snapshot_active:
                    time.sleep(0.01)
                    continue
                process(barrier)
                barrier = None
            if not events.empty():
                event = events.get_nowait()
                if isinstance(event, StreamLost) and (pending_depth or snapshot_active):
                    barrier = event
                else:
                    process(event)
            elif pending_depth or snapshot_active:
                time.sleep(0.01)
        poll_checkpoint(min(time.monotonic(), deadline))
        poll_clock(time.monotonic(), finishing=True)
        poll_validation(time.monotonic(), finishing=True)
        while repair_active:
            poll_repair(cutoff=finish_deadline)
            if repair_active:
                time.sleep(0.01)
        # 一轮补查期间又可能发现新的缺口；补查一次剩余范围。
        if time.monotonic() < finish_deadline:
            start_repair(cutoff=finish_deadline)
            while repair_active:
                poll_repair(cutoff=finish_deadline)
                if repair_active:
                    time.sleep(0.01)
        if not connected or sync is None or needs_bridge:
            raise RuntimeError('采集结束时盘口没有完成同步')
        if trade_tail_unknown:
            raise RuntimeError('断线后的成交尾部未知，不能确认完整性')
        if trade_flow.tracker.missing_ranges or repair_blocked:
            raise RuntimeError('成交补查未完成或发生错误')
    except (Exception, KeyboardInterrupt) as exc:
        failure = exc
    finally:
        if stopped_monotonic_ns is None:
            stopped_monotonic_ns = time.monotonic_ns()
            stopped_wall_ns = time.time_ns()
        stopped.set()
        stream.close()
        worker.join(timeout=2)
        if clock_executor is not None:
            clock_executor.shutdown(wait=False, cancel_futures=True)
        if validator is not None:
            validator.close()
        snapshot_retry.close()
        trade_retry.close()

    if market_store is not None:
        try:
            market_store.close()
        except Exception as exc:
            if failure is None:
                failure = exc

    if runtime_stats is not None:
        write_runtime_windows(runtime_stats.finish(min(stopped_monotonic_ns, stats_cutoff_ns)))
    timing = {'requested_duration_seconds': duration, 'started_wall_ns': started_wall_ns,
              'started_monotonic_ns': started_monotonic_ns, 'stopped_wall_ns': stopped_wall_ns,
              'stopped_monotonic_ns': stopped_monotonic_ns, 'finalized_wall_ns': time.time_ns()}
    if failure:
        journal.add('end', status='failed', reason=f'{type(failure).__name__}: {failure}',
                    received_count=received, **timing)
        raise failure
    journal.add('end', status='success', received_count=received, processed_depth=processed,
                pending_depth=0, last_update_id=sync.last_update_id, top=top_as_text(sync), **timing)
    return {'received_count': received, 'processed_depth': processed, 'resyncs': resyncs,
            'missing_ranges': trade_flow.tracker.missing_ranges.copy(),
            'last_update_id': sync.last_update_id}


def _positive_seconds(value):
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError('时长必须是正数') from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError('时长必须是有限正数')
    return seconds


def build_parser():
    parser = argparse.ArgumentParser(description='采集公开行情并按时间分区保存')
    parser.add_argument('--duration-seconds', type=_positive_seconds, default=3,
                        help='首次盘口同步成功后运行的秒数，默认 3')
    parser.add_argument('--output-root', type=Path,
                        default=Path.home() / 'Documents' / 'KID_Binance_练习数据',
                        help='存储根目录，每次运行在其中创建不覆盖旧数据的新目录')
    return parser


def main(argv=None):
    from partition_journal import PartitionJournal
    from partition_store import PartitionStore

    args = build_parser().parse_args(argv)
    data_dir = args.output_root
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / f'整合分片_{time.time_ns()}'
    path.mkdir(exist_ok=False)
    api = PublicAPIClient(reuse_connections=True)
    print(f'记录目录：{path}', flush=True)
    try:
        with PartitionJournal(path / 'journal', file_format='auto') as output:
            file_format = output.store.file_format
            storage_label = 'Parquet' if file_format == 'parquet' else 'gzip CSV（Parquet 依赖不可用）'
            print(f'存储格式：{storage_label}', flush=True)
            result = collect(ReconnectingStream(), lambda: api.depth(limit=1000).data,
                             TradeFlow(lambda first, limit: api.aggtrades(first, limit).data), output,
                             market_store=PartitionStore(path / 'market', file_format=file_format),
                             clock_query=api.server_time,
                             validation_query=lambda: api.depth(limit=1000).data,
                             duration=args.duration_seconds)
    except KeyboardInterrupt:
        print('已手动中止；本次采集未作为成功验收。', flush=True)
        raise SystemExit(130)
    finally:
        api.close()
    print('采集结果：', result)
    print('采集结束；仍需核验记录、回放和质量报告，不能仅凭运行结束认定作业通过。')


if __name__ == '__main__':
    main()
