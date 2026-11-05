"""约 3 秒教学短录：保存同次原文、快照和处理动作；不是正式采集器。"""

import json
from pathlib import Path
import queue
import threading
import time

from book_sync import DepthGapError, DepthSynchronizer
from live_book_sync import fetch_snapshot
from market_stream import connect, decode_message


class Journal:
    """两个线程共用文件时，锁保证记录编号与写入顺序一致。"""
    def __init__(self, file):
        self.file = file
        self.lock = threading.Lock()
        self.sequence = 0

    def add(self, kind, **details):
        with self.lock:
            self.sequence += 1
            row = {'journal_seq': self.sequence, 'type': kind, **details}
            self.file.write(json.dumps(row, ensure_ascii=False) + '\n')


def top_as_text(sync):
    top = sync.book.top_levels(1)
    return {side: [[str(price), str(quantity)] for price, quantity in values]
            for side, values in top.items()}


def capture(reader, get_snapshot, output, duration=3, wait_timeout=5, max_resync=3):
    journal = Journal(output)
    pending = queue.Queue(maxsize=5000)
    stopped = threading.Event()
    errors = []
    received = 0
    processed = 0
    applied = 0
    resyncs = 0
    needs_bridge = True
    sync = None

    def receive_loop():
        nonlocal received
        try:
            while not stopped.is_set():
                msg = reader.receive()
                # 先存原文再解码、排队；动作不会引用尚未记录的原文。
                journal.add('raw', receive_seq=msg.sequence, received_ns=msg.received_ns,
                            monotonic_ns=msg.monotonic_ns, raw_text=msg.raw_text)
                received += 1
                event = decode_message(msg)
                if event.kind == 'depth':
                    pending.put_nowait((msg.sequence, event.data))
        except Exception as exc:
            if not stopped.is_set():
                errors.append(exc)
                stopped.set()

    def next_depth(timeout):
        end = time.monotonic() + timeout
        while True:
            if errors:
                raise RuntimeError(f'接收失败：{errors[0]}') from errors[0]
            remaining = end - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('等待盘口更新超时')
            try:
                return pending.get(timeout=min(remaining, 0.1))
            except queue.Empty:
                continue

    thread = threading.Thread(target=receive_loop, daemon=True)
    thread.start()
    try:
        first = next_depth(wait_timeout)
        snapshot = get_snapshot()
        journal.add('snapshot', snapshot=snapshot)
        sync = DepthSynchronizer(snapshot)
        deadline = time.monotonic() + duration
        item = first
        while True:
            if errors:
                raise RuntimeError(f'接收失败：{errors[0]}') from errors[0]
            if item is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    item = next_depth(remaining)
                except TimeoutError:
                    break
            seq, event = item
            before = sync.last_update_id
            try:
                action = sync.apply(event)
            except DepthGapError:
                # 旧盘口已经失效；保留当前消息，新快照后再次检查它。
                journal.add('depth_gap', receive_seq=seq, before_id=before,
                            U=event['U'], u=event['u'])
                if resyncs >= max_resync:
                    raise RuntimeError(f'盘口重同步超过 {max_resync} 次')
                replacement = get_snapshot()
                journal.add('snapshot_reset', snapshot=replacement)
                sync = DepthSynchronizer(replacement)
                resyncs += 1
                needs_bridge = True
                continue
            journal.add('depth_action', receive_seq=seq, action=action,
                        before_id=before, after_id=sync.last_update_id)
            processed += 1
            applied += action == 'applied'
            if action == 'applied':
                needs_bridge = False
            item = None
        if not applied or needs_bridge:
            raise TimeoutError('未等到能衔接快照的更新')
    except Exception as exc:
        failure = exc
    else:
        failure = None
    finally:
        stopped.set()
        try:
            reader.close()
        finally:
            thread.join(timeout=2)

    if thread.is_alive():
        failure = RuntimeError('接收线程未停止，记录可能不完整')
    elif errors:
        failure = RuntimeError(f'接收失败：{errors[0]}')
    if failure:
        journal.add('end', status='failed', reason=f'{type(failure).__name__}: {failure}',
                    received_count=received)
        raise failure

    journal.add('end', status='success', received_count=received,
                processed_depth=processed, pending_depth=pending.qsize(),
                last_update_id=sync.last_update_id, top=top_as_text(sync))
    return {'received': received, 'processed_depth': processed, 'applied': applied,
            'resyncs': resyncs,
            'pending_depth': pending.qsize(), 'snapshot_id': snapshot['lastUpdateId'],
            'last_update_id': sync.last_update_id, 'top': top_as_text(sync)}


def main():
    data_dir = Path.home() / 'Documents' / 'KID_Binance_练习数据'
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / f'三路同步记录_{time.time_ns()}.jsonl'
    with path.open('x', encoding='utf-8') as output:
        print(f'记录文件：{path}', flush=True)
        result = capture(connect(), fetch_snapshot, output)
    print('短测结果：', result)
    print('快照和首条更新按作业 S+1 条件衔接；官方首次边界差异待确认。')
    print('教学记录同步写入，盘口缺口可有限次重新同步；未自动重连，不构成正式采集。')


if __name__ == '__main__':
    main()
