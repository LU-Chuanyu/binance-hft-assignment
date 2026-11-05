"""教学短测：先缓存实时 depth，再获取快照，检查更新编号。

首次衔接按作业的 U <= S+1 <= u；官方初始化文字的边界差异仍待核实。
不是正式采集器：不保存数据、不自动重连或重同步。
"""

import json
from pathlib import Path
import queue
import ssl
import sys
import threading
import time
from urllib.request import urlopen

from book_sync import DepthSynchronizer
from market_stream import connect, decode_message


SNAPSHOT_URL = 'https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20'


def fetch_snapshot():
    ca_file = Path('/etc/ssl/cert.pem')
    if sys.platform == 'darwin' and ca_file.is_file():
        context = ssl.create_default_context(cafile=str(ca_file))
    else:
        context = ssl.create_default_context()
    with urlopen(SNAPSHOT_URL, timeout=10, context=context) as response:
        return json.load(response)


def run_sync(reader, get_snapshot, duration=3, wait_timeout=5):
    """后台先接收缓存；快照返回后，顺序应用缓存和后续 depth 消息。"""
    events = queue.Queue(maxsize=5000)
    stop = threading.Event()
    errors = []

    def receive_loop():
        try:
            while not stop.is_set():
                message = reader.receive()
                event = decode_message(message)
                if event.kind == 'depth':
                    events.put_nowait(event.data)
        except Exception as exc:
            if not stop.is_set():
                errors.append(exc)
                stop.set()

    def get_event(timeout):
        end = time.monotonic() + timeout
        while True:
            if errors:
                raise RuntimeError(f'接收线程失败：{errors[0]}') from errors[0]
            remaining = end - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('等待盘口更新超时')
            try:
                return events.get(timeout=min(remaining, 0.1))
            except queue.Empty:
                continue

    thread = threading.Thread(target=receive_loop, daemon=True)
    thread.start()
    try:
        # 收到首条 depth 后请求快照；请求期间线程持续把消息放进队列。
        first_received = get_event(wait_timeout)
        snapshot = get_snapshot()
        snapshot_id = snapshot['lastUpdateId']
        sync = DepthSynchronizer(snapshot)
        first_applied = None
        ignored = applied = 0
        pending = first_received
        deadline = time.monotonic() + duration
        while True:
            if errors:
                raise RuntimeError(f'接收线程失败：{errors[0]}') from errors[0]
            if pending is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    pending = get_event(remaining)
                except TimeoutError:
                    break
            result = sync.apply(pending)
            if result == 'ignored':
                ignored += 1
            else:
                applied += 1
                if first_applied is None:
                    first_applied = (pending['U'], pending['u'])
            pending = None
        if errors:
            raise RuntimeError(f'接收线程失败：{errors[0]}') from errors[0]
        if first_applied is None:
            raise TimeoutError('在短测时间内没有等到能接上快照的盘口更新')
        return {'snapshot_id': snapshot_id, 'first_applied': first_applied,
                'ignored': ignored, 'applied': applied,
                'last_update_id': sync.last_update_id,
                'top': sync.book.top_levels(1)}
    finally:
        stop.set()
        try:
            reader.close()
        finally:
            thread.join(timeout=2)
        if thread.is_alive():
            raise RuntimeError('接收线程未能正常结束')
        if errors:
            raise RuntimeError(f'接收线程失败：{errors[0]}') from errors[0]


def main():
    print('开始教学短测：先接收缓存 depth，再请求 20 档快照。', flush=True)
    result = run_sync(connect(), fetch_snapshot)
    first, last = result['first_applied']
    print(f"快照编号 S：{result['snapshot_id']}")
    print(f"首次应用范围：[{first}, {last}]；采用作业 U ≤ S+1 ≤ u 判断。")
    if first == result['snapshot_id'] + 1:
        print('提示：U=S+1 边界与官方首次初始化文字的差异仍待核实。')
    print(f"忽略已覆盖 {result['ignored']} 条；应用 {result['applied']} 条；本地进度 {result['last_update_id']}")
    print(f"本地前一档：{result['top']}")
    print('仅为有限快照的短测，未保存数据，也不是完整性验收。')


if __name__ == '__main__':
    main()
