import io
import json
import threading
import time
from types import SimpleNamespace
import unittest

from integrated_collector import collect
from journal_replay import replay
from reconnect_stream import StreamClosed, StreamOpened
from test_integrated_collector import FakeStream, depth, message
from trade_flow import TradeFlow


class BlockingStream(FakeStream):
    def next_event(self):
        event = self.events.get()
        if event is None:
            raise StreamClosed('closed')
        return event


def make_stream():
    return BlockingStream([StreamOpened(1, False, True, 10),
                           message(1, 1, 'depth', depth(101, 105, '1'))])


def snapshot():
    return {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}


class IntegratedClockTests(unittest.TestCase):
    def test_clock_sample_is_recorded_and_accepted_by_replay(self):
        clock_started = threading.Event()
        snapshot_ready = threading.Event()

        def get_snapshot():
            # 保证过早提交的校时有机会在快照返回前执行。
            clock_started.wait(0.05)
            snapshot_ready.set()
            return snapshot()

        def get_clock():
            ready = snapshot_ready.is_set()
            clock_started.set()
            if not ready:
                raise RuntimeError('首次校时早于初始快照')
            return SimpleNamespace(data={'serverTime': 1000}, sent_wall_ns=1_100_000_000,
                                   received_wall_ns=1_120_000_000, sent_monotonic_ns=10,
                                   received_monotonic_ns=20_000_010, connection_reused=True)
        output = io.StringIO()
        collect(make_stream(), get_snapshot, TradeFlow(lambda a, b: []), output,
                duration=0.1, wait_timeout=1, clock_query=get_clock,
                clock_interval=1, clock_timeout=0.2)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        row = next(r for r in rows if r['type'] == 'clock_sample')
        self.assertEqual(row['status'], 'ok')
        self.assertEqual(row['offset_ns'], 110_000_000)
        self.assertEqual(row['rtt_ns'], 20_000_000)
        self.assertTrue(row['usable'])
        self.assertIs(row['connection_reused'], True)
        self.assertEqual(replay(io.StringIO(output.getvalue()))['last_update_id'], 105)

    def test_timeout_marks_unusable_without_blocking_message_processing(self):
        def slow():
            time.sleep(0.15)
            raise RuntimeError('late response')
        output = io.StringIO()
        start = time.monotonic()
        collect(make_stream(), snapshot, TradeFlow(lambda a, b: []), output,
                duration=0.06, wait_timeout=1, clock_query=slow,
                clock_interval=1, clock_timeout=0.02)
        self.assertLess(time.monotonic() - start, 0.12)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        row = next(r for r in rows if r['type'] == 'clock_sample')
        self.assertEqual(row['status'], 'timeout')
        self.assertFalse(row['usable'])
        self.assertIsNone(row['offset_ns'])
        self.assertEqual(replay(io.StringIO(output.getvalue()))['last_update_id'], 105)


if __name__ == '__main__':
    unittest.main()
