import io
import json
import queue
import time
import threading
import unittest

from integrated_collector import collect
from aggtrade_backfill import BackfillError
from journal_replay import replay
from market_stream import RawMessage
from reconnect_stream import ReconnectingStream, StreamClosed, StreamOpened, StreamLost, StreamMessage
from trade_flow import TradeFlow


def message(seq, generation, kind, data):
    names = {'depth': 'btcusdt@depth@100ms', 'aggTrade': 'btcusdt@aggTrade'}
    data = {'s': 'BTCUSDT', 'E': 1000, **data}
    raw = json.dumps({'stream': names[kind], 'data': data})
    return StreamMessage(generation, RawMessage(seq, seq * 100, seq * 10, raw))


def depth(U, u, quantity):
    return {'U': U, 'u': u, 'b': [['100', quantity]], 'a': []}


def trade(a):
    return {'a': a, 'p': '100', 'q': '0.2', 'T': 1000, 'm': False, 'f': a, 'l': a}


class FakeStream:
    def __init__(self, events):
        self.events = queue.Queue()
        for event in events:
            self.events.put(event)
        self.closed = False

    def next_event(self):
        event = self.events.get(timeout=1)
        if event is None:
            raise StreamClosed('closed')
        if isinstance(event, Exception):
            raise event
        return event

    def close(self):
        self.closed = True
        self.events.put(None)


class IntegratedCollectorTests(unittest.TestCase):
    def test_followup_overlap_resynchronizes_and_replays_with_same_rule(self):
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(98, 105, '1')),
                  message(2, 1, 'depth', depth(104, 108, '2')),
                  message(3, 1, 'depth', depth(109, 110, '3'))]
        snapshots = iter([{'lastUpdateId': 100, 'bids': [], 'asks': []},
                          {'lastUpdateId': 107, 'bids': [], 'asks': []}])
        output = io.StringIO()
        collect(FakeStream(events), lambda: next(snapshots),
                TradeFlow(lambda a, b: []), output, duration=0.05, wait_timeout=0.5)

        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        gaps = [(row['before_id'], row['U'], row['u']) for row in rows if row['type'] == 'depth_gap']
        self.assertEqual(gaps, [(105, 104, 108)])
        result = replay(io.StringIO(output.getvalue()))
        self.assertEqual((result['gap_count'], result['reset_count'], result['last_update_id']),
                         (1, 1, 110))
        self.assertEqual(result['top']['bids'], [['100', '3']])

    def test_receive_error_during_shutdown_cannot_report_success(self):
        class CorruptOnClose(FakeStream):
            def __init__(self, events):
                super().__init__(events)
                self.stop = threading.Event()
            def next_event(self):
                try:
                    return self.events.get_nowait()
                except queue.Empty:
                    if not self.stop.wait(2):
                        raise TimeoutError('test did not close')
                    raise ValueError('corrupt data received')
            def close(self):
                self.stop.set()
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1'))]
        output = io.StringIO()
        with self.assertRaisesRegex(RuntimeError, 'corrupt data'):
            collect(CorruptOnClose(events),
                    lambda: {'lastUpdateId': 100, 'bids': [], 'asks': []},
                    TradeFlow(lambda a,b: []), output, duration=0.05, wait_timeout=0.5)
        self.assertEqual(json.loads(output.getvalue().splitlines()[-1])['status'], 'failed')

    def test_successfully_received_message_during_close_is_drained(self):
        class RacingReader:
            def __init__(self):
                self.connection = self
                self.closed = threading.Event()
                self.n = 0
            def settimeout(self, value):
                pass
            def receive(self):
                self.n += 1
                if self.n == 1:
                    text = message(1, 1, 'depth', depth(101, 105, '1')).message.raw_text
                else:
                    text = message(2, 1, 'aggTrade', trade(100)).message.raw_text
                    if not self.closed.wait(2):
                        raise TimeoutError('test did not close')
                return RawMessage(self.n, 100, 200, text)
            def close(self):
                self.closed.set()
        reader = RacingReader()
        output = io.StringIO()
        result = collect(ReconnectingStream(connector=lambda: reader),
                         lambda: {'lastUpdateId': 100, 'bids': [], 'asks': []},
                         TradeFlow(lambda a,b: []), output, duration=0.05, wait_timeout=0.5)
        self.assertEqual(result['received_count'], 2)
        self.assertEqual(replay(io.StringIO(output.getvalue()))['counts']['aggTrade'], 1)

    def test_duration_starts_after_initial_snapshot(self):
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1'))]
        snapshot = {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
        def slow_snapshot():
            time.sleep(0.06)
            return snapshot
        output = io.StringIO()
        start = time.monotonic()
        result = collect(FakeStream(events), slow_snapshot, TradeFlow(lambda a, b: []),
                         output, duration=0.02, wait_timeout=0.5)
        self.assertGreaterEqual(time.monotonic() - start, 0.075)
        self.assertEqual(result['last_update_id'], 105)

    def test_disconnect_rebuild_and_trade_gap_repair_are_recorded_and_replayed(self):
        events = [
            StreamOpened(1, False, True, 10),
            message(1, 1, 'depth', depth(101, 105, '0.5')),
            message(2, 1, 'aggTrade', trade(100)),
            StreamLost(1, 20, 'lost', True),
            StreamOpened(2, True, True, 30),
            message(3, 2, 'depth', depth(111, 115, '3')),
            message(4, 2, 'aggTrade', trade(102)),
        ]
        snapshots = iter([
            {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []},
            {'lastUpdateId': 110, 'bids': [['100', '7']], 'asks': []},
        ])
        flow = TradeFlow(lambda first, limit: [trade(101)])
        output = io.StringIO()
        result = collect(FakeStream(events), lambda: next(snapshots), flow, output,
                         duration=0.1, wait_timeout=0.5)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([r['type'] for r in rows].count('connection_lost'), 1)
        self.assertEqual([r['type'] for r in rows].count('snapshot_reset'), 1)
        backfill = [r for r in rows if r['type'] == 'trade_backfill']
        self.assertEqual([r['record']['data']['a'] for r in backfill], [101])
        self.assertEqual(backfill[0]['record']['source'], 'rest_backfill')
        self.assertIsNone(backfill[0]['record']['received_ns'])
        self.assertEqual(result['missing_ranges'], [])
        result = replay(io.StringIO(output.getvalue()))
        self.assertEqual(result['last_update_id'], 115)
        self.assertEqual(result['top']['bids'], [['100', '3']])

    def test_disconnect_without_recovered_book_fails(self):
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1')),
                  StreamLost(1, 20, 'lost', True)]
        snapshot = {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
        output = io.StringIO()
        with self.assertRaisesRegex(RuntimeError, '同步'):
            collect(FakeStream(events), lambda: snapshot, TradeFlow(lambda a, b: []),
                    output, duration=0.05, wait_timeout=0.5)
        self.assertEqual(json.loads(output.getvalue().splitlines()[-1])['status'], 'failed')

    def test_disconnect_without_new_trade_cannot_claim_trade_completeness(self):
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1')),
                  message(2, 1, 'aggTrade', trade(100)),
                  StreamLost(1, 20, 'lost', True), StreamOpened(2, True, True, 30),
                  message(3, 2, 'depth', depth(111, 115, '1'))]
        snapshots = iter([{'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []},
                          {'lastUpdateId': 110, 'bids': [['100', '2']], 'asks': []}])
        output = io.StringIO()
        with self.assertRaisesRegex(RuntimeError, '成交.*未知'):
            collect(FakeStream(events), lambda: next(snapshots), TradeFlow(lambda a,b: []),
                    output, duration=0.05, wait_timeout=0.5)
        self.assertEqual(json.loads(output.getvalue().splitlines()[-1])['status'], 'failed')

    def test_late_trade_after_empty_backfill_has_consistent_replay(self):
        finished = threading.Event()
        class LateStream(FakeStream):
            def next_event(self):
                item = super().next_event()
                if isinstance(item, StreamMessage) and item.message.sequence == 4:
                    if not finished.wait(1):
                        raise TimeoutError('backfill did not start')
                    time.sleep(0.02)
                return item
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1')),
                  message(2, 1, 'aggTrade', trade(100)),
                  message(3, 1, 'aggTrade', trade(102)),
                  message(4, 1, 'aggTrade', trade(101))]
        def empty_page(first, limit):
            finished.set()
            return []
        output = io.StringIO()
        collect(LateStream(events), lambda: {'lastUpdateId': 100, 'bids': [], 'asks': []},
                TradeFlow(empty_page), output, duration=0.15, wait_timeout=0.5)
        self.assertEqual(replay(io.StringIO(output.getvalue()))['last_update_id'], 105)

    def test_missing_trade_that_cannot_be_repaired_reports_failure(self):
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1')),
                  message(2, 1, 'aggTrade', trade(100)),
                  message(3, 1, 'aggTrade', trade(102))]
        snapshot = {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
        output = io.StringIO()
        with self.assertRaisesRegex(RuntimeError, '补查'):
            collect(FakeStream(events), lambda: snapshot, TradeFlow(lambda a, b: []),
                    output, duration=0.05, wait_timeout=0.5)
        self.assertEqual(json.loads(output.getvalue().splitlines()[-1])['status'], 'failed')

    def test_rate_limited_backfill_is_not_immediately_retried(self):
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1')),
                  message(2, 1, 'aggTrade', trade(100)),
                  message(3, 1, 'aggTrade', trade(102))]
        snapshot = {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
        calls = []
        def limited(first, limit):
            calls.append(first)
            raise BackfillError('rate limited', status=429, retry_after='120')
        with self.assertRaisesRegex(RuntimeError, '补查'):
            collect(FakeStream(events), lambda: snapshot, TradeFlow(limited),
                    io.StringIO(), duration=0.05, wait_timeout=0.5, rate_retry_timeout=1)
        self.assertEqual(calls, [101])


if __name__ == '__main__':
    unittest.main()
