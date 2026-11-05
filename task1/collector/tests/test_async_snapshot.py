import io
import json
import threading
import time
import unittest

from integrated_collector import collect
from journal_replay import replay
from reconnect_stream import StreamOpened, StreamLost
from test_integrated_clock import BlockingStream
from test_integrated_collector import depth, message, trade
from trade_flow import TradeFlow
from api_client import RateLimitError


class ObservableFlow(TradeFlow):
    def __init__(self, observed):
        super().__init__(lambda a, b: [])
        self.observed = observed
    def observe_live(self, event, received_ns):
        result = super().observe_live(event, received_ns)
        self.observed.set()
        return result


class AsyncSnapshotTests(unittest.TestCase):
    def test_rate_limited_snapshot_is_failure_not_success(self):
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1'))]
        calls = []
        def snapshot():
            calls.append(1)
            raise RateLimitError(429, 120)
        output = io.StringIO()
        with self.assertRaises(RateLimitError):
            collect(BlockingStream(events), snapshot, TradeFlow(lambda a,b: []), output,
                    duration=0.05, wait_timeout=1, rate_retry_timeout=1)
        self.assertEqual(calls, [1])
        self.assertEqual(json.loads(output.getvalue().splitlines()[-1])['status'], 'failed')

    def test_timeout_is_failure_and_does_not_fake_sync(self):
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1'))]
        def snapshot():
            time.sleep(0.15)
            return {'lastUpdateId': 100, 'bids': [], 'asks': []}
        output = io.StringIO()
        with self.assertRaises(TimeoutError):
            collect(BlockingStream(events), snapshot, TradeFlow(lambda a,b: []), output,
                    duration=0.05, wait_timeout=1, snapshot_timeout=0.03)
        self.assertEqual(json.loads(output.getvalue().splitlines()[-1])['status'], 'failed')

    def test_snapshot_deadline_differs_from_waiting_for_first_message(self):
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1'))]
        def snapshot():
            time.sleep(0.08)
            return {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
        output = io.StringIO()
        start = time.monotonic()
        result = collect(BlockingStream(events), snapshot, TradeFlow(lambda a,b: []), output,
                         duration=0.03, wait_timeout=0.04, snapshot_timeout=1)
        self.assertEqual(result['last_update_id'], 105)
        self.assertGreaterEqual(time.monotonic() - start, 0.105)

    def test_initial_snapshot_wait_does_not_block_trade_processing(self):
        observed = threading.Event()
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1')),
                  message(2, 1, 'aggTrade', trade(100))]
        def snapshot():
            if not observed.wait(0.5):
                raise TimeoutError('trade was blocked by snapshot')
            return {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
        output = io.StringIO()
        collect(BlockingStream(events), snapshot, ObservableFlow(observed), output,
                duration=0.05, wait_timeout=1)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertLess(next(i for i,r in enumerate(rows) if r['type'] == 'trade_live'),
                        next(i for i,r in enumerate(rows) if r['type'] == 'snapshot'))
        self.assertEqual(replay(io.StringIO(output.getvalue()))['last_update_id'], 105)

    def test_gap_snapshot_wait_does_not_block_trade_processing(self):
        observed = threading.Event()
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1')),
                  message(2, 1, 'depth', depth(108, 110, '9')),
                  message(3, 1, 'aggTrade', trade(100)),
                  message(4, 1, 'depth', depth(111, 115, '3'))]
        calls = []
        def snapshot():
            calls.append(1)
            if len(calls) == 1:
                return {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
            if not observed.wait(0.5):
                raise TimeoutError('trade was blocked by resync')
            return {'lastUpdateId': 110, 'bids': [['100', '7']], 'asks': []}
        output = io.StringIO()
        collect(BlockingStream(events), snapshot, ObservableFlow(observed), output,
                duration=0.1, wait_timeout=1)
        self.assertEqual(replay(io.StringIO(output.getvalue()))['last_update_id'], 115)

    def test_loss_waits_for_pending_depth_then_reconnects_without_dropping(self):
        release = threading.Event()
        events = [StreamOpened(1, False, True, 10),
                  message(1, 1, 'depth', depth(101, 105, '1')),
                  message(2, 1, 'aggTrade', trade(100)),
                  StreamLost(1, 20, 'lost', True), StreamOpened(2, True, True, 30),
                  message(3, 2, 'depth', depth(111, 115, '3')),
                  message(4, 2, 'aggTrade', trade(101))]
        calls = []
        def snapshot():
            calls.append(1)
            if len(calls) == 1:
                if not release.wait(1):
                    raise TimeoutError('test did not release snapshot')
                return {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
            return {'lastUpdateId': 110, 'bids': [['100', '7']], 'asks': []}
        timer = threading.Timer(0.05, release.set)
        timer.start()
        output = io.StringIO()
        try:
            collect(BlockingStream(events), snapshot, TradeFlow(lambda a,b: []), output,
                    duration=0.15, wait_timeout=1)
        finally:
            timer.cancel()
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        actions = [i for i,r in enumerate(rows) if r['type'] == 'depth_action' and r['receive_seq'] == 1]
        loss = next(i for i,r in enumerate(rows) if r['type'] == 'connection_lost')
        self.assertLess(actions[0], loss)
        self.assertEqual(replay(io.StringIO(output.getvalue()))['last_update_id'], 115)


if __name__ == '__main__':
    unittest.main()
