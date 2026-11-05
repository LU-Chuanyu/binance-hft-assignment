import io
import json
import threading
import time
import unittest

from integrated_collector import collect
from journal_replay import replay
from reconnect_stream import StreamOpened, StreamLost
from test_integrated_clock import BlockingStream, snapshot
from test_integrated_collector import depth, message, trade
from trade_flow import TradeFlow


class IntegratedValidationTests(unittest.TestCase):
    def test_ahead_snapshot_waits_for_local_progress(self):
        stream = BlockingStream([StreamOpened(1, False, True, 10),
                                 message(1, 1, 'depth', depth(101, 105, '1'))])
        remote = {'lastUpdateId': 110, 'bids': [['100', '1']], 'asks': []}
        timer = threading.Timer(0.16, stream.events.put,
                                args=(message(2, 1, 'depth', depth(106, 110, '1')),))
        timer.start()
        output = io.StringIO()
        try:
            collect(stream, snapshot, TradeFlow(lambda a,b: []), output,
                duration=0.28, wait_timeout=1, validation_query=lambda: remote,
                validation_interval=0.02, validation_max_age=1)
        finally:
            timer.cancel()
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        matched = [r for r in rows if r['type'] == 'book_validation' and r['status'] == 'matched']
        self.assertTrue(matched)
        self.assertGreaterEqual(matched[0]['local_id'], 110)
        self.assertGreaterEqual(replay(io.StringIO(output.getvalue()))['validation_counts']['matched'], 1)

    def test_old_epoch_result_is_not_used_after_disconnect(self):
        stream = BlockingStream([StreamOpened(1, False, True, 10),
                                 message(1, 1, 'depth', depth(101, 105, '1')),
                                 message(2, 1, 'aggTrade', trade(100))])
        snapshots = iter([snapshot(), {'lastUpdateId': 110, 'bids': [['100', '7']], 'asks': []}])
        release = threading.Event()
        def delayed():
            release.wait(1)
            return {'lastUpdateId': 105, 'bids': [['100', '1']], 'asks': []}
        def disconnect():
            for event in (StreamLost(1, 20, 'lost', True), StreamOpened(2, True, True, 30),
                          message(3, 2, 'depth', depth(111, 115, '3')),
                          message(4, 2, 'aggTrade', trade(101))):
                stream.events.put(event)
        timers = [threading.Timer(0.11, disconnect), threading.Timer(0.20, release.set)]
        for t in timers:
            t.start()
        output = io.StringIO()
        try:
            collect(stream, lambda: next(snapshots), TradeFlow(lambda a,b: []), output,
                duration=0.27, wait_timeout=1, validation_query=delayed,
                validation_interval=0.02, validation_max_age=1)
        finally:
            release.set()
            for t in timers:
                t.cancel()
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        old = [r for r in rows if r['type'] == 'book_validation' and r['epoch'] == 1]
        self.assertTrue(old)
        self.assertTrue(all(r['status'] in ('discarded', 'unverifiable') for r in old))
        replay(io.StringIO(output.getvalue()))

    def test_mismatch_invalidates_book_and_rebuilds_with_next_depth(self):
        stream = BlockingStream([StreamOpened(1, False, True, 10),
                                 message(1, 1, 'depth', depth(101, 105, '1'))])
        snapshots = iter([snapshot(), {'lastUpdateId': 110, 'bids': [['100', '7']], 'asks': []}])
        # 交易所快照在 105，买价 100 的数量与本地的 1 不同。
        remote = {'lastUpdateId': 105, 'bids': [['100', '9']], 'asks': []}
        timer = threading.Timer(0.16, stream.events.put,
                                args=(message(2, 1, 'depth', depth(111, 115, '3')),))
        timer.start()
        output = io.StringIO()
        try:
            result = collect(stream, lambda: next(snapshots), TradeFlow(lambda a,b: []), output,
                duration=0.27, wait_timeout=1, validation_query=lambda: remote,
                validation_interval=0.02, validation_max_age=1)
        finally:
            timer.cancel()
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        validations = [r for r in rows if r['type'] == 'book_validation']
        self.assertIn('mismatch', [r['status'] for r in validations])
        self.assertEqual(result['last_update_id'], 115)
        replayed = replay(io.StringIO(output.getvalue()))
        self.assertEqual(replayed['validation_counts']['mismatch'], 1)
        self.assertEqual(replayed['last_update_id'], 115)

    def test_failed_query_is_recorded_as_error_not_match(self):
        stream = BlockingStream([StreamOpened(1, False, True, 10),
                                 message(1, 1, 'depth', depth(101, 105, '1'))])
        def offline():
            raise OSError('exchange snapshot unavailable')
        output = io.StringIO()
        collect(stream, snapshot, TradeFlow(lambda a,b: []), output,
                duration=0.08, wait_timeout=1, validation_query=offline,
                validation_interval=0.02, validation_max_age=1)
        result = replay(io.StringIO(output.getvalue()))
        self.assertGreaterEqual(result['validation_counts']['error'], 1)
        self.assertEqual(result['validation_counts']['matched'], 0)


if __name__ == '__main__':
    unittest.main()
