import json
import queue
import unittest

from book_sync import DepthGapError
from live_book_sync import run_sync
from market_stream import RawMessage


def depth(first, last, quantity='0.5'):
    data = {'U': first, 'u': last, 'b': [['100', quantity]], 'a': []}
    return json.dumps({'stream': 'btcusdt@depth@100ms', 'data': data})


class FakeReader:
    def __init__(self, messages):
        self.messages = queue.Queue()
        for message in messages:
            self.messages.put(message)
        self.closed = False
        self.n = 0

    def receive(self):
        text = self.messages.get(timeout=1)
        if text is None:
            raise ConnectionError('closed')
        if isinstance(text, Exception):
            raise text
        self.n += 1
        return RawMessage(self.n, 0, 0, text)

    def close(self):
        self.closed = True
        self.messages.put(None)


class LiveBookSyncTests(unittest.TestCase):
    def snapshot(self):
        return {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': [['101', '1']]}

    def test_first_depth_is_received_before_snapshot_then_stale_and_bridging_applied(self):
        reader = FakeReader([depth(95, 100), depth(101, 105), depth(106, 108, '0.3')])
        def fetch():
            self.assertGreaterEqual(reader.n, 1)
            return self.snapshot()
        result = run_sync(reader, fetch, duration=0.05, wait_timeout=0.5)
        self.assertEqual(result['first_applied'], (101, 105))
        self.assertEqual(result['last_update_id'], 108)
        self.assertEqual(result['ignored'], 1)
        self.assertEqual(str(result['top']['bids'][0][1]), '0.3')
        self.assertTrue(reader.closed)

    def test_gap_is_reported_and_reader_closed(self):
        reader = FakeReader([depth(101, 105), depth(108, 110)])
        with self.assertRaises(DepthGapError):
            run_sync(reader, self.snapshot, duration=0.05, wait_timeout=0.5)
        self.assertTrue(reader.closed)

    def test_timeout_no_initial_depth_and_reader_closed(self):
        reader = FakeReader([])
        with self.assertRaisesRegex(TimeoutError, '等待'):
            run_sync(reader, self.snapshot, duration=0.05, wait_timeout=0.05)
        self.assertTrue(reader.closed)

    def test_snapshot_failure_closes_reader(self):
        reader = FakeReader([depth(101, 105)])
        def fail():
            raise OSError('snapshot unavailable')
        with self.assertRaisesRegex(OSError, 'snapshot unavailable'):
            run_sync(reader, fail, duration=0.05, wait_timeout=0.5)
        self.assertTrue(reader.closed)

    def test_receive_error_is_reported_not_treated_as_success(self):
        reader = FakeReader([ConnectionError('connection lost')])
        with self.assertRaisesRegex(RuntimeError, 'connection lost'):
            run_sync(reader, self.snapshot, duration=0.05, wait_timeout=0.5)
        self.assertTrue(reader.closed)


if __name__ == '__main__':
    unittest.main()
