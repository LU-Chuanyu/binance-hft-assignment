import io
import json
import queue
import unittest

from journal_capture import capture
from market_stream import RawMessage


def raw(stream, data):
    return json.dumps({'stream': stream, 'data': data})


class Reader:
    def __init__(self, rows):
        self.rows = queue.Queue()
        for row in rows:
            self.rows.put(row)
        self.sequence = 0

    def receive(self):
        row = self.rows.get(timeout=1)
        if row is None:
            raise ConnectionError('closed')
        self.sequence += 1
        return RawMessage(self.sequence, self.sequence * 100, self.sequence * 10, row)

    def close(self):
        self.rows.put(None)


class CaptureTests(unittest.TestCase):
    def test_journal_contains_raw_snapshot_actions_and_checkpoint(self):
        rows = [
            raw('btcusdt@bookTicker', {'b': '100', 'a': '101'}),
            raw('btcusdt@depth@100ms', {'U': 98, 'u': 100, 'b': [], 'a': []}),
            raw('btcusdt@aggTrade', {'a': 12, 'p': '100'}),
            raw('btcusdt@depth@100ms', {'U': 101, 'u': 105, 'b': [['100', '0.5']], 'a': []}),
        ]
        snapshot = {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': [['101', '1']]}
        output = io.StringIO()
        capture(Reader(rows), lambda: snapshot, output, duration=0.05, wait_timeout=0.5)
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([r['journal_seq'] for r in records], list(range(1, len(records)+1)))
        self.assertEqual(len([r for r in records if r['type'] == 'raw']), 4)
        self.assertEqual([r['snapshot'] for r in records if r['type'] == 'snapshot'], [snapshot])
        actions = [r for r in records if r['type'] == 'depth_action']
        self.assertEqual([(r['receive_seq'], r['action']) for r in actions], [(2, 'ignored'), (4, 'applied')])
        self.assertEqual(records[-1]['last_update_id'], 105)
        self.assertEqual(records[-1]['top']['bids'], [['100', '0.5']])

    def test_gap_recorded_failed_not_success(self):
        rows = [raw('btcusdt@depth@100ms', {'U': 103, 'u': 105, 'b': [], 'a': []})]
        snapshot = {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}
        output = io.StringIO()
        with self.assertRaises(Exception):
            capture(Reader(rows), lambda: snapshot, output, duration=0.05, wait_timeout=0.5)
        self.assertEqual(json.loads(output.getvalue().splitlines()[-1])['status'], 'failed')


if __name__ == '__main__':
    unittest.main()
