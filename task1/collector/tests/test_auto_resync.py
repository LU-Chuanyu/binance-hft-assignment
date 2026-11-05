import io
import json
import threading
import unittest

from journal_capture import capture
from journal_replay import replay
from test_journal_capture import Reader, raw


def snapshot(update_id, quantity):
    return {'lastUpdateId': update_id, 'bids': [['100', quantity]], 'asks': []}


def depth(first, last, quantity):
    return raw('btcusdt@depth@100ms', {
        'U': first, 'u': last, 'b': [['100', quantity]], 'a': [],
    })


class AutoResyncTests(unittest.TestCase):
    def test_messages_continue_to_arrive_while_replacement_snapshot_is_fetched(self):
        class SignallingReader(Reader):
            def __init__(self):
                super().__init__([depth(101, 105, '0.5'), depth(108, 110, '9')])
                self.third_received = threading.Event()

            def receive(self):
                message = super().receive()
                if message.sequence == 3:
                    self.third_received.set()
                return message

        reader = SignallingReader()
        calls = 0

        def fetch():
            nonlocal calls
            calls += 1
            if calls == 1:
                return snapshot(100, '2')
            reader.rows.put(depth(111, 115, '3'))
            if not reader.third_received.wait(0.5):
                raise TimeoutError('获取新快照期间没有接收更新')
            return snapshot(110, '7')

        output = io.StringIO()
        capture(reader, fetch, output, duration=0.1, wait_timeout=0.5)
        result = replay(io.StringIO(output.getvalue()))
        self.assertEqual(result['last_update_id'], 115)
        self.assertEqual(result['gap_count'], 1)

    def test_gap_retries_same_message_then_replay_restores_rebuilt_book(self):
        reader = Reader([depth(101, 105, '0.5'), depth(108, 110, '9'),
                         depth(111, 115, '3')])
        snapshots = iter([snapshot(100, '2'), snapshot(110, '7')])
        output = io.StringIO()
        capture(reader, lambda: next(snapshots), output, duration=0.05, wait_timeout=0.5)
        rows = [json.loads(line) for line in output.getvalue().splitlines()]
        gap = next(row for row in rows if row['type'] == 'depth_gap')
        self.assertEqual((gap['receive_seq'], gap['before_id'], gap['U'], gap['u']),
                         (2, 105, 108, 110))
        self.assertEqual([r['action'] for r in rows if r['type'] == 'depth_action'],
                         ['applied', 'ignored', 'applied'])
        result = replay(io.StringIO(output.getvalue()))
        self.assertEqual((result['gap_count'], result['reset_count'], result['last_update_id']),
                         (1, 1, 115))
        self.assertEqual(result['top']['bids'], [['100', '3']])

    def test_same_message_may_require_more_than_one_snapshot(self):
        reader = Reader([depth(101, 105, '0.5'), depth(108, 110, '9'),
                         depth(111, 115, '3')])
        snapshots = iter([snapshot(100, '2'), snapshot(106, '8'), snapshot(110, '7')])
        output = io.StringIO()
        capture(reader, lambda: next(snapshots), output, duration=0.05, wait_timeout=0.5)
        result = replay(io.StringIO(output.getvalue()))
        self.assertEqual((result['gap_count'], result['reset_count']), (2, 2))

    def test_retry_limit_is_failure_even_if_old_round_was_applied(self):
        reader = Reader([depth(101, 105, '0.5'), depth(108, 110, '9')])
        output = io.StringIO()
        with self.assertRaisesRegex(RuntimeError, '重同步'):
            capture(reader, lambda: snapshot(100, '2'), output, duration=0.05,
                    wait_timeout=0.5, max_resync=2)
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(records[-1]['status'], 'failed')
        with self.assertRaises(ValueError):
            replay(io.StringIO(output.getvalue()))

    def test_reset_without_bridging_update_does_not_report_success(self):
        reader = Reader([depth(101, 105, '0.5'), depth(108, 110, '9')])
        snapshots = iter([snapshot(100, '2'), snapshot(110, '7')])
        output = io.StringIO()
        with self.assertRaisesRegex(TimeoutError, '衔接'):
            capture(reader, lambda: next(snapshots), output, duration=0.05, wait_timeout=0.5)
        self.assertEqual(json.loads(output.getvalue().splitlines()[-1])['status'], 'failed')


if __name__ == '__main__':
    unittest.main()
