import io
import json
import unittest

from book_sync import DepthGapError
from journal_replay import replay


def raw(sequence, stream, data):
    return {'type': 'raw', 'receive_seq': sequence, 'received_ns': 100,
            'monotonic_ns': 10, 'raw_text': json.dumps({'stream': stream, 'data': data})}


def lines(rows):
    return io.StringIO(''.join(json.dumps({'journal_seq': i, **row}) + '\n'
                               for i, row in enumerate(rows, 1)))


def records():
    return [
        raw(1, 'btcusdt@depth@100ms', {'U': 95, 'u': 100, 'b': [], 'a': []}),
        raw(2, 'btcusdt@bookTicker', {'b': '100', 'a': '101'}),
        {'type': 'snapshot', 'snapshot': {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': [['101', '1']]}},
        raw(3, 'btcusdt@depth@100ms', {'U': 101, 'u': 105, 'b': [['100', '0.5']], 'a': []}),
        {'type': 'depth_action', 'receive_seq': 1, 'action': 'ignored', 'before_id': 100, 'after_id': 100},
        {'type': 'depth_action', 'receive_seq': 3, 'action': 'applied', 'before_id': 100, 'after_id': 105},
        raw(4, 'btcusdt@depth@100ms', {'U': 106, 'u': 106, 'b': [], 'a': []}),
        {'type': 'end', 'status': 'success', 'received_count': 4, 'processed_depth': 2,
         'pending_depth': 1, 'last_update_id': 105,
         'top': {'bids': [['100', '0.5']], 'asks': [['101', '1']]}}
    ]


class JournalReplayTests(unittest.TestCase):
    def test_replay_restores_book_and_counts_unprocessed_tail(self):
        result = replay(lines(records()))
        self.assertEqual(result['last_update_id'], 105)
        self.assertEqual(result['top'], {'bids': [['100', '0.5']], 'asks': [['101', '1']]})
        self.assertEqual(result['received_count'], 4)
        self.assertEqual(result['pending_depth_count'], 1)
        self.assertEqual(result['applied_count'], 1)

    def test_missing_reference_or_skipped_depth_fails(self):
        rows = records()
        rows[4]['receive_seq'] = 9
        with self.assertRaises(ValueError):
            replay(lines(rows))
        rows = records()
        rows[4]['receive_seq'] = 3
        with self.assertRaises(ValueError):
            replay(lines(rows))

    def test_bad_action_or_checkpoint_fails(self):
        rows = records()
        rows[5]['after_id'] = 106
        with self.assertRaises(ValueError):
            replay(lines(rows))
        rows = records()
        rows[-1]['top']['bids'][0][1] = '42'
        with self.assertRaises(ValueError):
            replay(lines(rows))

    def test_missing_end_or_failed_capture_not_successful(self):
        rows = records()
        with self.assertRaises(ValueError):
            replay(lines(rows[:-1]))
        rows[-1]['status'] = 'failed'
        with self.assertRaises(ValueError):
            replay(lines(rows))

    def test_missing_final_ignored_action_does_not_silently_pass(self):
        rows = records()
        rows[6] = raw(4, 'btcusdt@depth@100ms', {'U': 100, 'u': 105, 'b': [], 'a': []})
        rows.insert(7, {'type': 'depth_action', 'receive_seq': 4, 'action': 'ignored',
                        'before_id': 105, 'after_id': 105})
        rows[-1]['processed_depth'] = 3
        rows[-1]['pending_depth'] = 0
        replay(lines(rows))
        del rows[7]  # 没有改变盘口，但结束记录仍记载这条消息已处理。
        with self.assertRaises(ValueError):
            replay(lines(rows))

    def test_exchange_gap_raises_and_local_received_gap_rejected(self):
        rows = records()
        rows[3] = raw(3, 'btcusdt@depth@100ms', {'U': 103, 'u': 105, 'b': [], 'a': []})
        with self.assertRaises(DepthGapError):
            replay(lines(rows))
        rows = records()
        rows[3]['receive_seq'] = 4
        with self.assertRaises(ValueError):
            replay(lines(rows))


if __name__ == '__main__':
    unittest.main()
