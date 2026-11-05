import unittest

from journal_replay import replay
from test_journal_replay import lines, raw


def event(status='matched', *, validation_id=1, epoch=1):
    snap = {'lastUpdateId': 105, 'bids': [['100', '0.5' if status == 'matched' else '9']], 'asks': []}
    differences = [] if status == 'matched' else [{'side': 'bids', 'price': '100',
                                                    'local_qty': '0.5', 'snapshot_qty': '9'}]
    return {'type': 'book_validation', 'validation_id': validation_id, 'status': status,
            'epoch': epoch, 'snapshot_id': 105, 'local_id': 105, 'snapshot': snap,
            'eligible': True, 'compared': 1, 'matches': status == 'matched',
            'differences': differences, 'skipped': []}


def base():
    return [
        {'type': 'snapshot', 'snapshot': {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}},
        raw(1, 'btcusdt@depth@100ms', {'U': 101, 'u': 105, 'b': [['100', '0.5']], 'a': []}),
        {'type': 'depth_action', 'receive_seq': 1, 'action': 'applied', 'before_id': 100, 'after_id': 105},
    ]


def end():
    return {'type': 'end', 'status': 'success', 'received_count': 1, 'processed_depth': 1,
            'pending_depth': 0, 'last_update_id': 105, 'top': {'bids': [['100', '0.5']], 'asks': []}}


class ValidationReplayTests(unittest.TestCase):
    def test_matched_comparison_is_reproduced_for_known_levels(self):
        result = replay(lines(base() + [event(), end()]))
        self.assertEqual(result['validation_counts']['matched'], 1)

    def test_mismatch_invalidates_book_then_new_snapshot_and_update_recover(self):
        rows = base() + [event('mismatch'),
            {'type': 'snapshot_reset', 'snapshot': {'lastUpdateId': 110, 'bids': [['100', '7']], 'asks': []}},
            raw(2, 'btcusdt@depth@100ms', {'U': 111, 'u': 115, 'b': [['100', '3']], 'a': []}),
            {'type': 'depth_action', 'receive_seq': 2, 'action': 'applied', 'before_id': 110, 'after_id': 115},
            {'type': 'end', 'status': 'success', 'received_count': 2, 'processed_depth': 2,
             'pending_depth': 0, 'last_update_id': 115, 'top': {'bids': [['100', '3']], 'asks': []}}]
        result = replay(lines(rows))
        self.assertEqual(result['validation_counts']['mismatch'], 1)
        self.assertEqual(result['reset_count'], 1)
        with self.assertRaises(ValueError):
            replay(lines(base() + [event('mismatch'), end()]))

    def test_cannot_forge_comparison_or_repeat_validation_id(self):
        forged = event()
        forged['snapshot']['bids'][0][1] = '9'
        with self.assertRaises(ValueError):
            replay(lines(base() + [forged, end()]))
        with self.assertRaises(ValueError):
            replay(lines(base() + [event(), event(), end()]))

    def test_error_unknown_and_unverifiable_are_not_successes(self):
        outcomes = [{'type': 'book_validation', 'validation_id': i, 'epoch': 1,
                     'status': status, 'reason': 'no evidence'}
                    for i, status in enumerate(('unverifiable', 'error'), 1)]
        result = replay(lines(base() + outcomes + [end()]))
        self.assertEqual(result['validation_counts']['matched'], 0)
        self.assertEqual(result['validation_counts']['unverifiable'], 1)
        self.assertEqual(result['validation_counts']['error'], 1)
        outcomes[0]['status'] = 'mystery'
        with self.assertRaises(ValueError):
            replay(lines(base() + outcomes + [end()]))

    def test_zero_comparisons_cannot_be_declared_matched(self):
        row = event()
        row['compared'] = 0
        with self.assertRaises(ValueError):
            replay(lines(base() + [row, end()]))

    def test_discarded_event_must_refer_to_older_epoch(self):
        discarded = {'type': 'book_validation', 'validation_id': 2, 'epoch': 1,
                     'status': 'discarded', 'reason': '盘口轮次已变化'}
        with self.assertRaises(ValueError):
            replay(lines(base() + [discarded, end()]))
        rows = base() + [event('mismatch'),
            {'type': 'snapshot_reset', 'snapshot': {'lastUpdateId': 110, 'bids': [['100', '7']], 'asks': []}},
            discarded,
            raw(2, 'btcusdt@depth@100ms', {'U': 111, 'u': 115, 'b': [['100', '3']], 'a': []}),
            {'type': 'depth_action', 'receive_seq': 2, 'action': 'applied', 'before_id': 110, 'after_id': 115},
            {'type': 'end', 'status': 'success', 'received_count': 2, 'processed_depth': 2,
             'pending_depth': 0, 'last_update_id': 115, 'top': {'bids': [['100', '3']], 'asks': []}}]
        result = replay(lines(rows))
        self.assertEqual(result['validation_counts']['discarded'], 1)
        self.assertEqual(result['validation_events'][1]['reason'], '盘口轮次已变化')


if __name__ == '__main__':
    unittest.main()
