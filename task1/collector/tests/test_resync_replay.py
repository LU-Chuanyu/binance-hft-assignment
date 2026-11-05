import unittest

from journal_replay import replay
from test_journal_replay import lines, raw


def rows():
    return [
        {'type': 'snapshot', 'snapshot': {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}},
        raw(1, 'btcusdt@depth@100ms', {'U': 101, 'u': 105, 'b': [['100', '0.5']], 'a': []}),
        {'type': 'depth_action', 'receive_seq': 1, 'action': 'applied', 'before_id': 100, 'after_id': 105},
        raw(2, 'btcusdt@depth@100ms', {'U': 108, 'u': 110, 'b': [['100', '9']], 'a': []}),
        {'type': 'depth_gap', 'receive_seq': 2, 'before_id': 105, 'U': 108, 'u': 110},
        raw(3, 'btcusdt@depth@100ms', {'U': 111, 'u': 115, 'b': [['100', '3']], 'a': []}),
        {'type': 'snapshot_reset', 'snapshot': {'lastUpdateId': 110, 'bids': [['100', '7']], 'asks': []}},
        {'type': 'depth_action', 'receive_seq': 2, 'action': 'ignored', 'before_id': 110, 'after_id': 110},
        {'type': 'depth_action', 'receive_seq': 3, 'action': 'applied', 'before_id': 110, 'after_id': 115},
        {'type': 'end', 'status': 'success', 'received_count': 3, 'processed_depth': 3,
         'pending_depth': 0, 'last_update_id': 115,
         'top': {'bids': [['100', '3']], 'asks': []}},
    ]


class ResyncReplayTests(unittest.TestCase):
    def test_gap_keeps_raw_and_reset_replaces_book_before_ordered_processing(self):
        result = replay(lines(rows()))
        self.assertEqual(result['last_update_id'], 115)
        self.assertEqual(result['top']['bids'], [['100', '3']])
        self.assertEqual((result['applied_count'], result['ignored_count']), (2, 1))
        self.assertEqual((result['gap_count'], result['reset_count']), (1, 1))

    def test_gap_must_match_queue_event_and_really_be_missing(self):
        for key, value in [('receive_seq', 1), ('before_id', 104), ('U', 109), ('u', 111)]:
            sample = rows()
            sample[4][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                replay(lines(sample))
        sample = rows()
        sample[3] = raw(2, 'btcusdt@depth@100ms', {'U': 106, 'u': 110, 'b': [], 'a': []})
        sample[4]['U'] = 106
        with self.assertRaises(ValueError):
            replay(lines(sample))

    def test_cannot_apply_or_end_while_old_book_invalid(self):
        sample = rows()
        del sample[6]
        with self.assertRaises(ValueError):
            replay(lines(sample))
        sample = rows()[:5] + rows()[-1:]
        with self.assertRaises(ValueError):
            replay(lines(sample))

    def test_reset_requires_gap_and_a_bridging_update_before_success(self):
        sample = rows()
        del sample[4]
        with self.assertRaises(ValueError):
            replay(lines(sample))
        sample = rows()
        del sample[8]
        sample[-1].update(processed_depth=2, pending_depth=1, last_update_id=110,
                          top={'bids': [['100', '7']], 'asks': []})
        with self.assertRaises(ValueError):
            replay(lines(sample))

    def test_same_pending_message_can_reveal_another_gap_after_reset(self):
        sample = rows()
        sample.insert(6, {'type': 'snapshot_reset', 'snapshot': {
            'lastUpdateId': 106, 'bids': [['100', '8']], 'asks': []}})
        sample.insert(7, {'type': 'depth_gap', 'receive_seq': 2, 'before_id': 106,
                          'U': 108, 'u': 110})
        result = replay(lines(sample))
        self.assertEqual((result['gap_count'], result['reset_count']), (2, 2))
        self.assertEqual(result['last_update_id'], 115)


if __name__ == '__main__':
    unittest.main()
