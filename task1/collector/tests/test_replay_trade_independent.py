import unittest

from journal_replay import replay
from test_journal_replay import lines, raw


def base():
    return [
        {'type': 'snapshot', 'snapshot': {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': []}},
        raw(1, 'btcusdt@depth@100ms', {'U': 101, 'u': 105, 'b': [], 'a': []}),
        {'type': 'depth_action', 'receive_seq': 1, 'action': 'applied', 'before_id': 100, 'after_id': 105},
    ]


def trade(seq, trade_id, status, include=True, missing=None):
    return [raw(seq, 'btcusdt@aggTrade', {'a': trade_id, 'p': '1', 'q': '1', 'T': 1}),
            {'type': 'trade_live', 'receive_seq': seq, 'status': status, 'include': include,
             'missing_ranges': missing or []}]


def end(received):
    return {'type': 'end', 'status': 'success', 'received_count': received,
            'processed_depth': 1, 'pending_depth': 0, 'last_update_id': 105,
            'top': {'bids': [['100', '2']], 'asks': []}}


def backfill(trade_id):
    return {'type': 'trade_backfill', 'record': {'source': 'rest_backfill', 'received_ns': None,
            'data': {'a': trade_id}}}


class IndependentTradeReplayTests(unittest.TestCase):
    def test_fake_continuity_does_not_hide_100_to_102_gap(self):
        rows = base() + trade(2, 100, 'first') + trade(3, 102, 'continuous') + [end(3)]
        with self.assertRaisesRegex(ValueError, '成交'):
            replay(lines(rows))

    def test_forged_empty_remaining_gap_does_not_pass(self):
        rows = base() + trade(2, 100, 'first') + trade(3, 102, 'gap', missing=[[101,101]])
        rows += [{'type': 'trade_repair', 'missing_ranges': [], 'errors': []}, end(3)]
        with self.assertRaisesRegex(ValueError, '成交'):
            replay(lines(rows))

    def test_gap_action_must_record_the_newly_discovered_missing_ids(self):
        rows = base() + trade(2, 100, 'first') + trade(3, 102, 'gap')
        rows += trade(4, 101, 'late') + [end(4)]
        with self.assertRaisesRegex(ValueError, '成交'):
            replay(lines(rows))

    def test_background_filled_before_duplicate_logged_then_backfill_is_valid(self):
        rows = base() + trade(2, 100, 'first') + trade(3, 102, 'gap', missing=[[101,101]])
        rows += trade(4, 101, 'duplicate', include=False)
        rows += [backfill(101), {'type': 'trade_repair', 'missing_ranges': [], 'errors': []}, end(4)]
        self.assertEqual(replay(lines(rows))['last_update_id'], 105)

    def test_real_duplicate_without_any_retained_copy_fails(self):
        rows = base() + trade(2, 100, 'first') + trade(3, 101, 'duplicate', include=False) + [end(3)]
        with self.assertRaisesRegex(ValueError, '成交'):
            replay(lines(rows))

    def test_legacy_raw_aggtrade_gap_detected_without_trade_actions(self):
        rows = base() + [raw(2, 'btcusdt@aggTrade', {'a': 100}), raw(3, 'btcusdt@aggTrade', {'a': 102}), end(3)]
        with self.assertRaisesRegex(ValueError, '成交'):
            replay(lines(rows))

    def test_duplicate_trade_action_is_rejected(self):
        rows = base() + trade(2, 100, 'first')
        rows.append(dict(rows[-1]))
        rows.append(end(2))
        with self.assertRaisesRegex(ValueError, '成交'):
            replay(lines(rows))

    def test_first_trade_cannot_be_discarded_as_duplicate(self):
        rows = base() + trade(2, 100, 'duplicate', include=False) + trade(3, 100, 'first') + [end(3)]
        with self.assertRaisesRegex(ValueError, '成交'):
            replay(lines(rows))


if __name__ == '__main__':
    unittest.main()
