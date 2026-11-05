import unittest

from book_sync import DepthSynchronizer
from checkpoint_helpers import make_checkpoint
from journal_replay import replay
from test_journal_replay import lines, records


class BookCheckpointTests(unittest.TestCase):
    def test_checkpoint_serializes_twenty_levels_in_priority_order(self):
        snapshot = {'lastUpdateId': 100,
                    'bids': [[str(i), str(i / 10)] for i in range(1, 26)],
                    'asks': [[str(i), '1'] for i in range(101, 126)]}
        sync = DepthSynchronizer(snapshot)
        checkpoint = make_checkpoint(sync)
        self.assertEqual(checkpoint['last_update_id'], 100)
        self.assertEqual(len(checkpoint['top']['bids']), 20)
        self.assertEqual(len(checkpoint['top']['asks']), 20)
        self.assertEqual(checkpoint['top']['bids'][0], ['25', '2.5'])
        self.assertEqual(checkpoint['top']['asks'][0], ['101', '1'])

    def test_replay_checks_each_recorded_checkpoint_and_counts(self):
        rows = records()
        checkpoint = {'type': 'book_checkpoint', 'last_update_id': 105,
                      'top': {'bids': [['100', '0.5']], 'asks': [['101', '1']]}}
        rows.insert(6, checkpoint)
        result = replay(lines(rows))
        self.assertEqual((result['checkpoints_checked'], result['checkpoints_passed']), (1, 1))

    def test_checkpoint_rejects_wrong_id_quantity_price_or_missing_level(self):
        for checkpoint in (
            {'last_update_id': 104, 'top': {'bids': [['100', '0.5']], 'asks': [['101', '1']]}},
            {'last_update_id': 105, 'top': {'bids': [['100', '0.6']], 'asks': [['101', '1']]}},
            {'last_update_id': 105, 'top': {'bids': [['100', '0.5']], 'asks': [['102', '1']]}},
            {'last_update_id': 105, 'top': {'bids': [['100', '0.5']], 'asks': []}},
        ):
            rows = records()
            rows.insert(6, {'type': 'book_checkpoint', **checkpoint})
            with self.subTest(checkpoint=checkpoint), self.assertRaisesRegex(ValueError, '摘要'):
                replay(lines(rows))

    def test_cannot_checkpoint_before_first_bridge_or_after_gap(self):
        checkpoint = {'type': 'book_checkpoint', 'last_update_id': 100,
                      'top': {'bids': [['100', '2']], 'asks': [['101', '1']]}}
        rows = records()
        rows.insert(3, checkpoint)
        with self.assertRaises(ValueError):
            replay(lines(rows))

    def test_old_logs_without_checkpoint_remain_valid(self):
        result = replay(lines(records()))
        self.assertEqual((result['checkpoints_checked'], result['checkpoints_passed']), (0, 0))


if __name__ == '__main__':
    unittest.main()
