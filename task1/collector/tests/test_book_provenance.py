import unittest
from decimal import Decimal

from book_provenance import BookProvenance
from book_validation import compare_exchange_snapshot


def snapshot(seq=100):
    return {'lastUpdateId': seq, 'bids': [['100', '2'], ['99', '3']],
            'asks': [['101', '1']]}


class BookProvenanceTests(unittest.TestCase):
    def test_snapshot_only_records_known_levels_and_initial_provenance(self):
        tracker = BookProvenance(snapshot())
        self.assertEqual(tracker.initial_id, 100)
        self.assertEqual(tracker.current_id, 100)
        self.assertEqual(tracker.known['bids'][Decimal('100')], Decimal('2'))
        self.assertIsNone(tracker.last_changes['bids'][Decimal('100')])
        self.assertNotIn(Decimal('98'), tracker.known['bids'])

    def test_applied_changes_track_zero_new_levels_and_range(self):
        tracker = BookProvenance(snapshot())
        tracker.observe_applied({'U': 101, 'u': 105, 'b': [['100', '0'], ['98', '1']], 'a': []})
        self.assertEqual(tracker.known['bids'][Decimal('100')], Decimal(0))
        self.assertEqual(tracker.known['bids'][Decimal('98')], Decimal('1'))
        self.assertEqual(tracker.last_changes['bids'][Decimal('100')], (101, 105))
        self.assertEqual(tracker.current_id, 105)

    def test_gap_stale_and_invalid_updates_never_change_state(self):
        tracker = BookProvenance(snapshot())
        for event in ({'U': 102, 'u': 103, 'b': [['100', '9']], 'a': []},
                      {'U': 90, 'u': 100, 'b': [['100', '9']], 'a': []},
                      {'U': 101, 'u': 105, 'b': [['100', '8'], ['98', 'bad']], 'a': []}):
            with self.subTest(event=event), self.assertRaises((ValueError, ArithmeticError)):
                tracker.observe_applied(event)
            self.assertEqual(tracker.known['bids'][Decimal('100')], Decimal('2'))
            self.assertEqual(tracker.current_id, 100)

    def test_snapshot_comparison_skips_overlapping_and_unknown_levels(self):
        tracker = BookProvenance(snapshot())
        tracker.observe_applied({'U': 101, 'u': 105, 'b': [['100', '0'], ['99', '4']], 'a': []})
        remote = {'lastUpdateId': 103, 'bids': [['100', '2'], ['99', '3'], ['98', '5']],
                  'asks': [['101', '1']]}
        result = compare_exchange_snapshot(remote, tracker.known, tracker.last_changes,
            initial_id=tracker.initial_id, local_id=tracker.current_id, contiguous=True)
        self.assertEqual(result['compared'], 1)
        self.assertEqual({x['reason'] for x in result['skipped']},
                         {'overlaps_snapshot', 'local_level_unknown'})

    def test_reset_returns_new_epoch_with_fresh_known_levels(self):
        tracker = BookProvenance(snapshot(), epoch=7)
        tracker.observe_applied({'U': 101, 'u': 105, 'b': [['100', '0']], 'a': []})
        next_round = tracker.reset({'lastUpdateId': 110, 'bids': [['98', '4']], 'asks': []})
        self.assertEqual(next_round.epoch, 8)
        self.assertEqual((next_round.initial_id, next_round.current_id), (110, 110))
        self.assertNotIn(Decimal('100'), next_round.known['bids'])
        self.assertEqual(tracker.current_id, 105)


if __name__ == '__main__':
    unittest.main()
