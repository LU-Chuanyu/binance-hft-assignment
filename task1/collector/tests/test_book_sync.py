import unittest
from decimal import Decimal

from book_sync import DepthGapError, DepthSynchronizer, UnsynchronizedError


def snapshot():
    return {'lastUpdateId': 100, 'bids': [['100', '2']], 'asks': [['101', '1']]}


def event(first, last, quantity='0.5'):
    return {'U': first, 'u': last, 'b': [['100', quantity]], 'a': []}


class DepthSynchronizerTests(unittest.TestCase):
    def test_discards_covered_events_without_modifying_snapshot(self):
        sync = DepthSynchronizer(snapshot())
        self.assertEqual(sync.apply(event(95, 100)), 'ignored')
        self.assertEqual(sync.last_update_id, 100)
        self.assertEqual(sync.book.top_levels(1)['bids'][0][1], Decimal('2'))

    def test_accepts_first_adjacent_event_and_subsequent_overlap(self):
        sync = DepthSynchronizer(snapshot())
        self.assertEqual(sync.apply(event(101, 105)), 'applied')
        self.assertEqual(sync.last_update_id, 105)
        self.assertEqual(sync.book.top_levels(1)['bids'][0][1], Decimal('0.5'))
        self.assertEqual(sync.apply(event(104, 108, '0.3')), 'applied')
        self.assertEqual(sync.last_update_id, 108)
        self.assertEqual(sync.book.top_levels(1)['bids'][0][1], Decimal('0.3'))
        self.assertEqual(sync.apply(event(102, 108, '9')), 'ignored')
        self.assertEqual(sync.book.top_levels(1)['bids'][0][1], Decimal('0.3'))

    def test_first_event_can_overlap_snapshot(self):
        sync = DepthSynchronizer(snapshot())
        self.assertEqual(sync.apply(event(98, 103)), 'applied')
        self.assertEqual(sync.last_update_id, 103)

    def test_gap_keeps_book_unchanged_and_invalidates_until_new_instance(self):
        sync = DepthSynchronizer(snapshot())
        sync.apply(event(101, 105))
        before = sync.book.top_levels(1)
        with self.assertRaises(DepthGapError):
            sync.apply(event(108, 110, '9'))
        self.assertEqual(sync.book.top_levels(1), before)
        self.assertEqual(sync.last_update_id, 105)
        self.assertFalse(sync.valid)
        with self.assertRaises(UnsynchronizedError):
            sync.apply(event(106, 107))
        fresh = DepthSynchronizer({'lastUpdateId': 110, 'bids': [['100', '8']], 'asks': []})
        self.assertEqual(fresh.apply(event(111, 112)), 'applied')

    def test_malformed_update_is_rejected_without_changing_book(self):
        sync = DepthSynchronizer(snapshot())
        with self.assertRaises(ValueError):
            sync.apply(event(110, 105))
        self.assertEqual(sync.book.top_levels(1)['bids'][0][1], Decimal('2'))
        self.assertFalse(sync.valid)


if __name__ == '__main__':
    unittest.main()
