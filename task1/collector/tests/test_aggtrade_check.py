import unittest

from aggtrade_check import AggTradeTracker


class AggTradeTrackerTests(unittest.TestCase):
    def test_continuous_duplicate_and_gap(self):
        tracker = AggTradeTracker()
        self.assertEqual(tracker.observe({'a': 100}), 'first')
        self.assertEqual(tracker.observe({'a': 101}), 'continuous')
        self.assertEqual(tracker.observe({'a': 101}), 'duplicate')
        self.assertEqual(tracker.observe({'a': 105}), 'gap')
        self.assertEqual(tracker.missing_ranges, [(102, 104)])
        self.assertEqual(tracker.highest_id, 105)

    def test_late_arrival_splits_or_removes_missing_range(self):
        tracker = AggTradeTracker()
        tracker.observe({'a': 100})
        tracker.observe({'a': 105})
        self.assertEqual(tracker.observe({'a': 103}), 'late')
        self.assertEqual(tracker.missing_ranges, [(101, 102), (104, 104)])
        self.assertEqual(tracker.observe({'a': 104}), 'late')
        self.assertEqual(tracker.missing_ranges, [(101, 102)])
        self.assertEqual(tracker.observe({'a': 103}), 'duplicate')
        self.assertEqual(tracker.highest_id, 105)

    def test_old_before_baseline_is_not_assumed_duplicate(self):
        tracker = AggTradeTracker()
        tracker.observe({'a': 100})
        self.assertEqual(tracker.observe({'a': 99}), 'older_than_baseline')
        self.assertEqual(tracker.highest_id, 100)
        self.assertEqual(tracker.missing_ranges, [])

    def test_invalid_number_rejected_without_changing_state(self):
        tracker = AggTradeTracker()
        tracker.observe({'a': 1})
        for bad in ({}, {'a': True}, {'a': '2'}, {'a': -1}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                tracker.observe(bad)
        self.assertEqual(tracker.highest_id, 1)
        self.assertEqual(tracker.missing_ranges, [])


if __name__ == '__main__':
    unittest.main()
