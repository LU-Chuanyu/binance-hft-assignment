import unittest

from book_validation import compare_top20, compare_exchange_snapshot


class BookValidationTests(unittest.TestCase):
    def test_checkpoint_compare_each_level_and_missing_level(self):
        captured = {'bids': [['100.0', '2'], ['99', '3']], 'asks': [['101', '1']]}
        same = {'bids': [['100', '2.000'], ['99', '3']], 'asks': [['101', '1']]}
        self.assertEqual(compare_top20(captured, same)['differences'], [])
        bad = {'bids': [['100', '2'], ['99', '4']], 'asks': []}
        result = compare_top20(captured, bad)
        self.assertFalse(result['matches'])
        self.assertEqual([(r['side'], r['level']) for r in result['differences']],
                         [('bids', 2), ('asks', 1)])

    def test_snapshot_comparison_only_uses_proven_unchanged_and_covered_prices(self):
        snap = {'lastUpdateId': 105,
                'bids': [['100', '2'], ['99', '3'], ['98', '6']],
                'asks': [['101', '2'], ['102', '3']]}
        known = {'bids': {'100': '2', '100.5': '1', '99': '4', '98': '7', '97': '8'},
                 'asks': {'101': '2'}}
        # None is explicitly known unchanged since the initial snapshot.
        changes = {'bids': {'100': (101, 104), '100.5': None,
                            '99': (104, 106), '98': (106, 108), '97': (101, 103)},
                   'asks': {'101': None}}
        result = compare_exchange_snapshot(snap, known, changes,
                                           initial_id=100, local_id=110, contiguous=True)
        self.assertEqual(result['compared'], 3)
        self.assertEqual([(x['side'], x['price']) for x in result['differences']],
                         [('bids', '100.5')])
        reasons = {(s['side'], s['price']): s['reason'] for s in result['skipped']}
        self.assertEqual(reasons[('bids', '99')], 'overlaps_snapshot')
        self.assertEqual(reasons[('bids', '98')], 'changed_after_snapshot')
        self.assertEqual(reasons[('bids', '97')], 'outside_snapshot_coverage')
        self.assertEqual(reasons[('asks', '102')], 'local_level_unknown')

    def test_untrusted_or_out_of_order_timeline_not_compared(self):
        snap = {'lastUpdateId': 105, 'bids': [['100', '2']], 'asks': []}
        known = {'bids': {'100': '2'}, 'asks': {}}
        changes = {'bids': {'100': None}, 'asks': {}}
        for initial, local, continuous in [(100, 110, False), (106, 110, True), (100, 104, True)]:
            with self.subTest(initial=initial, local=local, continuous=continuous):
                result = compare_exchange_snapshot(snap, known, changes, initial_id=initial,
                                                   local_id=local, contiguous=continuous)
                self.assertEqual(result['compared'], 0)
                self.assertFalse(result['eligible'])

    def test_missing_provenance_skipped_not_assumed_stable(self):
        snap = {'lastUpdateId': 105, 'bids': [['100', '2']], 'asks': []}
        result = compare_exchange_snapshot(snap, {'bids': {'100': '2'}, 'asks': {}},
                 {'bids': {}, 'asks': {}}, initial_id=100, local_id=110, contiguous=True)
        self.assertEqual(result['compared'], 0)
        self.assertEqual(result['skipped'][0]['reason'], 'missing_provenance')


if __name__ == '__main__':
    unittest.main()
