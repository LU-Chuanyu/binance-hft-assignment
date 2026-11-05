import unittest
from decimal import Decimal

from order_book import OrderBook


class OrderBookTests(unittest.TestCase):
    def test_snapshot_sort_and_best_prices(self):
        book = OrderBook({'bids': [['99', '3'], ['100', '2']],
                          'asks': [['102', '4'], ['101', '1']]})
        self.assertEqual(book.top_levels(1), {
            'bids': [(Decimal('100'), Decimal('2'))],
            'asks': [(Decimal('101'), Decimal('1'))],
        })

    def test_apply_replaces_quantities_and_removes_zero_levels(self):
        book = OrderBook({'bids': [['100', '2'], ['99', '3']],
                          'asks': [['101', '1'], ['102', '4']]})
        book.apply_update({'b': [['100', '0.5'], ['99', '0'], ['98', '1']],
                           'a': [['101', '0'], ['103', '2']]})
        self.assertEqual(book.top_levels(3), {
            'bids': [(Decimal('100'), Decimal('0.5')), (Decimal('98'), Decimal('1'))],
            'asks': [(Decimal('102'), Decimal('4')), (Decimal('103'), Decimal('2'))],
        })

    def test_price_keys_are_exact_and_equivalent_decimal_prices_match(self):
        book = OrderBook({'bids': [['100.01000000', '0.00000001']], 'asks': []})
        book.apply_update({'b': [['100.01', '0.00000002']], 'a': []})
        self.assertEqual(book.top_levels(2)['bids'], [(Decimal('100.01'), Decimal('0.00000002'))])

    def test_empty_book_and_zero_limit(self):
        book = OrderBook({'bids': [], 'asks': []})
        self.assertEqual(book.top_levels(0), {'bids': [], 'asks': []})


if __name__ == '__main__':
    unittest.main()
