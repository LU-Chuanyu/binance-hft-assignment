import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from aggtrade_backfill import backfill_range, fetch_aggtrade_page, BackfillError


def trade(n):
    return {'a': n, 'p': '100', 'q': '1', 'f': n, 'l': n, 'T': 1000 + n, 'm': False}


class BackfillTests(unittest.TestCase):
    def test_pages_inclusive_ids_and_records_estimate_separately(self):
        calls = []
        def fetch(start, limit):
            calls.append((start, limit))
            return [trade(i) for i in range(start, min(start + limit, 106))]
        result = backfill_range(101, 105, fetch, page_size=2,
                                typical_delay_ms=5, clock_offset_ms=-2)
        self.assertTrue(result['complete'])
        self.assertEqual(result['missing_ranges'], [])
        self.assertEqual(calls, [(101, 2), (103, 2), (105, 1)])
        self.assertEqual([r['data']['a'] for r in result['records']], list(range(101, 106)))
        first = result['records'][0]
        self.assertIsNone(first['received_ns'])
        self.assertEqual(first['source'], 'rest_backfill')
        self.assertEqual(first['estimated_received_ns'], (1101 + 5 - 2) * 1_000_000)

    def test_empty_or_incomplete_data_retains_missing_ranges(self):
        def fetch(start, limit):
            return [trade(102), trade(104)] if start == 101 else []
        result = backfill_range(101, 105, fetch)
        self.assertFalse(result['complete'])
        self.assertEqual(result['missing_ranges'], [(101, 101), (103, 103), (105, 105)])
        self.assertTrue(all(r['estimated_received_ns'] is None for r in result['records']))

    def test_out_of_order_duplicate_and_invalid_rows_report_failure(self):
        for page in ([trade(101), trade(101)], [trade(102), trade(101)], [{'a': 101}]):
            with self.subTest(page=page), self.assertRaises(BackfillError):
                backfill_range(101, 102, lambda start, limit: page)

    def test_page_limit_and_requested_id_range(self):
        result = backfill_range(100, 110, lambda start, limit: [trade(start)], max_pages=2)
        self.assertEqual(result['missing_ranges'], [(102, 110)])
        self.assertEqual(len(result['records']), 2)
        with self.assertRaises(ValueError):
            backfill_range(10, 9, lambda start, limit: [])

    def test_cannot_estimate_without_both_inputs(self):
        result = backfill_range(101, 101, lambda start, limit: [trade(101)], typical_delay_ms=5)
        self.assertIsNone(result['records'][0]['estimated_received_ns'])

    def test_rate_limit_preserves_retry_after_and_partial_missing(self):
        error = HTTPError('https://api.binance.com', 429, 'limited', {'Retry-After': '60'}, None)
        with patch('aggtrade_backfill.urlopen', side_effect=error):
            with self.assertRaises(BackfillError) as caught:
                backfill_range(1, 2, fetch_aggtrade_page)
        self.assertEqual(caught.exception.status, 429)
        self.assertEqual(caught.exception.retry_after, '60')
        self.assertEqual(caught.exception.partial_result['missing_ranges'], [(1, 2)])

    def test_request_failure_keeps_already_fetched_records(self):
        def fetch(start, limit):
            if start == 1:
                return [trade(1)]
            raise OSError('network failure')
        with self.assertRaises(BackfillError) as caught:
            backfill_range(1, 3, fetch, page_size=1)
        self.assertEqual([r['data']['a'] for r in caught.exception.partial_result['records']], [1])
        self.assertEqual(caught.exception.partial_result['missing_ranges'], [(2, 3)])

    def test_shared_rate_limit_error_preserves_status_and_retry_after(self):
        class RateLimitError(Exception):
            status = 429
            retry_after = '17'
        def fetch(start, limit):
            raise RateLimitError('limited')
        with self.assertRaises(BackfillError) as caught:
            backfill_range(1, 3, fetch)
        self.assertEqual(caught.exception.status, 429)
        self.assertEqual(caught.exception.retry_after, '17')
        self.assertEqual(caught.exception.partial_result['missing_ranges'], [(1, 3)])

    def test_shared_rate_limit_wait_preserves_remaining_and_partial(self):
        from api_client import RateLimitWait
        def fetch(start, limit):
            if start == 1:
                return [trade(1)]
            raise RateLimitWait(12.5)
        with self.assertRaises(BackfillError) as caught:
            backfill_range(1, 3, fetch, page_size=1)
        self.assertEqual(caught.exception.retry_after, 12.5)
        self.assertEqual([r['data']['a'] for r in caught.exception.partial_result['records']], [1])
        self.assertEqual(caught.exception.partial_result['missing_ranges'], [(2, 3)])


if __name__ == '__main__':
    unittest.main()
