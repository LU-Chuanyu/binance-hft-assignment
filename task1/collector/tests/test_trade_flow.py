import threading
import unittest

from trade_flow import TradeFlow


def trade(n):
    return {'a': n, 'p': '100', 'q': '1', 'f': n, 'l': n, 'T': 1000 + n, 'm': False}


class TradeFlowTests(unittest.TestCase):
    def test_gap_backfill_and_following_live_duplicate(self):
        flow = TradeFlow(lambda start, limit: [trade(i) for i in range(start, start + limit)])
        self.assertEqual(flow.observe_live(trade(100), 123)['status'], 'first')
        result = flow.observe_live(trade(103), 125)
        self.assertEqual(result['missing_ranges'], [(101, 102)])
        repaired = flow.repair_missing(typical_delay_ms=5, clock_offset_ms=-2)
        self.assertEqual([r['data']['a'] for r in repaired['records']], [101, 102])
        self.assertTrue(all(r['received_ns'] is None for r in repaired['records']))
        self.assertEqual(repaired['missing_ranges'], [])
        self.assertFalse(flow.observe_live(trade(101), 126)['include'])

    def test_unavailable_and_request_failure_remain_visible(self):
        flow = TradeFlow(lambda start, limit: [])
        flow.observe_live(trade(100), 1)
        flow.observe_live(trade(103), 2)
        self.assertEqual(flow.repair_missing()['missing_ranges'], [(101, 102)])
        def failure(start, limit):
            raise OSError('offline')
        flow.fetch_page = failure
        result = flow.repair_missing()
        self.assertEqual(result['missing_ranges'], [(101, 102)])
        self.assertEqual(len(result['errors']), 1)

    def test_live_arrival_during_slow_backfill_is_not_duplicated(self):
        started = threading.Event()
        finish = threading.Event()
        def fetch(start, limit):
            started.set()
            self.assertTrue(finish.wait(2))
            return [trade(i) for i in range(start, start + limit)]
        flow = TradeFlow(fetch)
        flow.observe_live(trade(100), 1)
        flow.observe_live(trade(103), 2)
        output = []
        worker = threading.Thread(target=lambda: output.append(flow.repair_missing()))
        worker.start()
        self.assertTrue(started.wait(2))
        live = flow.observe_live(trade(101), 3)
        finish.set()
        worker.join(2)
        self.assertTrue(live['include'])
        self.assertEqual([r['data']['a'] for r in output[0]['records']], [102])
        self.assertEqual(output[0]['missing_ranges'], [])

    def test_duplicate_live_and_older_than_initial_seen_once(self):
        flow = TradeFlow(lambda start, limit: [])
        flow.observe_live(trade(100), 1)
        self.assertTrue(flow.observe_live(trade(99), 2)['include'])
        self.assertFalse(flow.observe_live(trade(99), 3)['include'])
        self.assertFalse(flow.observe_live(trade(100), 4)['include'])


if __name__ == '__main__':
    unittest.main()
