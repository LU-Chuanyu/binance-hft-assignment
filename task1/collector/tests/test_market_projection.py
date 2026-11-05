import json
import unittest

from market_stream import RawMessage
from market_projection import project_live, project_backfill


def raw(stream, data):
    return RawMessage(7, 123456789, 987654321,
                      json.dumps({'stream': stream, 'data': data}))


class MarketProjectionTests(unittest.TestCase):
    def test_depth_one_row_per_changed_level_and_preserve_zero(self):
        event = raw('btcusdt@depth@100ms', {'s': 'BTCUSDT', 'E': 123, 'U': 10, 'u': 15,
                     'b': [['100.00', '2'], ['99', '0']], 'a': [['101', '3']]})
        rows = project_live(event, generation=2)
        self.assertEqual([(r['side'], r['price'], r['qty']) for r in rows],
                         [('bid', '100.00', '2'), ('bid', '99', '0'), ('ask', '101', '3')])
        self.assertTrue(all((r['symbol'], r['E'], r['U'], r['u'], r['received_ns'],
                             r['receive_seq'], r['generation']) ==
                            ('BTCUSDT', 123, 10, 15, 123456789, 7, 2) for r in rows))

    def test_trade_preserves_exchange_fields_and_local_receipt(self):
        data = {'s': 'BTCUSDT', 'a': 5, 'p': '100.0', 'q': '0.1', 'm': True, 'T': 1000, 'E': 1001}
        result = project_live(raw('btcusdt@aggTrade', data), generation=1)[0]
        self.assertEqual({k: result[k] for k in data if k != 's'}, {k: v for k, v in data.items() if k != 's'})
        self.assertEqual((result['source'], result['received_ns'], result['estimated_received_ns']),
                         ('live', 123456789, None))

    def test_bookticker_does_not_invent_event_time(self):
        data = {'s': 'BTCUSDT', 'u': 5, 'b': '100', 'B': '2', 'a': '101', 'A': '3'}
        result = project_live(raw('btcusdt@bookTicker', data), generation=1)[0]
        self.assertEqual({k: result[k] for k in ['u', 'b', 'B', 'a', 'A']},
                         {k: data[k] for k in ['u', 'b', 'B', 'a', 'A']})
        self.assertNotIn('E', result)

    def test_rest_backfill_never_sets_real_receipt_or_event_time(self):
        record = {'data': {'a': 5, 'p': '100', 'q': '2', 'm': False, 'T': 2000},
                  'source': 'rest_backfill', 'received_ns': None,
                  'estimated_received_ns': 2005000000,
                  'estimate_typical_delay_ms': 5, 'estimate_clock_offset_ms': 0}
        result = project_backfill(record, symbol='BTCUSDT', generation=2)[0]
        self.assertEqual((result['source'], result['received_ns'], result['estimated_received_ns'], result['E']),
                         ('rest_backfill', None, 2005000000, None))
        self.assertIsNone(result['receive_seq'])

    def test_bad_payload_or_false_real_backfill_receipt_is_rejected(self):
        with self.assertRaises(ValueError):
            project_live(raw('btcusdt@depth@100ms', {'s': 'BTCUSDT', 'E': 1, 'U': 1, 'u': 2,
                                                     'b': [['100']], 'a': []}), generation=1)
        with self.assertRaises(ValueError):
            project_backfill({'data': {}, 'source': 'rest_backfill', 'received_ns': 55},
                             symbol='BTCUSDT', generation=1)


if __name__ == '__main__':
    unittest.main()
