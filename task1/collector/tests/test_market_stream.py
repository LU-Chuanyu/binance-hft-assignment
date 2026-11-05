import unittest
from unittest.mock import patch

from market_stream import MarketStream, decode_message


class FakeConnection:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.closed = False

    def recv(self):
        item = next(self.messages)
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        self.closed = True


class StreamTests(unittest.TestCase):
    def test_receive_preserves_exact_text_and_records_order_and_times(self):
        raw = ' { "stream" : "btcusdt@bookTicker", "data": {"s":"BTCUSDT"} } '
        connection = FakeConnection([raw, raw])
        reader = MarketStream(connection)
        with patch('market_stream.time.time_ns', side_effect=[100, 200]), \
             patch('market_stream.time.monotonic_ns', side_effect=[11, 22]):
            first, second = reader.receive(), reader.receive()
        self.assertEqual(first.raw_text, raw)
        self.assertEqual((first.sequence, first.received_ns, first.monotonic_ns), (1, 100, 11))
        self.assertEqual((second.sequence, second.received_ns, second.monotonic_ns), (2, 200, 22))
        reader.close()
        self.assertTrue(connection.closed)

    def test_recorded_before_parsing_and_accepts_raw_text_without_json(self):
        reader = MarketStream(FakeConnection(['not yet parsed']))
        with patch('market_stream.json.loads', side_effect=AssertionError('parsed too early')):
            message = reader.receive()
        self.assertEqual(message.raw_text, 'not yet parsed')
        with self.assertRaises(ValueError):
            decode_message(message)

    def test_classifies_three_streams_without_requiring_book_ticker_event_time(self):
        cases = [
            ('{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","U":10,"u":12,"b":[["100","2"]],"a":[]}}', 'depth'),
            ('{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","a":123,"p":"100","q":"1"}}', 'aggTrade'),
            ('{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"100","B":"2","a":"101","A":"3"}}', 'bookTicker'),
        ]
        reader = MarketStream(FakeConnection([raw for raw, _ in cases]))
        for raw, kind in cases:
            with self.subTest(kind=kind):
                event = decode_message(reader.receive())
                self.assertEqual(event.kind, kind)
                self.assertEqual(event.stream, __import__('json').loads(raw)['stream'])
                self.assertEqual(event.data, __import__('json').loads(raw)['data'])
        self.assertNotIn('E', event.data)

    def test_malformed_and_unknown_messages_are_reported(self):
        cases = ['{}', '{"stream":"other","data":{}}',
                 '{"stream":"btcusdt@bookTicker","data":[]}', '[1,2]']
        for raw in cases:
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    decode_message(MarketStream(FakeConnection([raw])).receive())

    def test_invalid_frame_and_connection_error_are_not_silently_accepted(self):
        with self.assertRaises(ValueError):
            MarketStream(FakeConnection([b'binary'])).receive()
        with self.assertRaisesRegex(ConnectionError, 'disconnected'):
            MarketStream(FakeConnection([ConnectionError('disconnected')])).receive()


if __name__ == '__main__':
    unittest.main()
